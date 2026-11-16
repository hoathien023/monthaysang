"""REST API nhan su kien tu website/app va dua vao Kafka.

Chay (1 worker de seq khong trung):
  uvicorn src.api.app:app --port 8000
Swagger: http://127.0.0.1:8000/docs

POST /events         1 su kien (JSON object)         -> 202 khi Kafka DA xac nhan, 503 khi loi/timeout
POST /events/batch   danh sach toi da 1000 su kien   -> 202 khi tat ca da xac nhan
GET  /health         kiem tra ket noi Kafka
GET  /stats          so request/su kien da nhan, toc do trung binh
Bien moi truong: KAFKA_BOOTSTRAP_SERVERS (mac dinh localhost:29092; van doc KAFKA_BOOTSTRAP neu co), API_ACKS (all), API_LINGER_MS (5)
"""
import asyncio, os, threading, time
from contextlib import asynccontextmanager
from typing import Any

from confluent_kafka import Producer
from fastapi import Body, FastAPI, HTTPException

from src.api.core import Stamper, to_message

MAX_BATCH = 1000
DELIVERY_TIMEOUT = 10.0

state = {"producer": None, "stop": False, "t0": time.time(),
         "requests": 0, "events": 0, "rejected": 0}
stamper = Stamper()
stats_lock = threading.Lock()


def _poll_loop():
    p = state["producer"]
    while not state["stop"]:
        p.poll(0.05)
    p.flush(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    acks = os.getenv("API_ACKS", "all")
    state["producer"] = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")),
        "acks": acks,
        "enable.idempotence": acks == "all",
        "linger.ms": int(os.getenv("API_LINGER_MS", "5")),
        "queue.buffering.max.messages": 200000,
    })
    state["stop"] = False
    state["t0"] = time.time()
    th = threading.Thread(target=_poll_loop, daemon=True)
    th.start()
    yield
    state["stop"] = True
    th.join(timeout=10)


app = FastAPI(title="Ingestion API", version="1.0", lifespan=lifespan)


async def publish(events: list) -> list:
    """Dua danh sach su kien vao Kafka, cho Kafka xac nhan het. Tra ve danh sach topic."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    lock = threading.Lock()
    pending = {"n": len(events)}
    errors = []

    def finish():
        if not fut.done():
            fut.set_result(None)

    def on_delivery(err, msg):  # chay trong thread poll cua Kafka
        with lock:
            if err is not None:
                errors.append(str(err))
            pending["n"] -= 1
            done = pending["n"] == 0
        if done:
            loop.call_soon_threadsafe(finish)

    topics = []
    p = state["producer"]
    for ev in events:
        topic, key, value = to_message(stamper.stamp(ev))
        try:
            p.produce(topic, key=key, value=value, on_delivery=on_delivery)
        except BufferError:
            raise HTTPException(503, "Hang doi producer day, thu lai sau")
        topics.append(topic)
    try:
        await asyncio.wait_for(fut, timeout=DELIVERY_TIMEOUT)
    except asyncio.TimeoutError:
        with stats_lock:
            state["rejected"] += len(events)
        raise HTTPException(503, "Kafka khong xac nhan kip (timeout)")
    if errors:
        with stats_lock:
            state["rejected"] += len(events)
        raise HTTPException(503, f"Kafka bao loi: {errors[0]}")
    with stats_lock:
        state["requests"] += 1
        state["events"] += len(events)
    return topics


@app.post("/events", status_code=202)
async def post_event(event: dict[str, Any] = Body(...)):
    topics = await publish([event])
    return {"accepted": 1, "topic": topics[0], "event_id": event.get("event_id")}


@app.post("/events/batch", status_code=202)
async def post_batch(events: list[dict[str, Any]] = Body(...)):
    if not events:
        raise HTTPException(422, "Danh sach rong")
    if len(events) > MAX_BATCH:
        raise HTTPException(413, f"Toi da {MAX_BATCH} su kien moi request")
    topics = await publish(events)
    return {"accepted": len(events), "topics": {t: topics.count(t) for t in set(topics)}}


@app.get("/health")
def health():
    try:
        state["producer"].list_topics(timeout=2)
    except Exception as e:
        raise HTTPException(503, f"Khong ket noi duoc Kafka: {e}")
    return {"status": "ok", "kafka": True}


@app.get("/stats")
def stats():
    up = max(time.time() - state["t0"], 1e-9)
    with stats_lock:
        return {"uptime_s": round(up, 1), "requests": state["requests"], "events": state["events"],
                "rejected": state["rejected"], "requests_per_s": round(state["requests"] / up, 1),
                "events_per_s": round(state["events"] / up, 1)}
