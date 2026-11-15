"""Logic thuan cua API (khong phu thuoc FastAPI) de de kiem thu."""
import json, threading, uuid

from src.generator.generator import now_iso

TXN_TYPES = {"order_created", "payment_success", "payment_failed"}
TOPIC_CLICK = "web.clickstream"
TOPIC_TXN = "web.transactions"
API_PRODUCER_ID = "API01"


class Stamper:
    """Dien event_id, ts_event neu thieu. Neu client KHONG gui producer_id thi server gan
    producer_id=API01 va seq tang dan (phai chay 1 worker uvicorn de seq khong trung)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._seq = 0

    def stamp(self, ev: dict) -> dict:
        ev.setdefault("event_id", str(uuid.uuid4()))
        ev.setdefault("ts_event", now_iso())
        if "producer_id" not in ev:
            with self._lock:
                self._seq += 1
                seq = self._seq
            ev["producer_id"] = API_PRODUCER_ID
            ev.setdefault("seq", seq)
        return ev


def route(ev: dict) -> str:
    """Chon topic theo event_type; loai la -> clickstream (consumer se dua vao DLQ)."""
    return TOPIC_TXN if ev.get("event_type") in TXN_TYPES else TOPIC_CLICK


def to_message(ev: dict):
    """Tra ve (topic, key, value_bytes)."""
    key = ev.get("user_id")
    key = key if isinstance(key, str) and key else None
    value = json.dumps(ev, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return route(ev), key, value
