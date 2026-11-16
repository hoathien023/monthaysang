#!/usr/bin/env python3
"""
consumer/smoke_publish.py
-------------------------
Gui DU LIEU THU co chu dich vao Kafka de TV2 tu kiem tra Consumer, khi Producer
cua TV1 chua xong. KHONG phai producer cua do an (viec cua TV1).

Du lieu: 2 producer (P01, P02), moi producer seq 1..COUNT, xen ke click/tx.
Loi duoc chen co chu dich (co dinh, de doi chieu):
    P01 seq 10  : tx thieu user_id                       -> DLQ: missing_field:user_id
    P01 seq 20  : tx co qty = 0                          -> DLQ: qty_lt_1
    P02 seq 31  : click co event_type = "hack"           -> DLQ: invalid_event_type
    P01 seq 50  : KHONG gui                              -> Consumer bao THIEU 1
    P02 seq 70  : gui 2 lan                              -> Consumer bao TRUNG 1

Voi COUNT = 100 (mac dinh), Consumer phai bao:
    nhan = 200, hop le = 197, loi = 3, DLQ = 3, seq thieu = 1, seq trung = 1

Cach dung (Kafka dang chay, da tao topic):
    python consumer/smoke_publish.py
    python consumer/verify_consumer.py --run-name smoke --idle-timeout 10

Luu y: chay lai se gui them du lieu vao CUNG topic -> so lieu Consumer se nhan doi.
Truoc moi lan thu hay doc voi group moi VA topic sach:
    docker compose down -v ; docker compose up -d ; python infra/create_topics.py
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timezone

try:
    from confluent_kafka import Producer
except ImportError:
    print("Loi: pip install confluent-kafka", file=sys.stderr)
    sys.exit(1)

DEFAULT_BOOTSTRAP = "localhost:29092"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(rng: random.Random, pid: str, seq: int) -> tuple[str, dict]:
    """seq chia het cho 5 -> giao dich, con lai -> hanh vi."""
    user = f"U{rng.randint(1, 5000):06d}"
    base = {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "producer_id": pid,
        "seq": seq,
        "user_id": user,
        "ts_event": now_iso(),
    }
    if seq % 5 == 0:
        items = [
            {"product_id": f"SKU-{rng.randint(1, 99):04d}", "qty": rng.randint(1, 3),
             "price": float(rng.randint(5, 50))}
            for _ in range(rng.randint(1, 3))
        ]
        base.update({
            "event_type": rng.choice(["order_created", "payment_success", "payment_failed"]),
            "order_id": f"O{2026000000 + rng.randint(1, 999999)}",
            "items": items,
            "total_amount": float(sum(i["qty"] * i["price"] for i in items)),
            "currency": "USD",
            "payment_method": rng.choice(["card", "wallet", "cod", "bank_transfer"]),
        })
        return "web.transactions", base
    sku = f"SKU-{rng.randint(1, 99):04d}"
    base.update({
        "event_type": rng.choice(["page_view", "click", "add_to_cart"]),
        "session_id": f"S{rng.randint(1, 99999):05d}",
        "product_id": sku,
        "category": rng.choice(["electronics", "fashion", "home"]),
        "device": rng.choice(["mobile", "desktop", "tablet"]),
        "url": f"/product/{sku}",
    })
    return "web.clickstream", base


def main() -> None:
    ap = argparse.ArgumentParser(description="Gui du lieu thu cho Consumer (co loi/thieu/trung co chu dich).")
    ap.add_argument("--bootstrap-servers", "-b",
                    default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP))
    ap.add_argument("--count", type=int, default=100, help="So su kien moi producer (>= 70, mac dinh 100)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    if args.count < 70:
        sys.exit("--count phai >= 70 de cac loi chen co dinh hop le.")

    rng = random.Random(args.seed)
    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "acks": "all",
                         "enable.idempotence": True})
    failures: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            failures.append(str(err))

    sent = 0

    def send(topic: str, ev: dict, raw: bytes | None = None) -> None:
        nonlocal sent
        key = ev.get("user_id")
        producer.produce(topic, key=key.encode() if key else None,
                         value=raw if raw is not None else json.dumps(ev).encode("utf-8"),
                         on_delivery=on_delivery)
        sent += 1
        producer.poll(0)

    for seq in range(1, args.count + 1):
        for pid in ("P01", "P02"):
            if pid == "P01" and seq == 50:
                continue                                    # THIEU: khong gui
            topic, ev = make_event(rng, pid, seq)
            if pid == "P01" and seq == 10:
                ev.pop("user_id")                           # LOI: thieu truong
            elif pid == "P01" and seq == 20:
                ev["items"][0]["qty"] = 0                   # LOI: qty < 1
            elif pid == "P02" and seq == 31:
                ev["event_type"] = "hack"                   # LOI: event_type la
            send(topic, ev)
            if pid == "P02" and seq == 70:
                send(topic, ev)                             # TRUNG: gui lan 2

    left = producer.flush(30)
    print(f"Da gui {sent} message ({failures and len(failures) or 0} that bai, {left} chua xong).")
    if args.count == 100:
        print("Ky vong Consumer: nhan=200 hop_le=197 loi=3 DLQ=3 seq_thieu=1 seq_trung=1")
    if failures:
        print("Loi gui:", failures[:3], file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
