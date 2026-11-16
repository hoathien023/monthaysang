#!/usr/bin/env python3
"""
consumer/load_gen.py
--------------------
Bo tao tai TAM THOI cho thi nghiem E1 / E5 / E6 cua TV2.

>>> Day KHONG phai Producer chinh thuc cua do an (viec cua TV1). <<<
Khi Producer cua TV1 xong, nen dung Producer do cho cac thi nghiem va chi giu file
nay de tu kiem tra. Ten tham so duoc dat TRUNG voi ke hoach cua TV1
(--rate, --duration, --acks, --batch-size, --linger-ms, --compression, --bad-ratio)
de de thay the. Neu van dung file nay cho bao cao, hay ghi ro trong bao cao.

Moi lan chay ghi 1 file "manifest" (so da gui, so loi da tiem theo tung ly do) de
doi chieu voi file summary cua Consumer (dac biet E6).

Vi du:
    # E1: 500.000 su kien, toc do toi da, vao topic bench.p3
    python consumer/load_gen.py --topic bench.p3 --count 500000 --rate 0 --run-name E1_p3_run1

    # E5: 10 phut, ~1.000 su kien/giay, vao web.clickstream + web.transactions
    python consumer/load_gen.py --duration 600 --rate 1000 --run-name E5_run1 --producer-id E5R1

    # E6: 100.000 su kien, tiem dung 5% loi
    python consumer/load_gen.py --count 100000 --rate 2000 --bad-ratio 0.05 --run-name E6_run1
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

DEFAULT_BOOTSTRAP = "localhost:29092"
TOPIC_CLICK = "web.clickstream"
TOPIC_TX = "web.transactions"

# Cac kieu loi tiem vao. Ten = ma loi ma Consumer se bao (invalid_by_reason) -> doi chieu 1-1.
BAD_CLICK = ["missing_field:user_id", "wrong_type:url", "invalid_event_type",
             "ts_event_invalid_iso8601", "invalid_value:device"]
BAD_TX = ["missing_field:user_id", "qty_lt_1", "non_positive:price", "empty_items"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(rng: random.Random, pid: str, seq: int, kind: str) -> dict[str, Any]:
    """Tao 1 su kien HOP LE theo data contract v1.0."""
    ev: dict[str, Any] = {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "producer_id": pid,
        "seq": seq,
        "user_id": f"U{rng.randint(1, 50000):06d}",
        "ts_event": now_iso(),
    }
    if kind == "tx":
        items = [
            {"product_id": f"SKU-{rng.randint(1, 999):04d}", "qty": rng.randint(1, 3),
             "price": float(rng.randint(5, 50))}
            for _ in range(rng.randint(1, 3))
        ]
        ev.update({
            "event_type": rng.choice(["order_created", "payment_success", "payment_failed"]),
            "order_id": f"O{2026000000 + rng.randint(1, 9999999)}",
            "items": items,
            "total_amount": float(sum(i["qty"] * i["price"] for i in items)),
            "currency": "USD",
            "payment_method": rng.choice(["card", "wallet", "cod", "bank_transfer"]),
        })
    else:
        sku = f"SKU-{rng.randint(1, 999):04d}"
        ev.update({
            "event_type": rng.choice(["page_view", "search", "click", "add_to_cart"]),
            "session_id": f"S{rng.randint(1, 99999):05d}",
            "product_id": sku,
            "category": rng.choice(["electronics", "fashion", "home"]),
            "device": rng.choice(["mobile", "desktop", "tablet"]),
            "url": f"/product/{sku}",
        })
    return ev


def corrupt(rng: random.Random, ev: dict[str, Any], kind: str) -> str:
    """Lam hong 1 su kien (GIU NGUYEN producer_id + seq de seq van duoc theo doi). Tra ve ma loi."""
    reason = rng.choice(BAD_TX if kind == "tx" else BAD_CLICK)
    if reason == "missing_field:user_id":
        ev.pop("user_id")
    elif reason == "wrong_type:url":
        ev["url"] = 12345
    elif reason == "invalid_event_type":
        ev["event_type"] = "hack"
    elif reason == "ts_event_invalid_iso8601":
        ev["ts_event"] = "khong-phai-ngay-gio"
    elif reason == "invalid_value:device":
        ev["device"] = "smart_tv"
    elif reason == "qty_lt_1":
        ev["items"][0]["qty"] = 0
    elif reason == "non_positive:price":
        ev["items"][0]["price"] = -1.0
    elif reason == "empty_items":
        ev["items"] = []
    return reason


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="load_gen.py",
        description="Bo tao tai tam thoi cho E1/E5/E6 (khong phai Producer chinh thuc).",
    )
    p.add_argument("--bootstrap-servers", "-b",
                   default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP))
    p.add_argument("--topic", default=None,
                   help="Gui TAT CA su kien (loai hanh vi) vao 1 topic, vd bench.p3 (E1). "
                        "Bo trong = gui vao web.clickstream va web.transactions (xen ke, 1/5 la giao dich).")
    p.add_argument("--count", type=int, default=0, help="Dung sau N su kien")
    p.add_argument("--duration", type=float, default=0, help="Dung sau S giay")
    p.add_argument("--rate", type=float, default=0,
                   help="Toc do muc tieu (su kien/giay). 0 = toi da")
    p.add_argument("--producer-id", default="P01")
    p.add_argument("--bad-ratio", type=float, default=0.0, help="Ti le su kien loi can tiem (0.05 = 5%%)")
    p.add_argument("--acks", choices=["0", "1", "all"], default="all")
    p.add_argument("--batch-size", type=int, default=65536, help="batch.size (byte)")
    p.add_argument("--linger-ms", type=float, default=5)
    p.add_argument("--compression", choices=["none", "lz4", "zstd", "gzip", "snappy"], default="none")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--flush-timeout", type=float, default=180, help="Cho toi da S giay de gui not luc ket thuc")
    p.add_argument("--out-dir", default="results")
    p.add_argument("--run-name", default=None)
    p.add_argument("--progress-interval", type=float, default=5.0)
    args = p.parse_args(argv)
    if args.count <= 0 and args.duration <= 0:
        p.error("Can it nhat --count hoac --duration")
    if not 0 <= args.bad_ratio < 1:
        p.error("--bad-ratio phai trong [0, 1)")
    if args.rate < 0:
        p.error("--rate khong duoc am")
    return args


def run(args: argparse.Namespace, producer_cls: Any = None) -> int:
    if producer_cls is None:
        try:
            from confluent_kafka import Producer as producer_cls
        except ImportError:
            print("Loi: pip install confluent-kafka", file=sys.stderr)
            return 1

    rng = random.Random(args.seed)
    run_name = re.sub(r"[^\w.-]", "_", args.run_name or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / f"producer_{run_name}_manifest.json"

    # Tong du kien (de tiem DUNG ti le loi). Khong biet truoc -> tiem ngau nhien theo xac suat.
    if args.count > 0:
        total_planned: Optional[int] = args.count
    elif args.rate > 0 and args.duration > 0:
        total_planned = int(args.rate * args.duration)
    else:
        total_planned = None
    bad_set: Optional[set[int]] = None
    if args.bad_ratio > 0 and total_planned:
        n_bad = round(args.bad_ratio * total_planned)
        bad_set = set(rng.sample(range(1, total_planned + 1), n_bad))

    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": args.producer_id,
        "acks": args.acks,
        "enable.idempotence": args.acks == "all",   # idempotence bat buoc acks=all
        "linger.ms": args.linger_ms,
        "batch.size": args.batch_size,
        "compression.type": args.compression,
        "queue.buffering.max.messages": 500_000,
    }
    producer = producer_cls(conf)

    st = {"delivered": 0, "failed": 0, "last_delivery": None, "first_error": None}
    bad_by_reason: Counter[str] = Counter()

    def on_delivery(err, msg) -> None:
        if err is not None:
            st["failed"] += 1
            st["first_error"] = st["first_error"] or str(err)
        else:
            st["delivered"] += 1
            st["last_delivery"] = time.perf_counter()

    mixed = args.topic is None
    sent = 0
    started_utc = now_iso()
    t0 = time.perf_counter()
    last_progress = t0
    print(f"[load_gen] {run_name}: topic={'web.* (xen ke)' if mixed else args.topic} "
          f"count={args.count or '-'} duration={args.duration or '-'} rate={args.rate or 'max'} "
          f"bad_ratio={args.bad_ratio} producer_id={args.producer_id}", file=sys.stderr)

    def send_one() -> None:
        nonlocal sent
        sent += 1
        seq = sent
        kind = "tx" if (mixed and seq % 5 == 0) else "click"
        ev = make_event(rng, args.producer_id, seq, kind)
        if (seq in bad_set) if bad_set is not None else (args.bad_ratio > 0 and rng.random() < args.bad_ratio):
            bad_by_reason[corrupt(rng, ev, kind)] += 1
        topic = args.topic or (TOPIC_TX if kind == "tx" else TOPIC_CLICK)
        key = ev.get("user_id")
        value = json.dumps(ev, separators=(",", ":")).encode("utf-8")
        while True:
            try:
                producer.produce(topic, key=key.encode("utf-8") if key else None,
                                 value=value, on_delivery=on_delivery)
                break
            except BufferError:              # hang doi noi bo day (vd luc Kafka dang restart)
                producer.poll(0.2)

    try:
        while True:
            elapsed = time.perf_counter() - t0
            if args.count > 0 and sent >= args.count:
                break
            if args.duration > 0 and elapsed >= args.duration:
                break
            if args.rate > 0:
                todo = min(int(elapsed * args.rate) + 1 - sent, 5000)   # gui bu neu bi tre
                if todo <= 0:
                    producer.poll(0)
                    time.sleep(0.001)
                    continue
            else:
                todo = 1000
            if args.count > 0:
                todo = min(todo, args.count - sent)
            for _ in range(todo):
                send_one()
            producer.poll(0)

            now = time.perf_counter()
            if now - last_progress >= args.progress_interval:
                last_progress = now
                print(f"[load_gen] {now - t0:6.1f}s  da gui={sent}  xac nhan={st['delivered']}  "
                      f"({sent / (now - t0):,.0f} ev/s)", file=sys.stderr)
    except KeyboardInterrupt:
        print("[load_gen] Bi ngat boi Ctrl+C - dang gui not...", file=sys.stderr)

    t_end_send = time.perf_counter()
    left = producer.flush(args.flush_timeout)
    delivered_span = (st["last_delivery"] - t0) if st["last_delivery"] else 0.0

    manifest = {
        "run_name": run_name,
        "producer_id": args.producer_id,
        "topic": args.topic or "web.clickstream+web.transactions",
        "started_utc": started_utc,
        "ended_utc": now_iso(),
        "sent": sent,                       # so ban ghi da dua vao producer
        "delivered": st["delivered"],       # so ban ghi Kafka da xac nhan
        "failed": st["failed"],
        "not_flushed": left,
        "first_error": st["first_error"],
        "send_duration_s": round(t_end_send - t0, 3),
        "producer_duration_s": round(delivered_span, 3),   # tu luc bat dau den xac nhan cuoi
        "producer_eps": round(st["delivered"] / delivered_span, 1) if delivered_span > 0 else 0.0,
        "bad_injected": sum(bad_by_reason.values()),
        "bad_ratio_actual": round(sum(bad_by_reason.values()) / sent, 5) if sent else 0.0,
        "bad_by_reason": dict(bad_by_reason),
        "config": {k: v for k, v in vars(args).items()},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n" + "=" * 60)
    print(f"PRODUCER: {run_name}")
    print("=" * 60)
    print(f"  Da gui / xac nhan / that bai : {sent} / {st['delivered']} / {st['failed']}")
    print(f"  Thoi gian (toi xac nhan cuoi): {manifest['producer_duration_s']} s"
          f"  -> {manifest['producer_eps']:,} su kien/giay")
    if manifest["bad_injected"]:
        print(f"  Loi da tiem                  : {manifest['bad_injected']} "
              f"({manifest['bad_ratio_actual'] * 100:.2f}%)  {manifest['bad_by_reason']}")
    print(f"  Manifest                     : {manifest_path}")
    print("=" * 60 + "\n")

    if st["failed"] or left:
        print(f"CANH BAO: that bai={st['failed']} chua gui xong={left} loi dau tien={st['first_error']}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run(parse_args()))
