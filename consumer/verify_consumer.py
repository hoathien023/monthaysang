#!/usr/bin/env python3
"""
consumer/verify_consumer.py
---------------------------
TV2 - Consumer kiem chung cho pipeline Sources -> Ingestion.

Viec Consumer lam:
  1. Doc web.clickstream + web.transactions (mac dinh).
  2. Kiem tra tung su kien theo data contract v1.0 (schema, kieu, gia tri).
  3. Su kien loi  -> day vao topic web.events.dlq kem error_reason (KHONG bo im lang).
  4. Phat hien seq THIEU / TRUNG theo tung producer_id.
  5. Do do tre = thoi diem nhan - ts_event (producer va consumer chay cung may).
  6. Ghi log theo thoi gian, file timeline CSV va file tom tat JSON cuoi lan chay.

Cach dung:
    python consumer/verify_consumer.py
    python consumer/verify_consumer.py --run-name E6_run1 --idle-timeout 30
    python consumer/verify_consumer.py --max-messages 500000 --run-name E1_p6_run1
    python consumer/verify_consumer.py --idle-timeout 0          # chay den khi Ctrl+C (E5)

Luu y quan trong:
  * Phan tich seq chi dung khi CHI CO 1 consumer trong group doc toan bo partition
    (moi instance chi thay cac partition duoc giao).
  * Mac dinh moi lan chay dung group.id MOI (doc tu dau topic). Topic con du lieu cu
    cua lan chay truoc se bi tinh chung -> truoc moi thi nghiem hay dung topic/du lieu sach.
  * Khong phat hien duoc su kien that lac o CUOI day seq (khong co seq lon hon de so sanh).
    Hay doi chieu them voi so luong ma producer bao cao.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import signal
import sys
import time
from array import array
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Final, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Hang so theo DATA CONTRACT v1.0
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BOOTSTRAP: Final[str] = "localhost:29092"   # khop docker-compose.yml (EXTERNAL listener)
DEFAULT_TOPICS: Final[list[str]] = ["web.clickstream", "web.transactions"]
DLQ_TOPIC: Final[str] = "web.events.dlq"

CLICK_TYPES: Final[frozenset[str]] = frozenset({"page_view", "search", "click", "add_to_cart"})
TX_TYPES: Final[frozenset[str]] = frozenset({"order_created", "payment_success", "payment_failed"})
ALL_TYPES: Final[frozenset[str]] = CLICK_TYPES | TX_TYPES
DEVICES: Final[frozenset[str]] = frozenset({"mobile", "desktop", "tablet"})
CURRENCIES: Final[frozenset[str]] = frozenset({"VND", "USD"})
PAYMENT_METHODS: Final[frozenset[str]] = frozenset({"card", "wallet", "cod", "bank_transfer"})

# Topic nao chi chap nhan loai su kien nao. Topic khac (vd bench.*) -> suy ra tu event_type.
TOPIC_KIND: Final[dict[str, str]] = {"web.clickstream": "click", "web.transactions": "tx"}

REQUIRED_CLICK: Final[tuple[str, ...]] = (
    "event_id", "producer_id", "seq", "event_type", "user_id",
    "session_id", "device", "url", "ts_event",
)
REQUIRED_TX: Final[tuple[str, ...]] = (
    "event_id", "producer_id", "seq", "event_type", "user_id", "ts_event",
    "order_id", "items", "total_amount", "currency", "payment_method",
)
STR_FIELDS_CLICK: Final[tuple[str, ...]] = ("event_id", "producer_id", "user_id", "session_id", "url")
STR_FIELDS_TX: Final[tuple[str, ...]] = ("event_id", "producer_id", "user_id", "order_id")

UUID_RE: Final[re.Pattern[str]] = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
MAX_SEQ: Final[int] = 50_000_000   # chan seq vo ly (bitmap toi da ~50 MB / producer)


# ─────────────────────────────────────────────────────────────────────────────
# Do luong (dang duoc thiet ke de THAY bang common/metrics.py cua TV3)
# Khi TV3 giao file that: xoa 2 ham duoi day va dung
#     from common.metrics import throughput, percentiles
# (neu chu ky ham khac thi chi sua cho nay).
# ─────────────────────────────────────────────────────────────────────────────

def throughput(count: int, seconds: float) -> float:
    """So su kien tren giay."""
    return count / seconds if seconds > 0 else 0.0


def percentiles(values, qs=(50, 95, 99)) -> dict[int, Optional[float]]:
    """Percentile theo noi suy tuyen tinh (giong numpy mac dinh). Rong -> None."""
    if len(values) == 0:
        return {q: None for q in qs}
    data = sorted(values)
    n = len(data)
    out: dict[int, Optional[float]] = {}
    for q in qs:
        if n == 1:
            out[q] = data[0]
            continue
        pos = (n - 1) * q / 100.0
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        out[q] = data[lo] + (data[hi] - data[lo]) * (pos - lo)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Validate su kien
# ─────────────────────────────────────────────────────────────────────────────

def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def parse_ts(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 co mui gio (vd 2026-11-20T08:15:30.123Z). Khong co mui gio -> None."""
    if not isinstance(value, str):
        return None
    s = value.strip()
    if s[-1:] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def validate_event(
    obj: Any,
    topic: str,
    *,
    strict_uuid: bool = False,
    check_total: bool = True,
) -> tuple[Optional[str], Optional[datetime]]:
    """
    Kiem tra 1 su kien da parse JSON.

    Returns:
        (None, ts_event_utc)  neu hop le
        (reason, None)        neu loi. Chi tra LOI DAU TIEN gap -> moi su kien loi dung
                              dung 1 ly do, nen so trong DLQ doi chieu duoc 1-1.
    Quy uoc reason: "<ma_loi>:<truong>" hoac "<ma_loi>=<gia_tri>" (phan sau '=' la chi tiet
    tu do, khong dung de gom nhom thong ke).
    """
    if not isinstance(obj, dict):
        return "not_a_json_object", None

    et = obj.get("event_type")
    if et is None:
        return "missing_field:event_type", None
    if not isinstance(et, str) or et not in ALL_TYPES:
        return f"invalid_event_type={str(et)[:30]}", None

    kind = "click" if et in CLICK_TYPES else "tx"
    expected = TOPIC_KIND.get(topic)
    if expected is not None and expected != kind:
        return f"event_type_not_allowed_for_topic={et}@{topic}", None

    # 1) truong bat buoc
    for f in (REQUIRED_CLICK if kind == "click" else REQUIRED_TX):
        if obj.get(f) is None:
            return f"missing_field:{f}", None

    # 2) kieu chuoi
    for f in (STR_FIELDS_CLICK if kind == "click" else STR_FIELDS_TX):
        v = obj[f]
        if not isinstance(v, str):
            return f"wrong_type:{f}", None
        if not v.strip():
            return f"empty_value:{f}", None
    if strict_uuid and not UUID_RE.fullmatch(obj["event_id"]):
        return "invalid_value:event_id", None

    # 3) seq
    if not _is_int(obj["seq"]):
        return "wrong_type:seq", None
    if obj["seq"] < 1:
        return "invalid_value:seq", None

    # 4) rieng tung loai
    if kind == "click":
        if obj["device"] not in DEVICES:
            return "invalid_value:device", None
        for f in ("product_id", "category"):
            if obj.get(f) is not None and not isinstance(obj[f], str):
                return f"wrong_type:{f}", None
    else:
        items = obj["items"]
        if not isinstance(items, list):
            return "wrong_type:items", None
        if not items:
            return "empty_items", None
        computed = 0.0
        for it in items:
            if not isinstance(it, dict):
                return "wrong_type:items[]", None
            pid, qty, price = it.get("product_id"), it.get("qty"), it.get("price")
            if not isinstance(pid, str) or not pid.strip():
                return "missing_field:items[].product_id", None
            if not _is_int(qty):
                return "wrong_type:items[].qty", None
            if qty < 1:
                return "qty_lt_1", None
            if not _is_num(price):
                return "wrong_type:items[].price", None
            if price <= 0:
                return "non_positive:price", None
            computed += qty * price
        total = obj["total_amount"]
        if not _is_num(total):
            return "wrong_type:total_amount", None
        if total <= 0:
            return "non_positive:total_amount", None
        if obj["currency"] not in CURRENCIES:
            return "invalid_value:currency", None
        if obj["payment_method"] not in PAYMENT_METHODS:
            return "invalid_value:payment_method", None
        if check_total and abs(total - computed) > max(0.01, 1e-6 * abs(computed)):
            return "total_amount_mismatch", None

    # 5) ts_event
    ts = parse_ts(obj["ts_event"])
    if ts is None:
        return "ts_event_invalid_iso8601", None
    return None, ts


# ─────────────────────────────────────────────────────────────────────────────
# Phat hien thieu/trung seq
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _ProducerSeq:
    buf: bytearray = field(default_factory=lambda: bytearray(1024))  # buf[s] = 1 neu da thay seq s
    unique: int = 0
    dups: int = 0
    min_seq: Optional[int] = None
    max_seq: int = 0


class SeqTracker:
    """
    Theo doi seq theo producer_id bang BITMAP (1 byte/seq).

    Vi sao khong chi so sanh "seq tiep theo = seq truoc + 1"?
    Cac su kien cua cung producer di vao NHIEU partition (key = user_id), ma Kafka
    chi dam bao thu tu TRONG 1 partition -> consumer nhan seq lech thu tu la binh
    thuong. Dung bitmap thi nhan lech thu tu van khong bi bao thieu sai.

        trung   = seq da co trong bitmap khi gap lai
        thieu   = max_seq - so_seq_khac_nhau   (gia dinh seq bat dau tu 1)
    """

    def __init__(self, max_seq: int = MAX_SEQ) -> None:
        self.max_allowed = max_seq
        self._p: dict[str, _ProducerSeq] = {}

    def add(self, producer_id: str, seq: int) -> str:
        """Tra ve 'new' | 'dup' | 'ignored' (seq vuot nguong an toan)."""
        if seq > self.max_allowed:
            return "ignored"
        ps = self._p.get(producer_id)
        if ps is None:
            ps = self._p[producer_id] = _ProducerSeq()
        if seq >= len(ps.buf):
            ps.buf.extend(bytes(max(seq + 1 - len(ps.buf), len(ps.buf))))
        if ps.buf[seq]:
            ps.dups += 1
            return "dup"
        ps.buf[seq] = 1
        ps.unique += 1
        ps.max_seq = max(ps.max_seq, seq)
        ps.min_seq = seq if ps.min_seq is None else min(ps.min_seq, seq)
        return "new"

    @staticmethod
    def _missing_ranges(ps: _ProducerSeq, limit: int) -> list[list[int]]:
        ranges: list[list[int]] = []
        i, end = 1, ps.max_seq + 1
        while i < end and len(ranges) < limit:
            z = ps.buf.find(0, i, end)
            if z == -1:
                break
            o = ps.buf.find(1, z, end)
            ranges.append([z, (o - 1) if o != -1 else ps.max_seq])
            if o == -1:
                break
            i = o
        return ranges

    def report(self, range_limit: int = 20) -> dict[str, Any]:
        per: dict[str, Any] = {}
        total_missing = total_dups = 0
        for pid, ps in sorted(self._p.items()):
            missing = ps.max_seq - ps.unique
            per[pid] = {
                "unique": ps.unique,
                "min_seq": ps.min_seq,
                "max_seq": ps.max_seq,
                "missing": missing,
                "duplicates": ps.dups,
                "missing_ranges": self._missing_ranges(ps, range_limit) if missing else [],
            }
            total_missing += missing
            total_dups += ps.dups
        return {
            "per_producer": per,
            "total_missing": total_missing,
            "total_duplicates": total_dups,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Engine xu ly tung message (khong phu thuoc Kafka -> test duoc doc lap)
# ─────────────────────────────────────────────────────────────────────────────

DlqSend = Callable[[Optional[bytes], bytes], None]


class VerifyEngine:
    def __init__(
        self,
        dlq_send: DlqSend,
        *,
        strict_uuid: bool = False,
        check_total: bool = True,
        max_seq: int = MAX_SEQ,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._dlq_send = dlq_send
        self._strict_uuid = strict_uuid
        self._check_total = check_total
        self._clock = clock
        self.seq = SeqTracker(max_seq)

        self.received = 0
        self.valid = 0
        self.invalid = 0
        self.untracked_seq = 0     # su kien khong lay duoc (producer_id, seq) hop le
        self.key_mismatch = 0      # su kien hop le co key != user_id
        self.by_topic: dict[str, dict[str, int]] = defaultdict(
            lambda: {"received": 0, "valid": 0, "invalid": 0}
        )
        self.invalid_by_reason: Counter[str] = Counter()
        self.latency_ms = array("d")

        self._win_received = 0
        self._win_valid = 0
        self._win_invalid = 0
        self._win_lat: list[float] = []

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _decode(value: Optional[bytes]) -> tuple[Any, Any, Optional[str]]:
        """-> (obj, payload_de_ghi_DLQ, reason_neu_loi)"""
        if not value:
            return None, None, "empty_payload"
        try:
            raw = value.decode("utf-8")
        except UnicodeDecodeError:
            return None, value.decode("utf-8", "replace"), "invalid_json=bad_utf8"
        try:
            obj = json.loads(raw)
        except ValueError:
            return None, raw, "invalid_json"
        return obj, obj, None

    def _track_seq(self, obj: dict) -> None:
        pid, seq = obj.get("producer_id"), obj.get("seq")
        if isinstance(pid, str) and pid and _is_int(seq) and seq >= 1:
            if self.seq.add(pid, seq) == "ignored":
                self.untracked_seq += 1
        else:
            self.untracked_seq += 1

    # -- xu ly 1 message -----------------------------------------------------

    def handle(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: Optional[bytes],
        value: Optional[bytes],
    ) -> bool:
        """Xu ly 1 message. Tra ve True neu hop le."""
        self.received += 1
        self._win_received += 1
        tstat = self.by_topic[topic]
        tstat["received"] += 1

        obj, payload, reason = self._decode(value)
        ts: Optional[datetime] = None

        # seq duoc theo doi cho MOI su kien doc duoc (ke ca su kien sai o truong khac):
        # seq do muc do DAY DU cua viec giao nhan, khong do chat luong noi dung.
        if isinstance(obj, dict):
            self._track_seq(obj)
        else:
            self.untracked_seq += 1

        if reason is None:
            reason, ts = validate_event(
                obj, topic, strict_uuid=self._strict_uuid, check_total=self._check_total
            )

        if reason is None and ts is not None:
            self.valid += 1
            self._win_valid += 1
            tstat["valid"] += 1
            lat = (self._clock() - ts.timestamp()) * 1000.0
            self.latency_ms.append(lat)
            self._win_lat.append(lat)
            if key is None or key.decode("utf-8", "replace") != obj["user_id"]:
                self.key_mismatch += 1
            return True

        # --- su kien loi -> DLQ ---
        assert reason is not None
        self.invalid += 1
        self._win_invalid += 1
        tstat["invalid"] += 1
        self.invalid_by_reason[reason.split("=", 1)[0]] += 1

        record = {
            "payload": payload,
            "error_reason": reason,
            "source_topic": topic,
            "ts_failed": utc_now_iso(),
            "source_partition": partition,   # them de de truy vet
            "source_offset": offset,
        }
        event_id = obj.get("event_id") if isinstance(obj, dict) else None
        dlq_key = event_id.encode("utf-8") if isinstance(event_id, str) and event_id else None
        self._dlq_send(dlq_key, json.dumps(record, ensure_ascii=False).encode("utf-8"))
        return False

    # -- cua so thoi gian cho log --------------------------------------------

    def pop_window(self) -> dict[str, Any]:
        pct = percentiles(self._win_lat, (50, 95))
        win = {
            "received": self._win_received,
            "valid": self._win_valid,
            "invalid": self._win_invalid,
            "p50_ms": pct[50],
            "p95_ms": pct[95],
        }
        self._win_received = self._win_valid = self._win_invalid = 0
        self._win_lat = []
        return win

    # -- bao cao cuoi --------------------------------------------------------

    def report(self) -> dict[str, Any]:
        lat = self.latency_ms
        pct = percentiles(lat, (50, 95, 99))
        return {
            "counts": {
                "received": self.received,
                "valid": self.valid,
                "invalid": self.invalid,
                "untracked_seq": self.untracked_seq,
                "key_mismatch": self.key_mismatch,
            },
            "by_topic": {t: dict(v) for t, v in sorted(self.by_topic.items())},
            "invalid_by_reason": dict(self.invalid_by_reason.most_common()),
            "seq": self.seq.report(),
            "latency_ms": {
                "count": len(lat),
                "mean": (sum(lat) / len(lat)) if len(lat) else None,
                "min": min(lat) if len(lat) else None,
                "p50": pct[50],
                "p95": pct[95],
                "p99": pct[99],
                "max": max(lat) if len(lat) else None,
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# Ghi timeline CSV
# ─────────────────────────────────────────────────────────────────────────────

class Timeline:
    FIELDS = [
        "ts_utc", "elapsed_s", "received_total", "received_interval", "rate_eps",
        "valid_total", "invalid_total", "p50_ms", "p95_ms", "dlq_sent",
    ]

    def __init__(self, path: Path) -> None:
        self._f = open(path, "w", newline="", encoding="utf-8")
        self._w = csv.writer(self._f)
        self._w.writerow(self.FIELDS)
        self._f.flush()

    def row(self, values: list[Any]) -> None:
        self._w.writerow(values)
        self._f.flush()   # flush moi dong de Ctrl+C van con du lieu

    def close(self) -> None:
        self._f.close()


# ─────────────────────────────────────────────────────────────────────────────
# Chay voi Kafka
# ─────────────────────────────────────────────────────────────────────────────

class _Stop:
    def __init__(self) -> None:
        self.reason: Optional[str] = None

    def set(self, reason: str) -> None:
        if self.reason is None:
            self.reason = reason


def _load_kafka():
    try:
        import confluent_kafka as ck
    except ImportError:
        print("Loi: confluent-kafka chua duoc cai.\nChay: pip install confluent-kafka", file=sys.stderr)
        sys.exit(1)
    return ck


def _fmt(v: Optional[float], nd: int = 1) -> str:
    return "-" if v is None else f"{v:.{nd}f}"


def run(args: argparse.Namespace, kafka: Any = None) -> int:
    """Chay consumer. `kafka` chi dung de test (thay module confluent_kafka bang ban gia)."""
    log = logging.getLogger("verify_consumer")
    ck = kafka if kafka is not None else _load_kafka()

    if DLQ_TOPIC in args.topics:
        log.error("Khong duoc doc chinh topic %s (se tao vong lap).", DLQ_TOPIC)
        return 1

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_name = re.sub(r"[^\w.-]", "_", args.run_name or run_id)
    group_id = args.group_id or f"verify-{run_id}"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / f"consumer_{run_name}_summary.json"
    timeline_path = out_dir / f"consumer_{run_name}_timeline.csv"

    log.info("=== verify_consumer.py ===")
    log.info("Bootstrap : %s", args.bootstrap_servers)
    log.info("Topics    : %s", args.topics)
    log.info("Group id  : %s (offset.reset=%s)", group_id, args.offset_reset)
    log.info("Ket qua   : %s , %s", summary_path, timeline_path)

    # --- DLQ producer (acks=all + idempotence: khong mat/khong trung ban ghi loi) ---
    dlq = {"sent": 0, "delivered": 0, "failed": 0}

    def on_delivery(err, msg) -> None:
        if err is not None:
            dlq["failed"] += 1
            log.error("Gui DLQ that bai: %s", err)
        else:
            dlq["delivered"] += 1

    producer = ck.Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })

    def dlq_send(key: Optional[bytes], value: bytes) -> None:
        while True:
            try:
                producer.produce(DLQ_TOPIC, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:      # hang doi noi bo day -> nhuong cho producer day bot
                producer.poll(0.5)
        dlq["sent"] += 1
        producer.poll(0)

    # --- Consumer ---
    # enable.auto.commit=False + enable.auto.offset.store=False:
    #   offset chi duoc luu/commit SAU KHI xu ly xong (va DLQ da flush) -> at-least-once.
    consumer = ck.Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "enable.partition.eof": False,
        "log.connection.close": False,
    })

    # kiem tra ket noi + topic ton tai
    try:
        md = consumer.list_topics(timeout=15)
    except ck.KafkaException as exc:
        log.error("Khong ket noi duoc Kafka: %s", exc)
        log.error("Goi y: 'docker compose ps' -> kafka phai (healthy).")
        consumer.close()
        return 1
    missing_topics = [t for t in args.topics if t not in md.topics]
    if missing_topics:
        log.error("Topic chua ton tai: %s. Chay: python infra/create_topics.py", missing_topics)
        consumer.close()
        return 1

    engine = VerifyEngine(
        dlq_send,
        strict_uuid=args.strict_uuid,
        check_total=not args.no_total_check,
        max_seq=args.max_seq,
    )
    timeline = Timeline(timeline_path)
    stop = _Stop()

    def _on_signal(signum, frame) -> None:
        stop.set("signal")

    for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, _on_signal)
            except ValueError:   # khong o main thread
                pass

    consumer.subscribe(args.topics)

    kafka_errors: Counter[str] = Counter()
    err_last_log: dict[str, float] = {}

    def note_error(err) -> None:
        name = err.name() if hasattr(err, "name") else str(err)
        kafka_errors[name] += 1
        now_m = time.monotonic()
        if now_m - err_last_log.get(name, -1e9) >= 10:
            err_last_log[name] = now_m
            log.warning("Kafka error (%s): %s  [se tu ket noi lai]", name, err)

    pending: dict[tuple[str, int], int] = {}   # (topic, partition) -> offset tiep theo
    since_commit = 0

    def commit() -> None:
        nonlocal since_commit
        left = producer.flush(30)      # DLQ phai xuong Kafka TRUOC khi commit offset
        if left:
            log.warning("Con %d ban ghi DLQ chua gui xong sau 30s.", left)
        if pending:
            consumer.store_offsets(offsets=[
                ck.TopicPartition(t, p, o) for (t, p), o in pending.items()
            ])
            pending.clear()
        try:
            consumer.commit(asynchronous=False)
        except ck.KafkaException as exc:
            code = exc.args[0].code()
            if code != getattr(ck.KafkaError, "_NO_OFFSET", None):
                log.warning("Commit offset loi: %s", exc)
        since_commit = 0

    started_utc = utc_now_iso()
    t0 = time.monotonic()
    last_msg_t = t0
    last_commit_t = t0
    last_tick_t = t0
    first_msg_t: Optional[float] = None
    last_data_t: Optional[float] = None
    max_gap = 0.0
    exit_code = 0

    try:
        while stop.reason is None:
            msgs = consumer.consume(num_messages=args.batch_size, timeout=1.0)
            now = time.monotonic()
            got = 0
            for msg in msgs:
                err = msg.error()
                if err is not None:
                    if err.fatal():
                        raise ck.KafkaException(err)
                    note_error(err)
                    continue
                engine.handle(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value())
                pending[(msg.topic(), msg.partition())] = msg.offset() + 1
                got += 1

            if got:
                if first_msg_t is None:
                    first_msg_t = now
                else:
                    max_gap = max(max_gap, now - (last_data_t or now))
                last_data_t = now
                last_msg_t = now
                since_commit += got
                producer.poll(0)

            if since_commit >= args.commit_every or (
                since_commit and now - last_commit_t >= args.commit_interval
            ):
                commit()
                last_commit_t = now

            if now - last_tick_t >= args.log_interval:
                win = engine.pop_window()
                dt = now - last_tick_t
                rate = throughput(win["received"], dt)
                log.info(
                    "nhan=%d (+%d, %.0f ev/s) hop_le=%d loi=%d dlq=%d | tre p50=%s p95=%s ms",
                    engine.received, win["received"], rate, engine.valid, engine.invalid,
                    dlq["sent"], _fmt(win["p50_ms"]), _fmt(win["p95_ms"]),
                )
                timeline.row([
                    utc_now_iso(), round(now - t0, 2), engine.received, win["received"],
                    round(rate, 1), engine.valid, engine.invalid,
                    "" if win["p50_ms"] is None else round(win["p50_ms"], 2),
                    "" if win["p95_ms"] is None else round(win["p95_ms"], 2),
                    dlq["sent"],
                ])
                last_tick_t = now

            if args.max_messages and engine.received >= args.max_messages:
                stop.set("max_messages")
            limit = args.idle_timeout if first_msg_t is not None else args.start_timeout
            if limit > 0 and now - last_msg_t > limit:
                stop.set("idle_timeout" if first_msg_t is not None else "no_data_start_timeout")

    except ck.KafkaException as exc:
        log.error("Loi Kafka nghiem trong: %s", exc)
        stop.set("fatal_kafka_error")
        exit_code = 1
    finally:
        log.info("Dung (%s). Dang flush DLQ, commit offset...", stop.reason or "unknown")
        try:
            commit()
        except Exception as exc:  # noqa: BLE001
            log.warning("Khong commit duoc luc dong: %s", exc)
        consumer.close()
        timeline.close()

    ended = time.monotonic()
    active = (last_data_t - first_msg_t) if (first_msg_t is not None and last_data_t is not None) else 0.0
    report = engine.report()
    summary: dict[str, Any] = {
        "run_name": run_name,
        "group_id": group_id,
        "started_utc": started_utc,
        "ended_utc": utc_now_iso(),
        "stop_reason": stop.reason,
        "wall_duration_s": round(ended - t0, 3),
        "active_duration_s": round(active, 3),     # tu message dau den message cuoi
        "throughput_eps": round(throughput(engine.received, active), 1),
        "max_receive_gap_s": round(max_gap, 2),    # sai so ~1s; dung uoc luong thoi gian phuc hoi (E5)
        "kafka_errors": dict(kafka_errors),
        "dlq": dict(dlq),
        **report,
        "checks": {
            "valid_plus_invalid_equals_received": engine.valid + engine.invalid == engine.received,
            "dlq_sent_equals_invalid": dlq["sent"] == engine.invalid,
            "dlq_all_delivered": dlq["delivered"] == dlq["sent"] and dlq["failed"] == 0,
        },
        "args": vars(args),
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print_report(summary)
    log.info("Da ghi %s", summary_path)
    if not all(summary["checks"].values()) and exit_code == 0:
        exit_code = 2
    return exit_code


def print_report(s: dict[str, Any]) -> None:
    c, lat, seq = s["counts"], s["latency_ms"], s["seq"]
    line = "=" * 64
    print(f"\n{line}\nTOM TAT: {s['run_name']}   (dung vi: {s['stop_reason']})\n{line}")
    print(f"  Thoi gian hoat dong     : {s['active_duration_s']} s   (tong: {s['wall_duration_s']} s)")
    print(f"  Nhan                    : {c['received']}   ({s['throughput_eps']} su kien/giay)")
    print(f"  Hop le / Loi            : {c['valid']} / {c['invalid']}")
    for topic, t in s["by_topic"].items():
        print(f"     - {topic:<18} nhan={t['received']} hop_le={t['valid']} loi={t['invalid']}")
    if s["invalid_by_reason"]:
        print("  Ly do loi:")
        for reason, n in s["invalid_by_reason"].items():
            print(f"     - {reason:<40} {n}")
    print(f"  DLQ gui/xac nhan/that bai: {s['dlq']['sent']} / {s['dlq']['delivered']} / {s['dlq']['failed']}")
    print(f"  Seq THIEU / TRUNG       : {seq['total_missing']} / {seq['total_duplicates']}"
          f"   (khong theo doi duoc seq: {c['untracked_seq']})")
    for pid, p in seq["per_producer"].items():
        extra = f"  thieu o: {p['missing_ranges']}" if p["missing"] else ""
        print(f"     - {pid:<8} max_seq={p['max_seq']} khac_nhau={p['unique']} "
              f"thieu={p['missing']} trung={p['duplicates']}{extra}")
    print(f"  Do tre (ms)             : p50={_fmt(lat['p50'])} p95={_fmt(lat['p95'])} "
          f"p99={_fmt(lat['p99'])} max={_fmt(lat['max'])}")
    print(f"  Khoang lang lon nhat    : {s['max_receive_gap_s']} s")
    if s["kafka_errors"]:
        print(f"  Kafka errors            : {s['kafka_errors']}")
    print(f"  Kiem tra doi chieu      : {s['checks']}")
    print(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="verify_consumer.py",
        description="TV2 - Consumer kiem chung (schema, seq thieu/trung, DLQ, do tre).",
    )
    p.add_argument("--bootstrap-servers", "-b",
                   default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                   metavar="HOST:PORT", help=f"Kafka bootstrap (mac dinh {DEFAULT_BOOTSTRAP})")
    p.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS, metavar="TOPIC",
                   help="Cac topic can doc (mac dinh web.clickstream web.transactions)")
    p.add_argument("--group-id", default=None,
                   help="group.id. Mac dinh: tu sinh moi moi lan chay (doc tu dau). "
                        "Dat co dinh neu muon tiep tuc tu offset da commit.")
    p.add_argument("--offset-reset", choices=["earliest", "latest"], default="earliest")
    p.add_argument("--idle-timeout", type=float, default=30.0, metavar="S",
                   help="Tu dung neu S giay khong co message moi (sau message dau). 0 = khong dung. "
                        "E5 (restart Kafka) nen dat >= 120 hoac 0.")
    p.add_argument("--start-timeout", type=float, default=120.0, metavar="S",
                   help="Tu dung neu S giay ma chua nhan duoc message nao. 0 = khong dung.")
    p.add_argument("--max-messages", type=int, default=0, metavar="N",
                   help="Dung sau khi nhan N message (0 = khong gioi han).")
    p.add_argument("--batch-size", type=int, default=500, help="So message toi da moi lan consume()")
    p.add_argument("--commit-every", type=int, default=5000, metavar="N",
                   help="Commit offset moi N message")
    p.add_argument("--commit-interval", type=float, default=5.0, metavar="S",
                   help="...hoac moi S giay (cai nao den truoc)")
    p.add_argument("--log-interval", type=float, default=5.0, metavar="S", help="Chu ky ghi log/timeline")
    p.add_argument("--out-dir", default="results", help="Thu muc ghi summary.json va timeline.csv")
    p.add_argument("--run-name", default=None, help="Ten lan chay (vd E6_run1), dung dat ten file ket qua")
    p.add_argument("--strict-uuid", action="store_true", help="Bat buoc event_id dung dinh dang UUID")
    p.add_argument("--no-total-check", action="store_true",
                   help="Bo kiem tra total_amount == tong qty*price")
    p.add_argument("--max-seq", type=int, default=MAX_SEQ,
                   help="seq lon hon gia tri nay khong duoc theo doi (bao ve bo nho)")
    p.add_argument("--verbose", "-v", action="store_true")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    sys.exit(run(args))


if __name__ == "__main__":
    main()
