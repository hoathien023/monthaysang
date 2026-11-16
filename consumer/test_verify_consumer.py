#!/usr/bin/env python3
"""
consumer/test_verify_consumer.py
--------------------------------
Test don gian cho verify_consumer.py, KHONG can Kafka dang chay.

Chay:
    python -m unittest discover -s consumer -v
    # hoac:
    python consumer/test_verify_consumer.py -v
"""

from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import verify_consumer as vc  # noqa: E402


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_click(pid="P01", seq=1, **over):
    ev = {
        "event_id": f"00000000-0000-0000-0000-{int(seq) if str(seq).isdigit() else 0:012d}",
        "producer_id": pid, "seq": seq, "event_type": "add_to_cart",
        "user_id": "U000123", "session_id": "S98765", "product_id": "SKU-0042",
        "category": "electronics", "device": "mobile", "url": "/product/SKU-0042",
        "ts_event": now_iso(),
    }
    ev.update(over)
    return ev


def make_tx(pid="P01", seq=1, **over):
    ev = {
        "event_id": f"11111111-0000-0000-0000-{int(seq) if str(seq).isdigit() else 0:012d}",
        "producer_id": pid, "seq": seq, "event_type": "order_created",
        "user_id": "U000123", "ts_event": now_iso(), "order_id": "O2026000123",
        "items": [{"product_id": "SKU-1", "qty": 2, "price": 10.5},
                  {"product_id": "SKU-2", "qty": 1, "price": 4.0}],
        "total_amount": 25.0, "currency": "USD", "payment_method": "card",
    }
    ev.update(over)
    return ev


def enc(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


class ValidateTests(unittest.TestCase):
    def check(self, obj, topic, expect_reason):
        reason, ts = vc.validate_event(obj, topic)
        if expect_reason is None:
            self.assertIsNone(reason)
            self.assertIsNotNone(ts)
        else:
            self.assertEqual(reason, expect_reason)
            self.assertIsNone(ts)

    def test_valid_click(self):
        self.check(make_click(), "web.clickstream", None)

    def test_valid_click_without_optional_fields(self):
        ev = make_click(event_type="search")
        del ev["product_id"], ev["category"]
        self.check(ev, "web.clickstream", None)

    def test_valid_tx(self):
        self.check(make_tx(), "web.transactions", None)

    def test_missing_required_field(self):
        ev = make_click()
        del ev["user_id"]
        self.check(ev, "web.clickstream", "missing_field:user_id")

    def test_null_counts_as_missing(self):
        self.check(make_click(session_id=None), "web.clickstream", "missing_field:session_id")

    def test_wrong_type_seq_string(self):
        self.check(make_click(seq="5"), "web.clickstream", "wrong_type:seq")

    def test_wrong_type_seq_bool(self):
        self.check(make_click(seq=True), "web.clickstream", "wrong_type:seq")

    def test_seq_zero(self):
        self.check(make_click(seq=0), "web.clickstream", "invalid_value:seq")

    def test_bad_event_type(self):
        reason, _ = vc.validate_event(make_click(event_type="hack"), "web.clickstream")
        self.assertTrue(reason.startswith("invalid_event_type"))

    def test_event_type_wrong_topic(self):
        reason, _ = vc.validate_event(make_tx(), "web.clickstream")
        self.assertTrue(reason.startswith("event_type_not_allowed_for_topic"))

    def test_bench_topic_infers_kind(self):
        self.check(make_tx(), "bench.p3", None)
        self.check(make_click(), "bench.p3", None)

    def test_bad_device(self):
        self.check(make_click(device="tv"), "web.clickstream", "invalid_value:device")

    def test_ts_without_timezone(self):
        self.check(make_click(ts_event="2026-11-20T08:15:30"), "web.clickstream", "ts_event_invalid_iso8601")

    def test_ts_garbage(self):
        self.check(make_click(ts_event="hom nay"), "web.clickstream", "ts_event_invalid_iso8601")

    def test_empty_items(self):
        self.check(make_tx(items=[]), "web.transactions", "empty_items")

    def test_qty_zero(self):
        ev = make_tx()
        ev["items"][0]["qty"] = 0
        self.check(ev, "web.transactions", "qty_lt_1")

    def test_price_negative(self):
        ev = make_tx()
        ev["items"][0]["price"] = -3
        self.check(ev, "web.transactions", "non_positive:price")

    def test_total_not_positive(self):
        self.check(make_tx(total_amount=0), "web.transactions", "non_positive:total_amount")

    def test_total_mismatch(self):
        self.check(make_tx(total_amount=999.0), "web.transactions", "total_amount_mismatch")

    def test_total_mismatch_skipped_by_flag(self):
        reason, _ = vc.validate_event(make_tx(total_amount=999.0), "web.transactions", check_total=False)
        self.assertIsNone(reason)

    def test_strict_uuid(self):
        reason, _ = vc.validate_event(make_click(event_id="abc"), "web.clickstream", strict_uuid=True)
        self.assertEqual(reason, "invalid_value:event_id")
        reason, _ = vc.validate_event(make_click(event_id="abc"), "web.clickstream", strict_uuid=False)
        self.assertIsNone(reason)

    def test_not_an_object(self):
        self.check([1, 2], "web.clickstream", "not_a_json_object")


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.dlq: list[tuple] = []
        self.engine = vc.VerifyEngine(lambda k, v: self.dlq.append((k, v)))

    def feed(self, obj, topic="web.clickstream", key="U000123"):
        raw = obj if isinstance(obj, bytes) else enc(obj)
        return self.engine.handle(topic, 0, 0, key.encode() if key else None, raw)

    def test_counts_reconcile(self):
        self.feed(make_click(seq=1))
        self.feed(make_click(seq=2, user_id=None))
        self.feed(b"{khong phai json")
        self.feed(make_tx(seq=3), topic="web.transactions")
        e = self.engine
        self.assertEqual((e.received, e.valid, e.invalid), (4, 2, 2))
        self.assertEqual(e.valid + e.invalid, e.received)
        self.assertEqual(len(self.dlq), 2)

    def test_dlq_record_content(self):
        bad = make_click(seq=7, user_id=None)
        self.feed(bad)
        key, value = self.dlq[0]
        rec = json.loads(value)
        self.assertEqual(key, bad["event_id"].encode())
        self.assertEqual(rec["error_reason"], "missing_field:user_id")
        self.assertEqual(rec["source_topic"], "web.clickstream")
        self.assertEqual(rec["payload"]["seq"], 7)
        self.assertIn("ts_failed", rec)

    def test_dlq_for_invalid_json_keeps_raw(self):
        self.feed(b"{oops")
        rec = json.loads(self.dlq[0][1])
        self.assertEqual(rec["payload"], "{oops")
        self.assertEqual(rec["error_reason"], "invalid_json")
        self.assertIsNone(self.dlq[0][0])

    def test_empty_payload(self):
        self.engine.handle("web.clickstream", 0, 0, None, None)
        self.assertEqual(self.engine.invalid_by_reason["empty_payload"], 1)

    def test_key_mismatch_counted(self):
        self.feed(make_click(seq=1), key="U999999")
        self.feed(make_click(seq=2), key=None)
        self.feed(make_click(seq=3))
        self.assertEqual(self.engine.key_mismatch, 2)

    def test_latency_uses_clock(self):
        ts = datetime(2026, 11, 20, 8, 0, 0, tzinfo=timezone.utc)
        eng = vc.VerifyEngine(lambda k, v: None, clock=lambda: ts.timestamp() + 0.25)
        eng.handle("web.clickstream", 0, 0, b"U000123",
                   enc(make_click(ts_event="2026-11-20T08:00:00.000Z")))
        self.assertAlmostEqual(eng.latency_ms[0], 250.0, places=3)


class SeqTests(unittest.TestCase):
    def test_gap_and_duplicate(self):
        """Case ban dau: 1..100, thieu seq 50, trung seq 70."""
        t = vc.SeqTracker()
        for s in range(1, 101):
            if s != 50:
                t.add("P01", s)
        t.add("P01", 70)
        r = t.report()
        p = r["per_producer"]["P01"]
        self.assertEqual((p["missing"], p["duplicates"]), (1, 1))
        self.assertEqual(p["missing_ranges"], [[50, 50]])
        self.assertEqual(r["total_missing"], 1)
        self.assertEqual(r["total_duplicates"], 1)

    def test_out_of_order_not_reported_missing(self):
        t = vc.SeqTracker()
        for s in (3, 1, 2, 5, 4):
            t.add("P01", s)
        p = t.report()["per_producer"]["P01"]
        self.assertEqual((p["missing"], p["duplicates"]), (0, 0))

    def test_producers_are_independent(self):
        t = vc.SeqTracker()
        for s in range(1, 11):
            t.add("P01", s)
        for s in (1, 2, 4):
            t.add("P02", s)
        r = t.report()["per_producer"]
        self.assertEqual(r["P01"]["missing"], 0)
        self.assertEqual(r["P02"]["missing"], 1)
        self.assertEqual(r["P02"]["missing_ranges"], [[3, 3]])

    def test_multiple_ranges(self):
        t = vc.SeqTracker()
        for s in (1, 2, 5, 6, 10):
            t.add("P01", s)
        self.assertEqual(t.report()["per_producer"]["P01"]["missing_ranges"], [[3, 4], [7, 9]])

    def test_leading_missing_counts(self):
        t = vc.SeqTracker()
        t.add("P01", 3)
        self.assertEqual(t.report()["per_producer"]["P01"]["missing"], 2)

    def test_huge_seq_ignored(self):
        t = vc.SeqTracker(max_seq=1000)
        self.assertEqual(t.add("P01", 10**9), "ignored")

    def test_bad_event_still_tracked_in_seq(self):
        """Su kien sai noi dung nhung co seq hop le van duoc tinh la 'da nhan'."""
        eng = vc.VerifyEngine(lambda k, v: None)
        eng.handle("web.clickstream", 0, 0, None, enc(make_click(seq=1)))
        eng.handle("web.clickstream", 0, 1, None, enc(make_click(seq=2, user_id=None)))
        eng.handle("web.clickstream", 0, 2, None, enc(make_click(seq=3)))
        r = eng.seq.report()
        self.assertEqual(r["total_missing"], 0)
        self.assertEqual(eng.invalid, 1)

    def test_wrong_type_seq_gap_is_explained(self):
        """E6: su kien co seq sai kieu da NHAN (vao DLQ) -> khong tinh la THIEU THAT."""
        eng = vc.VerifyEngine(lambda k, v: None)
        for s in (1, 2, 4, 5):
            eng.handle("web.clickstream", 0, s, None, enc(make_click(seq=s)))
        eng.handle("web.clickstream", 0, 3, None, enc(make_click(seq="3")))   # sai kieu
        r = eng.report()["seq"]
        self.assertEqual(r["total_missing"], 1)          # lo hong tho o seq 3
        self.assertEqual(r["untracked_events"], 1)
        self.assertEqual(r["missing_unexplained"], 0)    # nhung da duoc giai thich

    def test_real_gap_still_reported_with_untracked(self):
        eng = vc.VerifyEngine(lambda k, v: None)
        for s in (1, 2, 5):                               # thieu 3 va 4
            eng.handle("web.clickstream", 0, s, None, enc(make_click(seq=s)))
        eng.handle("web.clickstream", 0, 9, None, enc(make_click(seq="3")))   # chi che duoc 1
        r = eng.report()["seq"]
        self.assertEqual((r["total_missing"], r["missing_unexplained"]), (2, 1))

    def test_event_without_seq_is_untracked(self):
        eng = vc.VerifyEngine(lambda k, v: None)
        ev = make_click()
        del ev["seq"]
        eng.handle("web.clickstream", 0, 0, None, enc(ev))
        self.assertEqual(eng.untracked_seq, 1)


class MetricsTests(unittest.TestCase):
    def test_percentiles(self):
        p = vc.percentiles(list(range(1, 101)), (50, 95, 99))
        self.assertAlmostEqual(p[50], 50.5)
        self.assertAlmostEqual(p[95], 95.05)
        self.assertIsNone(vc.percentiles([], (50,))[50])

    def test_throughput(self):
        self.assertEqual(vc.throughput(1000, 2.0), 500.0)
        self.assertEqual(vc.throughput(10, 0), 0.0)


# ───────────── Kafka gia de chay thu vong lap chinh khong can broker ─────────────

class FakeError:
    def __init__(self, name="_TRANSPORT"):
        self._n = name
    def name(self): return self._n
    def fatal(self): return False
    def code(self): return -1
    def __str__(self): return self._n


class FakeMsg:
    def __init__(self, topic, partition, offset, key, value, error=None):
        self._a = (topic, partition, offset, key, value)
        self._e = error
    def error(self): return self._e
    def topic(self): return self._a[0]
    def partition(self): return self._a[1]
    def offset(self): return self._a[2]
    def key(self): return self._a[3]
    def value(self): return self._a[4]


class FakeKafka:
    """Thay the confluent_kafka: Consumer tra het du lieu roi im lang."""
    messages: list = []
    produced: list = []
    committed = 0
    stored: list = []

    class KafkaException(Exception):
        pass

    class KafkaError:
        _NO_OFFSET = -168

    class TopicPartition:
        def __init__(self, t, p, o):
            self.topic, self.partition, self.offset = t, p, o

    class Producer:
        def __init__(self, conf): self._cbs = []
        def produce(self, topic, key=None, value=None, on_delivery=None):
            FakeKafka.produced.append((topic, key, value))
            self._cbs.append(on_delivery)
        def poll(self, t=0): self._deliver()
        def flush(self, t=0):
            self._deliver()
            return 0
        def _deliver(self):
            while self._cbs:
                cb = self._cbs.pop()
                if cb: cb(None, None)

    class Consumer:
        def __init__(self, conf): self.conf = conf
        def list_topics(self, timeout=0):
            return SimpleNamespace(topics={"web.clickstream": 1, "web.transactions": 1}, brokers={1: 1})
        def subscribe(self, topics): pass
        def consume(self, num_messages=1, timeout=1.0):
            if FakeKafka.messages:
                batch = FakeKafka.messages[:num_messages]
                del FakeKafka.messages[:num_messages]
                return batch
            time.sleep(0.05)
            return []
        def store_offsets(self, offsets=None, message=None):
            FakeKafka.stored.extend((o.topic, o.partition, o.offset) for o in offsets)
        def commit(self, asynchronous=False): FakeKafka.committed += 1
        def close(self): pass


class RunLoopTests(unittest.TestCase):
    def test_full_loop_with_fake_kafka(self):
        FakeKafka.produced, FakeKafka.stored, FakeKafka.committed = [], [], 0
        msgs = []
        off = 0
        for s in range(1, 101):                    # P01: 1..100, thieu 50
            if s == 50:
                continue
            ev = make_click(seq=s)
            if s == 10:
                del ev["user_id"]                  # loi 1
            msgs.append(FakeMsg("web.clickstream", s % 3, off, b"U000123", enc(ev)))
            off += 1
        msgs.append(FakeMsg("web.clickstream", 0, off, b"U000123", enc(make_click(seq=70))))   # trung
        msgs.append(FakeMsg("web.clickstream", 0, off + 1, None, b"{hong"))                    # loi 2
        msgs.append(FakeMsg("web.clickstream", 0, off + 2, None, None, error=FakeError()))     # loi Kafka
        FakeKafka.messages = msgs

        with tempfile.TemporaryDirectory() as tmp:
            args = vc.parse_args(["--out-dir", tmp, "--run-name", "unit", "--idle-timeout", "0.3",
                                  "--start-timeout", "5", "--log-interval", "0.2"])
            code = vc.run(args, kafka=FakeKafka)
            self.assertEqual(code, 0)
            with open(os.path.join(tmp, "consumer_unit_summary.json"), encoding="utf-8") as f:
                s = json.load(f)
            self.assertTrue(os.path.exists(os.path.join(tmp, "consumer_unit_timeline.csv")))

        self.assertEqual(s["stop_reason"], "idle_timeout")
        self.assertEqual(s["counts"]["received"], 101)             # 99 + 1 trung + 1 json hong
        self.assertEqual(s["counts"]["invalid"], 2)
        self.assertEqual(s["counts"]["valid"], 99)
        self.assertEqual(s["seq"]["total_missing"], 1)
        self.assertEqual(s["seq"]["total_duplicates"], 1)
        self.assertEqual(s["dlq"]["sent"], 2)
        self.assertEqual(len(FakeKafka.produced), 2)
        self.assertTrue(all(t == "web.events.dlq" for t, _, _ in FakeKafka.produced))
        self.assertEqual(s["kafka_errors"], {"_TRANSPORT": 1})
        self.assertTrue(all(s["checks"].values()))
        self.assertGreaterEqual(FakeKafka.committed, 1)
        self.assertTrue(FakeKafka.stored)

    def test_refuses_to_read_dlq(self):
        args = vc.parse_args(["--topics", "web.events.dlq"])
        self.assertEqual(vc.run(args, kafka=FakeKafka), 1)


if __name__ == "__main__":
    unittest.main()
