"""Kiem tra su kien theo data contract v1.0 (khop voi schemas/*.json).
Viet tay thay vi dung jsonschema vi nhanh hon nhieu khi xu ly hang nghin su kien/giay.
validate(event, topic) -> None neu hop le, nguoc lai tra ve chuoi ly do loi."""
import re

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
CLICK_TYPES = {"page_view", "search", "click", "add_to_cart"}
TXN_TYPES = {"order_created", "payment_success", "payment_failed"}
DEVICES = {"mobile", "desktop", "tablet"}
CURRENCIES = {"VND", "USD"}
PAYMENTS = {"card", "wallet", "cod", "bank_transfer"}

COMMON_REQ = ["event_id", "producer_id", "seq", "event_type", "user_id", "ts_event"]
CLICK_REQ = COMMON_REQ + ["session_id", "device", "url"]
TXN_REQ = COMMON_REQ + ["order_id", "items", "total_amount", "currency", "payment_method"]


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _str(ev, key):
    v = ev.get(key)
    return isinstance(v, str) and len(v) > 0


def validate(ev, topic):
    if not isinstance(ev, dict):
        return "not_an_object"
    is_click = topic.endswith("clickstream") or topic in ("bench.p1", "bench.p3", "bench.p6")
    req = CLICK_REQ if is_click else TXN_REQ
    for k in req:
        if k not in ev:
            return f"missing_field:{k}"
    for k in ("event_id", "producer_id", "user_id"):
        if not _str(ev, k):
            return f"wrong_type:{k}"
    if not _is_int(ev["seq"]) or ev["seq"] < 1:
        return "wrong_type:seq"
    if not isinstance(ev["ts_event"], str) or not TS_RE.match(ev["ts_event"]):
        return "bad_timestamp"
    if is_click:
        if ev["event_type"] not in CLICK_TYPES:
            return "unknown_event_type"
        if not _str(ev, "session_id") or not _str(ev, "url"):
            return "wrong_type:session_id_or_url"
        if ev["device"] not in DEVICES:
            return "invalid_device"
        return None
    # transaction
    if ev["event_type"] not in TXN_TYPES:
        return "unknown_event_type"
    if not _str(ev, "order_id"):
        return "wrong_type:order_id"
    items = ev["items"]
    if not isinstance(items, list) or len(items) == 0:
        return "empty_items"
    total = 0.0
    for it in items:
        if not isinstance(it, dict) or not _str(it, "product_id"):
            return "invalid_item"
        if not _is_int(it.get("qty")) or it["qty"] < 1:
            return "invalid_qty"
        if not _is_num(it.get("price")) or it["price"] <= 0:
            return "negative_amount"
        total += it["qty"] * it["price"]
    if not _is_num(ev["total_amount"]) or ev["total_amount"] <= 0:
        return "negative_amount"
    if abs(ev["total_amount"] - total) > 0.01:
        return "total_mismatch"
    if ev["currency"] not in CURRENCIES:
        return "invalid_currency"
    if ev["payment_method"] not in PAYMENTS:
        return "invalid_payment_method"
    return None
