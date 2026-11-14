"""Tu kiem tra phan logic cua API (khong can Kafka, khong can FastAPI).
Chay: python -m src.api.selftest"""
import json
import threading

from common.validate import validate
from src.api.core import Stamper, route, to_message
from src.generator.generator import Generator

# 1) Gan producer_id/seq khi client khong gui; giu nguyen neu client da gui
s = Stamper()
e1 = s.stamp({"event_type": "page_view", "user_id": "U1"})
e2 = s.stamp({"event_type": "payment_success", "user_id": "U2"})
e3 = s.stamp({"event_type": "x", "producer_id": "C01", "seq": 7})
assert e1["producer_id"] == "API01" and e1["seq"] == 1 and e2["seq"] == 2
assert e3["producer_id"] == "C01" and e3["seq"] == 7
assert e1["event_id"] and e1["ts_event"].endswith("Z")

# 2) seq khong trung khi nhieu thread cung gan (8 thread x 1000 su kien)
s = Stamper()
seqs, lock = [], threading.Lock()
def work():
    local = [s.stamp({"event_type": "click"})["seq"] for _ in range(1000)]
    with lock:
        seqs.extend(local)
ths = [threading.Thread(target=work) for _ in range(8)]
[t.start() for t in ths]; [t.join() for t in ths]
assert sorted(seqs) == list(range(1, 8001)), "seq bi trung hoac thieu"

# 3) Chon topic theo event_type; loai la -> clickstream (consumer se dua vao DLQ)
assert route({"event_type": "page_view"}) == "web.clickstream"
assert route({"event_type": "order_created"}) == "web.transactions"
assert route({"event_type": "hack"}) == "web.clickstream"
assert route({}) == "web.clickstream"

# 4) Thong diep gui di: key = user_id (None neu thieu), value la JSON UTF-8
t, k, v = to_message({"event_type": "click", "user_id": "U9", "url": "/ti\u1ebfng-vi\u1ec7t"})
assert k == "U9" and json.loads(v.decode("utf-8"))["url"] == "/ti\u1ebfng-vi\u1ec7t"
assert to_message({"event_type": "click"})[1] is None

# 5) Su kien hop le tu generator, sau khi di qua API (Stamper + route), van hop le theo data contract
gen = Generator("C01", seed=5)
for _ in range(2000):
    topic, key, ev, label = gen.next_event(0.3, 0.0)
    topic2, key2, value = to_message(Stamper().stamp(ev))
    assert topic2 == topic and key2 == key
    assert validate(json.loads(value), topic2) is None
print("API SELFTEST OK")
