"""Generator su kien thuong mai dien tu (clickstream + transaction).
- Cung seed -> cung noi dung (tru ts_event la gio that luc sinh).
- seq tang dan lien tuc theo tung producer_id (de phat hien thieu/trung).
- Co the tiem loi co chu dich, moi loi co nhan (de doi chieu voi DLQ o thi nghiem E6).

Chay thu (sinh file mau):
  python -m src.generator.generator --n 1000 --seed 42 --bad-ratio 0.05 --out data/sample/sample.jsonl
Sinh du lieu lon (hon 1.000.000 su kien, khoang 18 giay, ~390 MB; thu muc data/large/ khong dua len git):
  python -m src.generator.generator --n 1000000 --seed 1 --out data/large/big.jsonl
Moi dong file: {"topic", "key", "event", "injected_error"}; injected_error = nhan loi (null neu su kien hop le).
Ngoai file du lieu con co <out>.summary.json (so su kien, so loi da tiem theo loai).
"""
import argparse, json, os, random, sys, uuid
from collections import Counter
from datetime import datetime, timezone

CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "books", "toys", "grocery"]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_W = [0.6, 0.3, 0.1]
CLICK_TYPES = ["page_view", "search", "click", "add_to_cart"]
CLICK_W = [0.55, 0.15, 0.2, 0.1]
TXN_TYPES = ["order_created", "payment_success", "payment_failed"]
TXN_W = [0.5, 0.42, 0.08]
PAYMENTS = ["card", "wallet", "cod", "bank_transfer"]

BAD_KINDS_CLICK = ["missing_field", "wrong_type", "unknown_event_type", "bad_timestamp"]
BAD_KINDS_TXN = ["missing_field", "wrong_type", "unknown_event_type", "bad_timestamp",
                 "negative_amount", "empty_items"]


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Generator:
    def __init__(self, producer_id="P01", seed=42, n_users=5000, n_products=2000):
        self.pid = producer_id
        self.rng = random.Random(seed)
        self.n_users = n_users
        self.n_products = n_products
        self.seq = 0
        self.order_no = 0
        self.sessions = {}  # user_idx -> session_id

    def _uuid(self):
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _user(self):
        # phan bo lech: mot so nguoi dung hoat dong nhieu hon
        idx = min(int(self.rng.paretovariate(1.2)) - 1, self.n_users - 1) if self.rng.random() < 0.3 \
            else self.rng.randrange(self.n_users)
        return idx, f"U{idx:06d}"

    def _session(self, idx):
        if idx not in self.sessions or self.rng.random() < 0.05:
            self.sessions[idx] = f"S{self.rng.randrange(10**6):06d}"
        return self.sessions[idx]

    def _product(self):
        return f"SKU-{self.rng.randrange(self.n_products):04d}"

    def _next_seq(self):
        self.seq += 1
        return self.seq

    def clickstream(self):
        idx, uid = self._user()
        etype = self.rng.choices(CLICK_TYPES, CLICK_W)[0]
        ev = {
            "event_id": self._uuid(), "producer_id": self.pid, "seq": self._next_seq(),
            "event_type": etype, "user_id": uid, "session_id": self._session(idx),
            "device": self.rng.choices(DEVICES, DEVICE_W)[0], "ts_event": now_iso(),
        }
        if etype == "search":
            ev["url"] = "/search?q=" + self.rng.choice(["ao", "giay", "tai nghe", "laptop", "son", "sach"])
        else:
            pid = self._product()
            ev["product_id"] = pid
            ev["category"] = self.rng.choice(CATEGORIES)
            ev["url"] = f"/product/{pid}"
        return ev

    def transaction(self):
        idx, uid = self._user()
        self.order_no += 1
        currency = self.rng.choices(["VND", "USD"], [0.85, 0.15])[0]
        items, total = [], 0.0
        for _ in range(self.rng.randint(1, 4)):
            qty = self.rng.randint(1, 3)
            price = self.rng.randrange(50, 3000) * 1000 if currency == "VND" \
                else round(self.rng.uniform(2, 150), 2)
            items.append({"product_id": self._product(), "qty": qty, "price": price})
            total += qty * price
        return {
            "event_id": self._uuid(), "producer_id": self.pid, "seq": self._next_seq(),
            "event_type": self.rng.choices(TXN_TYPES, TXN_W)[0], "user_id": uid,
            "order_id": f"O{self.pid}{self.order_no:09d}", "items": items,
            "total_amount": round(total, 2), "currency": currency,
            "payment_method": self.rng.choice(PAYMENTS), "ts_event": now_iso(),
        }

    def corrupt(self, ev, topic):
        """Tra ve (su_kien_loi, nhan_loi). seq van duoc giu (de van dem duoc la da gui)."""
        ev = json.loads(json.dumps(ev))
        kinds = BAD_KINDS_TXN if topic == "web.transactions" else BAD_KINDS_CLICK
        kind = self.rng.choice(kinds)
        if kind == "missing_field":
            ev.pop(self.rng.choice(["user_id", "event_type", "ts_event"]))
        elif kind == "wrong_type":
            ev["seq"] = "abc"
        elif kind == "unknown_event_type":
            ev["event_type"] = "hack_the_planet"
        elif kind == "bad_timestamp":
            ev["ts_event"] = "yesterday-ish"
        elif kind == "negative_amount":
            ev["total_amount"] = -abs(ev["total_amount"])
        elif kind == "empty_items":
            ev["items"] = []
        return ev, kind

    def next_event(self, txn_ratio=0.1, bad_ratio=0.0):
        """Tra ve (topic, key, event, nhan_loi_hoac_None)."""
        topic = "web.transactions" if self.rng.random() < txn_ratio else "web.clickstream"
        ev = self.transaction() if topic == "web.transactions" else self.clickstream()
        label = None
        if bad_ratio > 0 and self.rng.random() < bad_ratio:
            ev, label = self.corrupt(ev, topic)
        return topic, ev.get("user_id", ""), ev, label


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--producer-id", default="P01")
    ap.add_argument("--txn-ratio", type=float, default=0.1)
    ap.add_argument("--bad-ratio", type=float, default=0.0)
    ap.add_argument("--out", default="data/sample/sample.jsonl")
    a = ap.parse_args()

    gen = Generator(a.producer_id, a.seed)
    bad = Counter()
    per_topic = Counter()
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        for _ in range(a.n):
            topic, key, ev, label = gen.next_event(a.txn_ratio, a.bad_ratio)
            f.write(json.dumps({"topic": topic, "key": key, "event": ev, "injected_error": label},
                               ensure_ascii=False) + "\n")
            per_topic[topic] += 1
            if label:
                bad[label] += 1
    summary = {"file": a.out, "events": a.n, "seed": a.seed, "producer_id": a.producer_id,
               "bad_ratio": a.bad_ratio, "per_topic": dict(per_topic),
               "injected_total": sum(bad.values()), "injected_by_kind": dict(bad)}
    with open(a.out + ".summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"Da ghi {a.n} su kien vao {a.out}; so loi da tiem: {sum(bad.values())} {dict(bad)}")
    print(f"  tom tat: {a.out}.summary.json")


if __name__ == "__main__":
    main()
