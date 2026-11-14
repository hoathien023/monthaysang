"""Tu kiem tra: su kien hop le phai qua validate(); su kien bi tiem loi phai bi validate() bat.
Chay: python -m src.generator.selftest"""
from common.validate import validate
from src.generator.generator import Generator

gen = Generator("P01", seed=7)
n_ok = n_bad = missed = false_alarm = 0
seqs = []
for _ in range(20000):
    topic, key, ev, label = gen.next_event(txn_ratio=0.2, bad_ratio=0.1)
    err = validate(ev, topic)
    if label is None:
        n_ok += 1
        false_alarm += err is not None
        seqs.append(ev["seq"])
    else:
        n_bad += 1
        missed += err is None
assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs), "seq khong tang dan"
print(f"hop le: {n_ok} (bao nham: {false_alarm}) | co loi tiem: {n_bad} (bo sot: {missed})")
assert false_alarm == 0 and missed == 0
print("SELFTEST OK")
