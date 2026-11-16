"""Doi chieu 3 thu phai khop nhau: JSON Schema (schemas/*.json), bo kiem tra chay that (common/validate.py)
va generator (su kien hop le / su kien bi tiem loi co nhan).
Chay: python -m src.generator.schema_check     (can thu vien jsonschema, co trong requirements.txt)"""
import json

from jsonschema import Draft202012Validator

from common.validate import validate
from src.generator.generator import Generator

SCHEMAS = {}
for topic, path in (("web.clickstream", "schemas/clickstream.schema.json"),
                    ("web.transactions", "schemas/transaction.schema.json")):
    with open(path, encoding="utf-8") as f:
        schema = json.load(f)
    Draft202012Validator.check_schema(schema)  # chinh file schema phai hop le
    SCHEMAS[topic] = Draft202012Validator(schema)
print("2 file schema hop le")

N = 30000
gen = Generator("P01", seed=11)
ok = bad = 0
mismatch = []  # truong hop JSON Schema va validate.py cho ket qua khac nhau
wrong = []     # truong hop ket qua khong dung nhan cua generator
for _ in range(N):
    topic, key, ev, label = gen.next_event(txn_ratio=0.3, bad_ratio=0.2)
    schema_ok = not any(True for _ in SCHEMAS[topic].iter_errors(ev))
    runtime_ok = validate(ev, topic) is None
    if schema_ok != runtime_ok:
        mismatch.append((label, ev))
    expected_ok = label is None
    if schema_ok != expected_ok or runtime_ok != expected_ok:
        wrong.append((label, schema_ok, runtime_ok))
    ok += expected_ok
    bad += not expected_ok
print(f"{N} su kien: {ok} hop le, {bad} bi tiem loi")
print(f"JSON Schema khac validate.py: {len(mismatch)} | ket qua sai nhan: {len(wrong)}")
assert not mismatch and not wrong, (mismatch[:2], wrong[:2])
print("SCHEMA CHECK OK")
