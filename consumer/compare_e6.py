#!/usr/bin/env python3
"""
consumer/compare_e6.py
----------------------
Doi chieu thi nghiem E6 (phat hien du lieu loi):
  manifest cua bo tao tai (so loi DA TIEM)  <->  summary cua Consumer (so loi DA BAT).

Tieu chi dat theo ke hoach:
  - so ban ghi trong DLQ khop so da tiem
  - hop le + loi = tong da gui
Kiem tra them (chat hon): khop TUNG ly do loi, khong mat/trung seq.

Cach dung:
    python consumer/compare_e6.py \
        --manifest results/producer_E6_run1_manifest.json \
        --summary  results/consumer_E6_run1_summary.json
Exit code: 0 = DAT tat ca, 1 = co muc KHONG DAT.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def load(path: Path) -> dict:
    if not path.is_file():
        sys.exit(f"Khong tim thay file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def compare(manifest: dict, summary: dict) -> list[tuple[str, bool, str]]:
    c = summary["counts"]
    seq = summary["seq"]
    dlq = summary["dlq"]
    inj = manifest["bad_injected"]
    sent = manifest["sent"]
    rows: list[tuple[str, bool, str]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        rows.append((name, ok, detail))

    add("Producer gui khong loi (failed = 0)", manifest["failed"] == 0 and manifest["not_flushed"] == 0,
        f"failed={manifest['failed']} not_flushed={manifest['not_flushed']}")
    add("Consumer nhan du: received == tong da gui", c["received"] == sent,
        f"received={c['received']} sent={sent}")
    add("hop le + loi == tong da gui", c["valid"] + c["invalid"] == sent,
        f"{c['valid']} + {c['invalid']} = {c['valid'] + c['invalid']} (sent={sent})")
    add("loi Consumer bat == loi da tiem", c["invalid"] == inj,
        f"invalid={c['invalid']} injected={inj}")
    add("DLQ gui == loi da tiem", dlq["sent"] == inj, f"dlq_sent={dlq['sent']} injected={inj}")
    add("DLQ xac nhan het (delivered == sent, failed = 0)",
        dlq["delivered"] == dlq["sent"] and dlq["failed"] == 0,
        f"delivered={dlq['delivered']} failed={dlq['failed']}")

    got = summary["invalid_by_reason"]
    want = manifest["bad_by_reason"]
    for reason in sorted(set(want) | set(got)):
        add(f"  ly do '{reason}'", want.get(reason, 0) == got.get(reason, 0),
            f"tiem={want.get(reason, 0)} bat={got.get(reason, 0)}")

    missing_real = seq.get("missing_unexplained", seq["total_missing"])
    add("Khong thieu seq (da tru su kien loi khong doc duoc seq)", missing_real == 0,
        f"missing_unexplained={missing_real} (tho={seq['total_missing']}, "
        f"khong doc duoc seq={seq.get('untracked_events', 0)})")
    add("Khong trung seq", seq["total_duplicates"] == 0, f"duplicates={seq['total_duplicates']}")
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Doi chieu E6: loi da tiem vs loi Consumer bat/DLQ.")
    ap.add_argument("--manifest", required=True, type=Path, help="producer_*_manifest.json")
    ap.add_argument("--summary", required=True, type=Path, help="consumer_*_summary.json")
    args = ap.parse_args()

    manifest, summary = load(args.manifest), load(args.summary)
    rows = compare(manifest, summary)

    print(f"\nE6: {args.manifest.name}  <->  {args.summary.name}")
    print("-" * 78)
    for name, ok, detail in rows:
        print(f"  [{'DAT ' if ok else 'LOI '}] {name:<52} {detail}")
    print("-" * 78)
    failed = [n for n, ok, _ in rows if not ok]
    if failed:
        print(f"KET QUA: KHONG DAT ({len(failed)}/{len(rows)} muc)\n")
        return 1
    print(f"KET QUA: DAT ({len(rows)}/{len(rows)} muc)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
