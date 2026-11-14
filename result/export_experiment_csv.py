
#!/usr/bin/env python3
"""
results/export_experiment_csv.py

Chuyển summary.json của verify_consumer.py thành CSV
chuẩn dùng chung cho E1, E5, E6.

Ví dụ:
python results/export_experiment_csv.py `
  --experiment E1 `
  --run 1 `
  --params '{"partitions":1,"producers":1,"target_events":500000}' `
  --summary results/consumer_E1_p1_run1_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any


FIELDS = [
    "experiment",
    "run",
    "params",
    "events",
    "duration_s",
    "throughput_eps",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "missing",
    "duplicates",
]


def load_summary(path: Path) -> dict[str, Any]:
    """Đọc summary JSON do Consumer xuất ra."""
    if not path.is_file():
        raise FileNotFoundError(f"Không tìm thấy: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    required = ["counts", "latency_ms", "seq",
                "active_duration_s", "throughput_eps"]

    for key in required:
        if key not in data:
            raise ValueError(f"Summary thiếu trường: {key}")

    return data


def build_row(
    experiment: str,
    run: int,
    params: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:

    counts = summary["counts"]
    latency = summary["latency_ms"]
    seq = summary["seq"]

    return {
        "experiment": experiment,
        "run": run,
        "params": json.dumps(
            params, ensure_ascii=False, separators=(",", ":")
        ),
        "events": counts["received"],
        "duration_s": summary["active_duration_s"],
        "throughput_eps": summary["throughput_eps"],
        "p50_ms": latency["p50"],
        "p95_ms": latency["p95"],
        "p99_ms": latency["p99"],
        "missing": seq["total_missing"],
        "duplicates": seq["total_duplicates"],
    }


def append_csv(path: Path, row: dict[str, Any]) -> None:
    """Ghi nối tiếp CSV, chỉ tạo header khi file chưa tồn tại."""
    path.parent.mkdir(parents=True, exist_ok=True)

    write_header = not path.exists() or path.stat().st_size == 0

    with path.open("a", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)

        if write_header:
            writer.writeheader()

        writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Xuất kết quả Consumer sang CSV thí nghiệm."
    )

    parser.add_argument(
        "--experiment",
        required=True,
        choices=["E1", "E5", "E6"],
    )

    parser.add_argument("--run", required=True, type=int)

    parser.add_argument(
        "--params",
        required=True,
        help="Thông số chạy dưới dạng JSON."
    )

    parser.add_argument("--summary", required=True, type=Path)

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/experiments.csv"),
    )

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()

    if args.run < 1:
        raise ValueError("--run phải >= 1")

    try:
        params = json.loads(args.params)

        if not isinstance(params, dict):
            raise ValueError("--params phải là một JSON object")

        summary = load_summary(args.summary)

        row = build_row(
            args.experiment,
            args.run,
            params,
            summary,
        )

        append_csv(args.output, row)

        logging.info("Đã xuất kết quả thành công.")
        logging.info("File: %s", args.output)
        logging.info("Experiment: %s - Run %d",
                     args.experiment, args.run)

        return 0

    except (ValueError, KeyError, OSError, json.JSONDecodeError) as exc:
        logging.error("Không thể xuất kết quả: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
