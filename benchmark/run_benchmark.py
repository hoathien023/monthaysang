"""benchmark/run_benchmark.py

Bo Benchmark Runner thong nhat cua TV3, doc cau hinh tu benchmark/config.json.
Khong sua bat ky code nao cua TV1 hay TV2; truyen --bootstrap localhost:29092
de ket noi dung Kafka Docker cua TV2.

Vi du:
  python -m benchmark.run_benchmark --only BASELINE
  python -m benchmark.run_benchmark --only E4
  python -m benchmark.run_benchmark --only E1 E4 E5 E6
  python -m benchmark.run_benchmark --only E4 --reps 3
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

CSV_COLS = [
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

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CFG = ROOT / "benchmark" / "config.json"


def append_csv(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        if new_file:
            w.writeheader()
        w.writerow(row)


def recreate_bench_topics(bootstrap: str) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "infra" / "create_topics.py"),
        "--bootstrap-servers",
        bootstrap,
        "--recreate",
    ]
    subprocess.run(cmd, cwd=ROOT, check=False, capture_output=True, text=True)
    time.sleep(1.5)
    ensure_topics(bootstrap)


def ensure_topics(bootstrap: str) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "infra" / "create_topics.py"),
        "--bootstrap-servers",
        bootstrap,
    ]
    subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True)


def run_baseline(cfg: dict[str, Any], reps: int) -> None:
    b = cfg["baseline"]
    out_csv = ROOT / "results" / "BASELINE.csv"
    if out_csv.exists():
        out_csv.unlink()
    ev_dir = ROOT / "evidence"
    ev_dir.mkdir(parents=True, exist_ok=True)
    log_lines: list[str] = []

    for rep in range(1, reps + 1):
        recreate_bench_topics(cfg["bootstrap"])
        print(f"[BASELINE] lan {rep}/{reps} ({b['num_records']} records, {b['record_size']}B)...")
        cmd = [
            "docker",
            "exec",
            "kafka",
            "/opt/kafka/bin/kafka-producer-perf-test.sh",
            "--topic",
            b["topic"],
            "--num-records",
            str(b["num_records"]),
            "--record-size",
            str(b["record_size"]),
            "--throughput",
            "-1",
            "--producer-props",
            "bootstrap.servers=localhost:9092",
            f"acks={b['acks']}",
            f"linger.ms={b['linger_ms']}",
            f"batch.size={b['batch_size']}",
        ]
        t0 = time.perf_counter()
        res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        elapsed = round(time.perf_counter() - t0, 3)
        full_out = (res.stdout or "") + "\n" + (res.stderr or "")
        log_lines.append(f"=== BASELINE run {rep} ===\n{full_out}\n")

        # Parse final summary line from kafka-producer-perf-test.sh:
        # e.g. 200000 records sent, 85470.085470 records/sec (30.97 MB/sec), 112.45 ms avg latency, ... 210 ms 50th, 320 ms 95th, 410 ms 99th.
        lines = [ln.strip() for ln in full_out.splitlines() if "records/sec" in ln]
        summary_ln = lines[-1] if lines else ""
        m_eps = re.search(r"([\d.]+)\s+records/sec", summary_ln)
        m_p50 = re.search(r"([\d.]+)\s+ms\s+50th", summary_ln)
        m_p95 = re.search(r"([\d.]+)\s+ms\s+95th", summary_ln)
        m_p99 = re.search(r"([\d.]+)\s+ms\s+99th", summary_ln)
        eps = round(float(m_eps.group(1)), 1) if m_eps else 0.0
        p50 = round(float(m_p50.group(1)), 2) if m_p50 else None
        p95 = round(float(m_p95.group(1)), 2) if m_p95 else None
        p99 = round(float(m_p99.group(1)), 2) if m_p99 else None

        row = {
            "experiment": "BASELINE",
            "run": rep,
            "params": json.dumps(
                {
                    "tool": "kafka-producer-perf-test",
                    "topic": b["topic"],
                    "record_size": b["record_size"],
                    "acks": b["acks"],
                },
                separators=(",", ":"),
            ),
            "events": b["num_records"],
            "duration_s": elapsed,
            "throughput_eps": eps,
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "missing": 0,
            "duplicates": 0,
        }
        append_csv(out_csv, row)
        print(f"   -> {eps} records/s | p50={p50} p95={p95} p99={p99} ms")
        time.sleep(cfg["pause_between_runs_s"])

    (ev_dir / "BASELINE_perf_test.log").write_text("\n".join(log_lines), encoding="utf-8")


def run_e1(cfg: dict[str, Any], reps: int) -> None:
    e1 = cfg["E1"]
    bootstrap = cfg["bootstrap"]
    out_csv = ROOT / "results" / "E1.csv"
    if out_csv.exists():
        out_csv.unlink()
    runs_dir = ROOT / "results" / "runs_tv3"
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_lines: list[str] = []

    for item in e1["topics"]:
        topic = item["topic"]
        parts = item["partitions"]
        for rep in range(1, reps + 1):
            recreate_bench_topics(bootstrap)
            rid = f"E1_p{parts}_r{rep}"
            print(f"[E1] topic={topic} (P={parts}), n={e1['num_events']}, lan {rep}/{reps}...")
            cmd = [
                sys.executable,
                "-m",
                "src.producer.producer",
                "--bootstrap",
                bootstrap,
                "--topic",
                topic,
                "--n",
                str(e1["num_events"]),
                "--producers",
                "1",
                "--run-id",
                rid,
            ]
            res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
            log_lines.append(f"=== {rid} ===\n{res.stdout}\n{res.stderr}\n")
            run_json = ROOT / "results" / "runs" / f"{rid}.json"
            if not run_json.exists():
                print(f"   ! Khong tim thay {run_json}")
                continue
            d = json.loads(run_json.read_text(encoding="utf-8"))
            al = d.get("ack_latency_ms", {})
            row = {
                "experiment": "E1",
                "run": rep,
                "params": json.dumps(
                    {"topic": topic, "partitions": parts, "producers": 1, "target_events": e1["num_events"]},
                    separators=(",", ":"),
                ),
                "events": d["delivered"],
                "duration_s": d["wall_seconds"],
                "throughput_eps": d["throughput_delivered_eps"],
                "p50_ms": al.get("p50"),
                "p95_ms": al.get("p95"),
                "p99_ms": al.get("p99"),
                "missing": d["failed"],
                "duplicates": 0,
            }
            append_csv(out_csv, row)
            print(f"   -> {row['throughput_eps']} eps | wall={row['duration_s']}s | ack_p95={row['p95_ms']}ms")
            time.sleep(cfg["pause_between_runs_s"])

    (ROOT / "evidence" / "E1_run.log").write_text("\n".join(log_lines), encoding="utf-8")


def run_e4(cfg: dict[str, Any], reps: int) -> None:
    e4 = cfg["E4"]
    bootstrap = cfg["bootstrap"]
    out_csv = ROOT / "results" / "E4.csv"
    if out_csv.exists():
        out_csv.unlink()
    runs_dir = ROOT / "results" / "runs_tv3"
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_lines: list[str] = []

    for rate in e4["rates_eps"]:
        rate_label = "max" if rate == 0 else str(rate)
        for rep in range(1, reps + 1):
            recreate_bench_topics(bootstrap)
            rid = f"E4_rate_{rate_label}_r{rep}"
            print(f"[E4] rate={rate_label} eps, duration={e4['duration_s']}s, lan {rep}/{reps}...")

            cons_cmd = [
                sys.executable,
                str(ROOT / "consumer" / "verify_consumer.py"),
                "--bootstrap-servers",
                bootstrap,
                "--topics",
                e4["topic"],
                "--offset-reset",
                "earliest",
                "--idle-timeout",
                "8",
                "--start-timeout",
                "30",
                "--out-dir",
                str(runs_dir),
                "--run-name",
                rid,
            ]
            cons_proc = subprocess.Popen(cons_cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            time.sleep(3)

            prod_cmd = [
                sys.executable,
                "-m",
                "src.producer.producer",
                "--bootstrap",
                bootstrap,
                "--topic",
                e4["topic"],
                "--rate",
                str(rate),
                "--duration",
                str(e4["duration_s"]),
                "--producers",
                "1",
                "--run-id",
                rid,
            ]
            prod_res = subprocess.run(prod_cmd, cwd=ROOT, capture_output=True, text=True)
            cons_out, cons_err = cons_proc.communicate(timeout=120)
            log_lines.append(
                f"=== {rid} ===\n-- PRODUCER --\n{prod_res.stdout}\n{prod_res.stderr}\n-- CONSUMER --\n{cons_out}\n{cons_err}\n"
            )

            sum_path = runs_dir / f"consumer_{rid}_summary.json"
            if not sum_path.exists():
                print(f"   ! Khong tim thay {sum_path}")
                continue
            s = json.loads(sum_path.read_text(encoding="utf-8"))
            lat = s["latency_ms"]
            seq = s["seq"]
            row = {
                "experiment": "E4",
                "run": rep,
                "params": json.dumps(
                    {"rate_target_eps": rate_label, "duration_s": e4["duration_s"], "topic": e4["topic"]},
                    separators=(",", ":"),
                ),
                "events": s["counts"]["received"],
                "duration_s": s["active_duration_s"],
                "throughput_eps": s["throughput_eps"],
                "p50_ms": lat["p50"],
                "p95_ms": lat["p95"],
                "p99_ms": lat["p99"],
                "missing": seq.get("missing_unexplained", seq.get("missing_unexplained", seq["total_missing"])),
                "duplicates": seq["total_duplicates"],
            }
            append_csv(out_csv, row)
            print(
                f"   -> rx={row['events']} ({row['throughput_eps']} eps) | "
                f"e2e p50={row['p50_ms']} p95={row['p95_ms']} p99={row['p99_ms']} ms | "
                f"missing={row['missing']} dup={row['duplicates']}"
            )
            time.sleep(cfg["pause_between_runs_s"])

    (ROOT / "evidence" / "E4_run.log").write_text("\n".join(log_lines), encoding="utf-8")


def run_e5(cfg: dict[str, Any], reps: int) -> None:
    e5 = cfg["E5"]
    bootstrap = cfg["bootstrap"]
    out_csv = ROOT / "results" / "E5.csv"
    if out_csv.exists():
        out_csv.unlink()
    runs_dir = ROOT / "results" / "runs_tv3"
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_lines: list[str] = []

    for rep in range(1, reps + 1):
        recreate_bench_topics(bootstrap)
        rid = f"E5_continuity_r{rep}"
        print(
            f"[E5] Continuity test (rate={e5['rate_eps']} eps, duration={e5['duration_s']}s, "
            f"restart Kafka at {e5['restart_at_s']}s), lan {rep}/{reps}..."
        )
        cons_cmd = [
            sys.executable,
            str(ROOT / "consumer" / "verify_consumer.py"),
            "--bootstrap-servers",
            bootstrap,
            "--topics",
            "bench.p6",
            "--offset-reset",
            "earliest",
            "--idle-timeout",
            "90",
            "--start-timeout",
            "45",
            "--out-dir",
            str(runs_dir),
            "--run-name",
            rid,
        ]
        cons_proc = subprocess.Popen(cons_cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(3)

        prod_cmd = [
            sys.executable,
            "-m",
            "src.producer.producer",
            "--bootstrap",
            bootstrap,
            "--topic",
            "bench.p6",
            "--rate",
            str(e5["rate_eps"]),
            "--duration",
            str(e5["duration_s"]),
            "--producers",
            "1",
            "--run-id",
            rid,
        ]
        prod_proc = subprocess.Popen(prod_cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        time.sleep(e5["restart_at_s"])
        print("   -> Restarting Kafka container...")
        t_rs = time.perf_counter()
        rs = subprocess.run(["docker", "restart", e5["container_name"]], capture_output=True, text=True)
        rs_elapsed = round(time.perf_counter() - t_rs, 2)
        print(f"   -> docker restart xong sau {rs_elapsed}s (rc={rs.returncode})")

        prod_out, prod_err = prod_proc.communicate(timeout=180)
        cons_out, cons_err = cons_proc.communicate(timeout=180)
        log_lines.append(
            f"=== {rid} (restart_cmd_s={rs_elapsed}) ===\n"
            f"-- PRODUCER --\n{prod_out}\n{prod_err}\n-- CONSUMER --\n{cons_out}\n{cons_err}\n"
        )

        sum_path = runs_dir / f"consumer_{rid}_summary.json"
        if not sum_path.exists():
            print(f"   ! Khong tim thay {sum_path}")
            continue
        s = json.loads(sum_path.read_text(encoding="utf-8"))
        lat = s["latency_ms"]
        seq = s["seq"]
        row = {
            "experiment": "E5",
            "run": rep,
            "params": json.dumps(
                {
                    "rate_eps": e5["rate_eps"],
                    "duration_s": e5["duration_s"],
                    "restart_at_s": e5["restart_at_s"],
                    "max_gap_s": s.get("max_receive_gap_s"),
                },
                separators=(",", ":"),
            ),
            "events": s["counts"]["received"],
            "duration_s": s["active_duration_s"],
            "throughput_eps": s["throughput_eps"],
            "p50_ms": lat["p50"],
            "p95_ms": lat["p95"],
            "p99_ms": lat["p99"],
            "missing": seq.get("missing_unexplained", seq["total_missing"]),
            "duplicates": seq["total_duplicates"],
        }
        append_csv(out_csv, row)
        print(
            f"   -> rx={row['events']} | max_gap={s.get('max_receive_gap_s')}s | "
            f"missing={row['missing']} dup={row['duplicates']}"
        )
        time.sleep(5)

    (ROOT / "evidence" / "E5_run.log").write_text("\n".join(log_lines), encoding="utf-8")


def run_e6(cfg: dict[str, Any], reps: int) -> None:
    e6 = cfg["E6"]
    bootstrap = cfg["bootstrap"]
    out_csv = ROOT / "results" / "E6.csv"
    if out_csv.exists():
        out_csv.unlink()
    runs_dir = ROOT / "results" / "runs_tv3"
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_lines: list[str] = []

    for rep in range(1, reps + 1):
        ensure_topics(bootstrap)
        rid = f"E6_dlq_r{rep}"
        print(f"[E6] DLQ test (bad_ratio={e6['bad_ratio']}, rate={e6['rate_eps']}), lan {rep}/{reps}...")
        cons_cmd = [
            sys.executable,
            str(ROOT / "consumer" / "verify_consumer.py"),
            "--bootstrap-servers",
            bootstrap,
            "--topics",
            "web.clickstream",
            "web.transactions",
            "--offset-reset",
            "latest",
            "--idle-timeout",
            "8",
            "--start-timeout",
            "30",
            "--out-dir",
            str(runs_dir),
            "--run-name",
            rid,
        ]
        cons_proc = subprocess.Popen(cons_cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(4)  # doi consumer subscribe va assign partition truoc khi bom du lieu

        prod_cmd = [
            sys.executable,
            "-m",
            "src.producer.producer",
            "--bootstrap",
            bootstrap,
            "--rate",
            str(e6["rate_eps"]),
            "--duration",
            str(e6["duration_s"]),
            "--bad-ratio",
            str(e6["bad_ratio"]),
            "--producers",
            "1",
            "--run-id",
            rid,
        ]
        prod_res = subprocess.run(prod_cmd, cwd=ROOT, capture_output=True, text=True)
        cons_out, cons_err = cons_proc.communicate(timeout=120)

        p_path = ROOT / "results" / "runs" / f"{rid}.json"
        c_path = runs_dir / f"consumer_{rid}_summary.json"
        p_data = json.loads(p_path.read_text(encoding="utf-8")) if p_path.exists() else {}
        c_data = json.loads(c_path.read_text(encoding="utf-8")) if c_path.exists() else {}
        log_lines.append(
            f"=== {rid} ===\n-- PRODUCER --\n{prod_res.stdout}\n{prod_res.stderr}\n"
            f"-- CONSUMER --\n{cons_out}\n{cons_err}\n"
        )
        if not c_data:
            print(f"   ! Khong tim thay {c_path}")
            continue

        lat = c_data["latency_ms"]
        seq = c_data["seq"]
        injected = p_data.get("injected_total", 0)
        invalid = c_data["counts"]["invalid"]
        dlq_del = c_data["dlq"]["delivered"]
        row = {
            "experiment": "E6",
            "run": rep,
            "params": json.dumps(
                {
                    "bad_ratio": e6["bad_ratio"],
                    "injected": injected,
                    "invalid_detected": invalid,
                    "dlq_delivered": dlq_del,
                },
                separators=(",", ":"),
            ),
            "events": c_data["counts"]["received"],
            "duration_s": c_data["active_duration_s"],
            "throughput_eps": c_data["throughput_eps"],
            "p50_ms": lat["p50"],
            "p95_ms": lat["p95"],
            "p99_ms": lat["p99"],
            "missing": seq.get("missing_unexplained", seq["total_missing"]),
            "duplicates": seq["total_duplicates"],
        }
        append_csv(out_csv, row)
        print(
            f"   -> rx={row['events']} | injected={injected} invalid={invalid} dlq_delivered={dlq_del} | "
            f"valid+invalid==rx: {c_data['checks']['valid_plus_invalid_equals_received']}"
        )
        time.sleep(cfg["pause_between_runs_s"])

    (ROOT / "evidence" / "E6_run.log").write_text("\n".join(log_lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark Runner tong hop (TV3)")
    ap.add_argument("--config", type=Path, default=DEFAULT_CFG)
    ap.add_argument(
        "--only",
        nargs="+",
        choices=["BASELINE", "E1", "E4", "E5", "E6"],
        default=["BASELINE", "E1", "E4", "E5", "E6"],
        help="Chon cac thi nghiem can chay (mac dinh chay BASELINE, E1, E4, E5, E6)",
    )
    ap.add_argument("--reps", type=int, default=None, help="Ghi de so lan lap moi cau hinh")
    args = ap.parse_args()

    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    ensure_topics(cfg["bootstrap"])

    runners = {
        "BASELINE": lambda: run_baseline(cfg, args.reps or cfg["baseline"]["reps"]),
        "E1": lambda: run_e1(cfg, args.reps or cfg["E1"]["reps"]),
        "E4": lambda: run_e4(cfg, args.reps or cfg["E4"]["reps"]),
        "E5": lambda: run_e5(cfg, args.reps or cfg["E5"]["reps"]),
        "E6": lambda: run_e6(cfg, args.reps or cfg["E6"]["reps"]),
    }
    for name in args.only:
        runners[name]()
    print("Hoan tat benchmark. Xem results/*.csv va evidence/*_run.log")


if __name__ == "__main__":
    main()
