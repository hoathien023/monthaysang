"""Chay tu dong thi nghiem E2, E3, E7 (phan cua TV1), moi cau hinh nhieu lan, xuat CSV.

  python -m benchmark.run_tv1                 # chay ca E2, E3, E7 (~25-30 phut)
  python -m benchmark.run_tv1 --only E2       # chi E2
  python -m benchmark.run_tv1 --only E3 --reps 3 --duration 20

Dieu kien: Docker/Kafka dang chay, da tao topic. E7 can API dang chay (uvicorn src.api.app:app --port 8000).
Ket qua: results/E2.csv, results/E3.csv, results/E7.csv (ghi tung dong, dung giua chung van con du lieu da chay).
"""
import argparse, csv, json, os, subprocess, sys, time

BASE = {"acks": "all", "linger-ms": 5, "batch-size": 65536, "compression": "none"}
E2 = [{"producers": p} for p in (1, 2, 4)]
E3 = [dict(BASE)]  # cau hinh goc
for k, vals in (("batch-size", (16384, 262144)), ("linger-ms", (0, 20)),
                ("compression", ("lz4", "zstd")), ("acks", ("0", "1"))):
    for v in vals:
        c = dict(BASE); c[k] = v; E3.append(c)
E7 = [{"mode": m, "clients": c} for m in ("single", "batch") for c in (1, 10, 50)]

PROD_COLS = ["experiment", "config", "rep", "producers", "acks", "linger_ms", "batch_size", "compression",
             "sent", "delivered", "failed", "duration_s", "throughput_eps", "ack_p50_ms", "ack_p95_ms", "ack_p99_ms"]
API_COLS = ["experiment", "config", "rep", "mode", "clients", "requests_ok", "requests_failed",
            "requests_per_s", "events_per_s", "p50_ms", "p95_ms", "p99_ms"]


def append_csv(path, cols, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if new:
            w.writeheader()
        w.writerow(row)


def run_producer(exp, cfg, rep, duration, topic):
    full = {**BASE, "producers": 1, **cfg}
    rid = f"{exp}_{rep}_" + "_".join(f"{k}{v}" for k, v in sorted(full.items()))
    cmd = [sys.executable, "-m", "src.producer.producer", "--topic", topic, "--duration", str(duration),
           "--producers", str(full["producers"]), "--acks", str(full["acks"]), "--linger-ms", str(full["linger-ms"]),
           "--batch-size", str(full["batch-size"]), "--compression", str(full["compression"]), "--run-id", rid]
    subprocess.run(cmd, check=False, capture_output=True, text=True)
    path = f"results/runs/{rid}.json"
    if not os.path.exists(path):
        print(f"   ! khong co ket qua cho {rid} (producer loi?)")
        return None
    d = json.load(open(path, encoding="utf-8"))
    al = d.get("ack_latency_ms", {})
    row = {"experiment": exp, "config": rid.split("_", 2)[2], "rep": rep, "producers": full["producers"],
           "acks": full["acks"], "linger_ms": full["linger-ms"], "batch_size": full["batch-size"],
           "compression": full["compression"], "sent": d["sent"], "delivered": d["delivered"],
           "failed": d["failed"], "duration_s": d["wall_seconds"], "throughput_eps": d["throughput_delivered_eps"],
           "ack_p50_ms": al.get("p50"), "ack_p95_ms": al.get("p95"), "ack_p99_ms": al.get("p99")}
    append_csv(f"results/{exp}.csv", PROD_COLS, row)
    return row


def run_api(cfg, rep, duration):
    rid = f"E7_{rep}_{cfg['mode']}_c{cfg['clients']}"
    cmd = [sys.executable, "-m", "src.api.load_client", "--clients", str(cfg["clients"]),
           "--duration", str(duration), "--mode", cfg["mode"], "--batch-size", "100", "--run-id", rid]
    subprocess.run(cmd, check=False, capture_output=True, text=True)
    path = f"results/runs/{rid}.json"
    if not os.path.exists(path):
        print(f"   ! khong co ket qua cho {rid} (API co dang chay khong?)")
        return None
    d = json.load(open(path, encoding="utf-8"))
    row = {"experiment": "E7", "config": f"{cfg['mode']}_c{cfg['clients']}", "rep": rep, "mode": cfg["mode"],
           "clients": cfg["clients"], "requests_ok": d["requests_ok"], "requests_failed": d["requests_failed"],
           "requests_per_s": d["requests_per_s"], "events_per_s": d["events_per_s"],
           "p50_ms": d["latency_ms"]["p50"], "p95_ms": d["latency_ms"]["p95"], "p99_ms": d["latency_ms"]["p99"]}
    append_csv("results/E7.csv", API_COLS, row)
    return row


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", choices=["E2", "E3", "E7"], help="chi chay 1 thi nghiem")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--pause", type=float, default=3, help="nghi giua cac lan chay (giay)")
    a = ap.parse_args()
    os.makedirs("results/runs", exist_ok=True)

    plan = []
    for exp, cfgs in (("E2", E2), ("E3", E3), ("E7", E7)):
        if a.only in (None, exp):
            for cfg in cfgs:
                for rep in range(1, a.reps + 1):
                    plan.append((exp, cfg, rep))
    est = len(plan) * (a.duration + a.pause + 4) / 60
    print(f"Se chay {len(plan)} lan, uoc tinh khoang {est:.0f} phut. Ctrl+C de dung (du lieu da chay van duoc luu).")
    for i, (exp, cfg, rep) in enumerate(plan, 1):
        print(f"[{i}/{len(plan)}] {exp} {cfg} lan {rep}")
        row = run_api(cfg, rep, a.duration) if exp == "E7" else run_producer(exp, cfg, rep, a.duration, "bench.p6")
        if row:
            if exp == "E7":
                print(f"   {row['requests_per_s']} req/s, {row['events_per_s']} su kien/s, p95={row['p95_ms']}ms, loi={row['requests_failed']}")
            else:
                print(f"   {row['throughput_eps']} su kien/s, p95 xac nhan={row['ack_p95_ms']}ms, loi={row['failed']}")
        time.sleep(a.pause)
    print("Xong. Xem results/E2.csv, E3.csv, E7.csv")


if __name__ == "__main__":
    main()
