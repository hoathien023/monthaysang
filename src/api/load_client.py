"""Gia lap nhieu client gui su kien qua REST API (thi nghiem E7).
Vi du:
  python -m src.api.load_client --clients 10 --duration 30 --mode single
  python -m src.api.load_client --clients 10 --duration 30 --mode batch --batch-size 100 --run-id e7_batch
Luu y: client Python cung ton CPU; neu so do thap bat thuong, thu giam --clients hoac chay tren may khac.
"""
import argparse, json, os, threading, time

import httpx

from src.generator.generator import Generator


def worker(idx, a, deadline, out):
    gen = Generator(f"C{idx + 1:02d}", seed=a.seed + idx)
    lat, ok, fail, events = [], 0, 0, 0
    with httpx.Client(base_url=a.url, timeout=15.0) as c:
        while time.perf_counter() < deadline:
            n = a.batch_size if a.mode == "batch" else 1
            evs = [gen.next_event(a.txn_ratio, a.bad_ratio)[2] for _ in range(n)]
            t = time.perf_counter()
            try:
                r = c.post("/events/batch" if a.mode == "batch" else "/events",
                           json=evs if a.mode == "batch" else evs[0])
                good = r.status_code == 202
            except Exception:
                good = False
            lat.append((time.perf_counter() - t) * 1000)
            if good:
                ok += 1
                events += n
            else:
                fail += 1
    out[idx] = {"ok": ok, "fail": fail, "events": events, "lat": lat}


def pct(sorted_vals, q):
    return round(sorted_vals[min(int(len(sorted_vals) * q), len(sorted_vals) - 1)], 2) if sorted_vals else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--mode", choices=["single", "batch"], default="single")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--txn-ratio", type=float, default=0.1)
    ap.add_argument("--bad-ratio", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=1000)
    ap.add_argument("--run-id", default="")
    a = ap.parse_args()
    run_id = a.run_id or time.strftime("api_%Y%m%d_%H%M%S")

    out = {}
    t0 = time.perf_counter()
    deadline = t0 + a.duration
    ths = [threading.Thread(target=worker, args=(i, a, deadline, out)) for i in range(a.clients)]
    [t.start() for t in ths]
    [t.join() for t in ths]
    wall = time.perf_counter() - t0

    ok = sum(r["ok"] for r in out.values())
    fail = sum(r["fail"] for r in out.values())
    events = sum(r["events"] for r in out.values())
    lat = sorted(x for r in out.values() for x in r["lat"])
    summary = {"run_id": run_id, "params": vars(a), "wall_seconds": round(wall, 2),
               "requests_ok": ok, "requests_failed": fail, "events_accepted": events,
               "requests_per_s": round(ok / wall, 1), "events_per_s": round(events / wall, 1),
               "latency_ms": {"p50": pct(lat, 0.5), "p95": pct(lat, 0.95), "p99": pct(lat, 0.99)}}
    os.makedirs("results/runs", exist_ok=True)
    path = f"results/runs/{run_id}.json"
    json.dump(summary, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[{run_id}] {a.mode}, {a.clients} client | request ok: {ok}, loi: {fail} | "
          f"{summary['requests_per_s']} req/s, {summary['events_per_s']} su kien/s | "
          f"p50={summary['latency_ms']['p50']}ms p95={summary['latency_ms']['p95']}ms")
    print(f"  tom tat: {path}")


if __name__ == "__main__":
    main()
