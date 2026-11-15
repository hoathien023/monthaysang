"""Producer: sinh su kien bang Generator va gui vao Kafka. Ghi file tom tat de lam minh chung.

Vi du:
  # 1 producer, toc do toi da, 200.000 su kien
  python -m src.producer.producer --n 200000
  # 2 producer, tong 2.000 su kien/giay, chay 60 giay, tiem 5% loi
  python -m src.producer.producer --producers 2 --rate 2000 --duration 60 --bad-ratio 0.05
  # Thi nghiem E1: ep tat ca vao 1 topic bench
  python -m src.producer.producer --topic bench.p3 --n 500000

Quy uoc:
  --rate      TONG toc do muc tieu (su kien/giay) cua tat ca producer; 0 = toi da.
  --burst-*   mo phong gio cao diem: cu moi --burst-every giay, trong --burst-seconds giay cuoi cua chu ky
              toc do tang len --burst-rate (can --rate > 0). Vd: --rate 1000 --burst-rate 5000 --burst-every 10 --burst-seconds 3
  --n         so su kien MOI producer gui (dung de dung); --duration (giay) dung o dau den truoc.
  idempotence tu dong bat khi acks=all (Kafka yeu cau); acks=0/1 thi tat (de do thi nghiem E3).
"""
import argparse, json, multiprocessing as mp, os, sys, time
from collections import Counter

from confluent_kafka import Producer

from src.generator.generator import Generator


def build_conf(a):
    conf = {
        "bootstrap.servers": a.bootstrap,
        "acks": a.acks,
        "linger.ms": a.linger_ms,
        "batch.size": a.batch_size,
        "compression.type": a.compression,
        "enable.idempotence": a.acks == "all",
        "queue.buffering.max.messages": 500000,
    }
    return conf


def run_one(args_dict, index):
    """Chay 1 producer (1 tien trinh). Tra ve dict thong ke."""
    a = argparse.Namespace(**args_dict)
    pid = f"P{index + 1:02d}"
    gen = Generator(pid, seed=a.seed + index)
    txn_ratio = 0.0 if a.topic else a.txn_ratio
    rate = a.rate / a.producers if a.rate > 0 else 0
    burst_rate = a.burst_rate / a.producers if (a.burst_rate > 0 and rate > 0) else 0

    def rate_at(elapsed):
        """Toc do muc tieu (su kien/giay/producer) tai thoi diem elapsed; tang vot trong giai doan burst."""
        if burst_rate and a.burst_every > 0 and (elapsed % a.burst_every) >= a.burst_every - a.burst_seconds:
            return burst_rate
        return rate

    stats = {"delivered": 0, "failed": 0}
    last_err = []
    lat_samples = []  # lay mau 1/10 su kien: do tre tu luc produce() den luc Kafka xac nhan (ms)

    def on_delivery(err, msg):
        if err is None:
            stats["delivered"] += 1
            if stats["delivered"] % 10 == 0 and msg is not None:
                lat = msg.latency()
                if lat is not None:
                    lat_samples.append(lat * 1000.0)
        else:
            stats["failed"] += 1
            if len(last_err) < 3:
                last_err.append(str(err))

    p = Producer(build_conf(a))
    injected = Counter()
    per_topic = Counter()
    sent = 0
    timeline = []  # so su kien gui trong tung giay (de chung minh toc do va tinh lien tuc)
    t0 = time.perf_counter()
    next_due = 0.0  # thoi diem (tinh tu t0) duoc phep gui su kien ke tiep khi co gioi han toc do
    while True:
        elapsed = time.perf_counter() - t0
        if a.n and sent >= a.n:
            break
        if a.duration and elapsed >= a.duration:
            break
        if rate > 0 and next_due > elapsed:
            p.poll(min(next_due - elapsed, 0.01))  # cho den han, tranh gui qua nhanh
            continue
        if rate > 0:
            next_due += 1.0 / rate_at(elapsed)
        topic, key, ev, label = gen.next_event(txn_ratio, a.bad_ratio)
        if a.topic:
            topic = a.topic
        value = json.dumps(ev, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        while True:
            try:
                p.produce(topic, key=key or None, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                p.poll(0.1)  # hang doi day: cho Kafka giai phong
        sent += 1
        sec = int(elapsed)
        if sec >= len(timeline):
            timeline.extend([0] * (sec + 1 - len(timeline)))
        timeline[sec] += 1
        per_topic[topic] += 1
        if label:
            injected[label] += 1
        if sent % 500 == 0:
            p.poll(0)
    t_produce = time.perf_counter() - t0
    remaining = p.flush(60)
    t_total = time.perf_counter() - t0
    return {
        "producer_id": pid, "sent": sent, "delivered": stats["delivered"], "failed": stats["failed"],
        "undelivered_after_flush": remaining, "seq_first": 1 if sent else 0, "seq_last": gen.seq,
        "seconds_produce": round(t_produce, 3), "seconds_total": round(t_total, 3),
        "per_topic": dict(per_topic), "injected_errors": dict(injected), "delivery_errors": last_err,
        "lat_samples_ms": lat_samples, "timeline_sent_per_s": timeline,
    }


def parse():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--producers", type=int, default=1, help="so tien trinh producer")
    ap.add_argument("--rate", type=float, default=0, help="tong su kien/giay (0 = toi da)")
    ap.add_argument("--burst-rate", type=float, default=0, help="tong toc do trong giai doan burst (0 = tat)")
    ap.add_argument("--burst-every", type=float, default=10, help="chu ky burst (giay)")
    ap.add_argument("--burst-seconds", type=float, default=2, help="so giay burst cuoi moi chu ky")
    ap.add_argument("--n", type=int, default=0, help="so su kien moi producer")
    ap.add_argument("--duration", type=float, default=0, help="thoi gian chay (giay)")
    ap.add_argument("--topic", default="", help="ep tat ca vao 1 topic (vd bench.p3)")
    ap.add_argument("--txn-ratio", type=float, default=0.1)
    ap.add_argument("--bad-ratio", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--acks", default="all", choices=["0", "1", "all"])
    ap.add_argument("--linger-ms", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=65536)
    ap.add_argument("--compression", default="none", choices=["none", "lz4", "zstd", "snappy", "gzip"])
    ap.add_argument("--run-id", default="")
    a = ap.parse_args()
    if not a.n and not a.duration:
        ap.error("can --n hoac --duration")
    if a.burst_rate > 0 and a.rate <= 0:
        ap.error("--burst-rate can di kem --rate > 0")
    if a.burst_rate > 0 and not (0 < a.burst_seconds < a.burst_every):
        ap.error("can 0 < --burst-seconds < --burst-every")
    return a


def main():
    a = parse()
    run_id = a.run_id or time.strftime("run_%Y%m%d_%H%M%S")
    args_dict = vars(a)
    print(f"[{run_id}] {a.producers} producer, rate={a.rate or 'max'}, acks={a.acks}, "
          f"linger={a.linger_ms}ms, batch={a.batch_size}, nen={a.compression}, bad={a.bad_ratio}")
    t0 = time.perf_counter()
    if a.producers == 1:
        results = [run_one(args_dict, 0)]
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(a.producers) as pool:
            results = pool.starmap(run_one, [(args_dict, i) for i in range(a.producers)])
    wall = time.perf_counter() - t0

    sent = sum(r["sent"] for r in results)
    delivered = sum(r["delivered"] for r in results)
    failed = sum(r["failed"] for r in results)
    inj = Counter()
    for r in results:
        inj.update(r["injected_errors"])
    secs = max(r["seconds_total"] for r in results)
    lat = sorted(x for r in results for x in r.pop("lat_samples_ms", []))
    tls = [r["timeline_sent_per_s"] for r in results]
    n_sec = max((len(t) for t in tls), default=0)
    timeline = [sum(t[i] for t in tls if i < len(t)) for i in range(n_sec)]
    full = timeline[:-1] if len(timeline) > 1 else timeline  # bo giay cuoi (thuong chua tron giay)

    def pct(q):
        return round(lat[min(int(len(lat) * q), len(lat) - 1)], 2) if lat else None

    summary = {
        "run_id": run_id, "params": args_dict, "wall_seconds": round(wall, 3),
        "sent": sent, "delivered": delivered, "failed": failed,
        "throughput_delivered_eps": round(delivered / secs, 1) if secs else 0,
        "ack_latency_ms": {"p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99)},
        "injected_total": sum(inj.values()), "injected_by_kind": dict(inj),
        "timeline_sent_per_s": timeline,
        "rate_per_s": {"min": min(full) if full else 0, "avg": round(sum(full) / len(full), 1) if full else 0,
                       "max": max(full) if full else 0, "seconds_with_zero": sum(1 for x in full if x == 0)},
        "producers": results,
    }
    os.makedirs("results/runs", exist_ok=True)
    path = f"results/runs/{run_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    tl_path = f"results/runs/{run_id}_timeline.csv"
    with open(tl_path, "w", encoding="utf-8", newline="") as f:
        f.write("second,events_sent\n")
        for i, v in enumerate(timeline):
            f.write(f"{i},{v}\n")
    print(f"  gui: {sent} | xac nhan: {delivered} | loi gui: {failed} | "
          f"{summary['throughput_delivered_eps']} su kien/giay | loi da tiem: {summary['injected_total']}")
    rp = summary["rate_per_s"]
    print(f"  toc do moi giay: min={rp['min']} avg={rp['avg']} max={rp['max']} | so giay khong gui: {rp['seconds_with_zero']}")
    al = summary["ack_latency_ms"]
    print(f"  do tre xac nhan (ms): p50={al['p50']} p95={al['p95']} p99={al['p99']}")
    print(f"  tom tat: {path}")
    if failed or sent != delivered:
        print("  CANH BAO: co su kien chua duoc xac nhan, xem delivery_errors trong file tom tat")
        sys.exit(2)


if __name__ == "__main__":
    main()
