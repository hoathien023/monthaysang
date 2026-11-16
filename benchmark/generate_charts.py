"""benchmark/generate_charts.py

Tong hop ket qua tu tat ca file CSV trong results/ (E1..E7 va BASELINE),
ve bieu do PNG chuan trong charts/ (co ten truc, don vi, error bar)
va xuat bang thong ke tong hop results/summary_E1_E7.csv.
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RES_DIR = ROOT / "results"
CHART_DIR = ROOT / "charts"


def setup_style() -> None:
    plt.rcParams.update({
        "figure.facecolor": "white",
        "axes.facecolor": "#f8fafc",
        "axes.edgecolor": "#cbd5e1",
        "axes.grid": True,
        "grid.alpha": 0.4,
        "grid.linestyle": "--",
        "font.size": 10,
    })


def plot_e1(df: pd.DataFrame, summary_rows: list[dict]) -> None:
    df = df.copy()
    df["partitions"] = df["params"].apply(lambda s: json.loads(s)["partitions"])
    grp = df.groupby("partitions")["throughput_eps"].agg(["mean", "std", "min", "max"]).reset_index()

    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=160)
    labels = [f"{int(p)} partition" for p in grp["partitions"]]
    bars = ax.bar(labels, grp["mean"], yerr=grp["std"].fillna(0), capsize=6,
                  color=["#60a5fa", "#3b82f6", "#1d4ed8"], edgecolor="#1e3a8a", width=0.52)
    for b, m in zip(bars, grp["mean"]):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + max(grp["mean"]) * 0.03,
                f"{m:,.0f}", ha="center", va="bottom", fontweight="bold", fontsize=9.5)
    ax.set_title("E1: Ảnh hưởng của số Partition đến thông lượng Producer", fontweight="bold", pad=12)
    ax.set_xlabel("Số partition của topic (bench.p1 / p3 / p6)")
    ax.set_ylabel("Thông lượng (sự kiện/giây)")
    ax.set_ylim(0, max(grp["mean"] + grp["std"].fillna(0)) * 1.2)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "E1_partitions.png")
    plt.close(fig)

    for _, r in grp.iterrows():
        summary_rows.append({
            "experiment": "E1", "config": f"{int(r['partitions'])} partition",
            "runs": int((df["partitions"] == r["partitions"]).sum()),
            "metric": "throughput_eps", "mean": round(r["mean"], 1),
            "std": round(0.0 if pd.isna(r["std"]) else r["std"], 1),
            "min": round(r["min"], 1), "max": round(r["max"], 1),
        })


def plot_e2(df: pd.DataFrame, summary_rows: list[dict]) -> None:
    grp = df.groupby("producers").agg(
        eps_mean=("throughput_eps", "mean"),
        eps_std=("throughput_eps", "std"),
        eps_min=("throughput_eps", "min"),
        eps_max=("throughput_eps", "max"),
        p95_mean=("ack_p95_ms", "mean"),
    ).reset_index()

    fig, ax1 = plt.subplots(figsize=(8.2, 4.8), dpi=160)
    x = [f"{int(p)} producer" for p in grp["producers"]]
    bars = ax1.bar(x, grp["eps_mean"], yerr=grp["eps_std"].fillna(0), capsize=6,
                   color="#10b981", edgecolor="#065f46", width=0.48, label="Thông lượng TB (eps)")
    for b, m in zip(bars, grp["eps_mean"]):
        ax1.text(b.get_x() + b.get_width() / 2, b.get_height() + max(grp["eps_mean"]) * 0.03,
                 f"{m:,.0f}", ha="center", va="bottom", fontweight="bold", fontsize=9.5)
    ax1.set_xlabel("Số tiến trình Producer song song (topic 6 partition)")
    ax1.set_ylabel("Tổng thông lượng (sự kiện/giây)", color="#065f46")
    ax1.set_ylim(0, max(grp["eps_mean"] + grp["eps_std"].fillna(0)) * 1.22)

    ax2 = ax1.twinx()
    ax2.plot(x, grp["p95_mean"], color="#dc2626", marker="o", linewidth=2.2, label="Ack p95 (ms)")
    ax2.set_ylabel("Độ trễ xác nhận Ack p95 (ms)", color="#dc2626")
    ax2.set_ylim(0, max(grp["p95_mean"]) * 1.35)
    ax2.grid(False)

    ax1.set_title("E2: Khả năng mở rộng theo số Producer (Thông lượng & Ack p95)", fontweight="bold", pad=12)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "E2_producers.png")
    plt.close(fig)

    for _, r in grp.iterrows():
        summary_rows.append({
            "experiment": "E2", "config": f"{int(r['producers'])} producers",
            "runs": int((df["producers"] == r["producers"]).sum()),
            "metric": "throughput_eps", "mean": round(r["eps_mean"], 1),
            "std": round(0.0 if pd.isna(r["eps_std"]) else r["eps_std"], 1),
            "min": round(r["eps_min"], 1), "max": round(r["eps_max"], 1),
        })


def plot_e3(df: pd.DataFrame, summary_rows: list[dict]) -> None:
    label_map = {
        "acksall_batch-size65536_compressionnone_linger-ms5_producers1": "Gốc (64K, linger 5, all)",
        "acksall_batch-size16384_compressionnone_linger-ms5_producers1": "batch.size = 16 KB",
        "acksall_batch-size262144_compressionnone_linger-ms5_producers1": "batch.size = 256 KB",
        "acksall_batch-size65536_compressionnone_linger-ms0_producers1": "linger.ms = 0",
        "acksall_batch-size65536_compressionnone_linger-ms20_producers1": "linger.ms = 20",
        "acksall_batch-size65536_compressionlz4_linger-ms5_producers1": "compression = lz4",
        "acksall_batch-size65536_compressionzstd_linger-ms5_producers1": "compression = zstd",
        "acks0_batch-size65536_compressionnone_linger-ms5_producers1": "acks = 0",
        "acks1_batch-size65536_compressionnone_linger-ms5_producers1": "acks = 1",
    }
    grp = df.groupby("config", sort=False).agg(
        eps_mean=("throughput_eps", "mean"),
        eps_std=("throughput_eps", "std"),
        eps_min=("throughput_eps", "min"),
        eps_max=("throughput_eps", "max"),
        p95_mean=("ack_p95_ms", "mean"),
    ).reset_index()
    grp["label"] = grp["config"].map(lambda c: label_map.get(c, c))
    grp = grp.sort_values("eps_mean", ascending=True)

    fig, ax = plt.subplots(figsize=(9.5, 5.4), dpi=160)
    colors = ["#ef4444" if "16 KB" in lb else "#3b82f6" for lb in grp["label"]]
    bars = ax.barh(grp["label"], grp["eps_mean"], xerr=grp["eps_std"].fillna(0), capsize=4,
                   color=colors, edgecolor="#1e293b", height=0.6)
    for b, m, p95 in zip(bars, grp["eps_mean"], grp["p95_mean"]):
        ax.text(b.get_width() + 1200, b.get_y() + b.get_height() / 2,
                f"{m:,.0f} eps (p95={p95:.0f} ms)", va="center", fontsize=8.5, fontweight="bold")
    ax.set_title("E3: Ảnh hưởng cấu hình Producer đến Thông lượng & Ack p95", fontweight="bold", pad=12)
    ax.set_xlabel("Thông lượng trung bình (sự kiện/giây)")
    ax.set_xlim(0, max(grp["eps_mean"]) * 1.38)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "E3_producer_configs.png")
    plt.close(fig)

    for _, r in grp.iterrows():
        summary_rows.append({
            "experiment": "E3", "config": r["label"],
            "runs": int((df["config"] == r["config"]).sum()),
            "metric": "throughput_eps", "mean": round(r["eps_mean"], 1),
            "std": round(0.0 if pd.isna(r["eps_std"]) else r["eps_std"], 1),
            "min": round(r["eps_min"], 1), "max": round(r["eps_max"], 1),
        })


def plot_e4(df: pd.DataFrame, summary_rows: list[dict]) -> None:
    df = df.copy()
    df["rate_target"] = df["params"].apply(lambda s: str(json.loads(s)["rate_target_eps"]))
    order = ["500", "2000", "5000", "max"]
    df["order"] = df["rate_target"].apply(lambda r: order.index(r) if r in order else 99)
    grp = df.groupby(["order", "rate_target"]).agg(
        p50_mean=("p50_ms", "mean"),
        p95_mean=("p95_ms", "mean"),
        p99_mean=("p99_ms", "mean"),
        eps_mean=("throughput_eps", "mean"),
        p95_min=("p95_ms", "min"),
        p95_max=("p95_ms", "max"),
        p95_std=("p95_ms", "std"),
    ).reset_index().sort_values("order")

    x_labels = [f"{r} eps" if r != "max" else f"Tối đa\n(~{m:,.0f} eps)"
                for r, m in zip(grp["rate_target"], grp["eps_mean"])]

    fig, ax = plt.subplots(figsize=(8.6, 5.0), dpi=160)
    idx = range(len(x_labels))
    w = 0.25
    ax.bar([i - w for i in idx], grp["p50_mean"], width=w, label="p50 (trung vị)", color="#22c55e")
    ax.bar(idx, grp["p95_mean"], width=w, label="p95", color="#f59e0b")
    ax.bar([i + w for i in idx], grp["p99_mean"], width=w, label="p99 (tail)", color="#ef4444")

    for i, (p50, p95, p99) in enumerate(zip(grp["p50_mean"], grp["p95_mean"], grp["p99_mean"])):
        ax.text(i - w, p50 + 5, f"{p50:.0f}", ha="center", va="bottom", fontsize=8)
        ax.text(i, p95 + 5, f"{p95:.0f}", ha="center", va="bottom", fontsize=8, fontweight="bold")
        ax.text(i + w, p99 + 5, f"{p99:.0f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(list(idx))
    ax.set_xticklabels(x_labels)
    ax.set_title("E4: Độ trễ End-to-End (p50, p95, p99) theo mức tải", fontweight="bold", pad=12)
    ax.set_xlabel("Mức tải mục tiêu đầu vào")
    ax.set_ylabel("Độ trễ End-to-End (ms)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(CHART_DIR / "E4_latency_vs_load.png")
    plt.close(fig)

    for _, r in grp.iterrows():
        summary_rows.append({
            "experiment": "E4", "config": f"rate={r['rate_target']}",
            "runs": int((df["rate_target"] == r["rate_target"]).sum()),
            "metric": "e2e_p95_ms", "mean": round(r["p95_mean"], 2),
            "std": round(0.0 if pd.isna(r["p95_std"]) else r["p95_std"], 2),
            "min": round(r["p95_min"], 2), "max": round(r["p95_max"], 2),
        })


def plot_e7(df: pd.DataFrame, summary_rows: list[dict]) -> None:
    grp = df.groupby(["mode", "clients"]).agg(
        eps_mean=("events_per_s", "mean"),
        eps_std=("events_per_s", "std"),
        eps_min=("events_per_s", "min"),
        eps_max=("events_per_s", "max"),
        p95_mean=("p95_ms", "mean"),
    ).reset_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5, 4.6), dpi=160)
    for mode, ax, title, col in [
        ("single", ax1, "E7a: REST API từng sự kiện (single)", "#6366f1"),
        ("batch", ax2, "E7b: REST API theo lô 100 sự kiện (batch)", "#0ea5e9"),
    ]:
        sub = grp[grp["mode"] == mode].sort_values("clients")
        xl = [f"{int(c)} client" for c in sub["clients"]]
        bars = ax.bar(xl, sub["eps_mean"], yerr=sub["eps_std"].fillna(0), capsize=5,
                      color=col, edgecolor="#0f172a", width=0.5)
        for b, m, p95 in zip(bars, sub["eps_mean"], sub["p95_mean"]):
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + max(sub["eps_mean"]) * 0.03,
                    f"{m:,.0f} eps\n(p95={p95:.0f}ms)", ha="center", va="bottom", fontsize=8.5, fontweight="bold")
        ax.set_title(title, fontweight="bold")
        ax.set_xlabel("Số client đồng thời")
        ax.set_ylabel("Thông lượng (sự kiện/giây)")
        ax.set_ylim(0, max(sub["eps_mean"] + sub["eps_std"].fillna(0)) * 1.25)

    fig.tight_layout()
    fig.savefig(CHART_DIR / "E7_rest_api.png")
    plt.close(fig)

    for _, r in grp.iterrows():
        summary_rows.append({
            "experiment": "E7", "config": f"{r['mode']}_c{int(r['clients'])}",
            "runs": int(((df["mode"] == r["mode"]) & (df["clients"] == r["clients"])).sum()),
            "metric": "events_per_s", "mean": round(r["eps_mean"], 1),
            "std": round(0.0 if pd.isna(r["eps_std"]) else r["eps_std"], 1),
            "min": round(r["eps_min"], 1), "max": round(r["eps_max"], 1),
        })


def plot_baseline_vs_python(base_df: pd.DataFrame, e2_df: pd.DataFrame, summary_rows: list[dict]) -> None:
    b_mean = base_df["throughput_eps"].mean()
    b_std = base_df["throughput_eps"].std()
    p1 = e2_df[e2_df["producers"] == 1]["throughput_eps"]
    p4 = e2_df[e2_df["producers"] == 4]["throughput_eps"]

    labels = ["Baseline\n(kafka-producer-perf-test)", "Python Producer\n(1 tiến trình)", "Python Producer\n(4 tiến trình)"]
    means = [b_mean, p1.mean(), p4.mean()]
    stds = [0.0 if pd.isna(b_std) else b_std, p1.std(), p4.std()]

    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=160)
    bars = ax.bar(labels, means, yerr=stds, capsize=6, color=["#64748b", "#3b82f6", "#10b981"], width=0.5)
    for b, m in zip(bars, means):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + max(means) * 0.03,
                f"{m:,.0f} eps", ha="center", va="bottom", fontweight="bold", fontsize=9.5)
    ax.set_title("Đối chiếu đường cơ sở Kafka Perf Test với Python Producer", fontweight="bold", pad=12)
    ax.set_ylabel("Thông lượng trung bình (sự kiện/giây)")
    ax.set_ylim(0, max([m + s for m, s in zip(means, stds)]) * 1.22)
    fig.tight_layout()
    fig.savefig(CHART_DIR / "BASELINE_comparison.png")
    plt.close(fig)

    summary_rows.append({
        "experiment": "BASELINE", "config": "kafka-producer-perf-test (6P)",
        "runs": len(base_df), "metric": "throughput_eps",
        "mean": round(b_mean, 1), "std": round(0.0 if pd.isna(b_std) else b_std, 1),
        "min": round(base_df["throughput_eps"].min(), 1),
        "max": round(base_df["throughput_eps"].max(), 1),
    })


def main() -> None:
    setup_style()
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict] = []

    if (RES_DIR / "E1.csv").exists():
        plot_e1(pd.read_csv(RES_DIR / "E1.csv"), summary_rows)
    if (RES_DIR / "E2.csv").exists():
        plot_e2(pd.read_csv(RES_DIR / "E2.csv"), summary_rows)
    if (RES_DIR / "E3.csv").exists():
        plot_e3(pd.read_csv(RES_DIR / "E3.csv"), summary_rows)
    if (RES_DIR / "E4.csv").exists():
        plot_e4(pd.read_csv(RES_DIR / "E4.csv"), summary_rows)
    if (RES_DIR / "E7.csv").exists():
        plot_e7(pd.read_csv(RES_DIR / "E7.csv"), summary_rows)
    if (RES_DIR / "BASELINE.csv").exists() and (RES_DIR / "E2.csv").exists():
        plot_baseline_vs_python(pd.read_csv(RES_DIR / "BASELINE.csv"), pd.read_csv(RES_DIR / "E2.csv"), summary_rows)

    for extra, metric in [("E5.csv", "throughput_eps"), ("E6.csv", "throughput_eps")]:
        p = RES_DIR / extra
        if p.exists():
            df = pd.read_csv(p)
            exp = extra.replace(".csv", "")
            summary_rows.append({
                "experiment": exp, "config": "standard", "runs": len(df),
                "metric": metric, "mean": round(df[metric].mean(), 1),
                "std": round(0.0 if pd.isna(df[metric].std()) else df[metric].std(), 1),
                "min": round(df[metric].min(), 1), "max": round(df[metric].max(), 1),
            })

    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(RES_DIR / "summary_E1_E7.csv", index=False, encoding="utf-8-sig")
        print(f"Da tao {len(summary_rows)} dong trong results/summary_E1_E7.csv va bieu do trong charts/")


if __name__ == "__main__":
    main()
