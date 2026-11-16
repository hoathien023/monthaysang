"""benchmark/render_diagram_png.py

Xuat so do kien truc & data-flow (khop noi dung voi docs/diagrams/architecture.drawio)
ra file anh PNG do phan giai cao tai docs/diagrams/architecture.png.
"""

from __future__ import annotations

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch


def box(ax, x, y, w, h, title, body, fc, ec, title_size=10.5, body_size=8.5, lw=1.6):
    p = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.012,rounding_size=0.015",
        facecolor=fc, edgecolor=ec, linewidth=lw, zorder=3
    )
    ax.add_patch(p)
    if body:
        ax.text(x + w / 2, y + h - 0.028, title, ha="center", va="top",
                fontsize=title_size, fontweight="bold", color="#0f172a", zorder=4)
        ax.text(x + w / 2, y + h / 2 - 0.018, body, ha="center", va="center",
                fontsize=body_size, color="#1e293b", linespacing=1.32, zorder=4)
    else:
        ax.text(x + w / 2, y + h / 2, title, ha="center", va="center",
                fontsize=title_size, fontweight="bold", color="#0f172a", zorder=4)


def group_box(ax, x, y, w, h, label, fc, ec):
    p = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.01,rounding_size=0.018",
        facecolor=fc, edgecolor=ec, linewidth=1.3, linestyle="--", zorder=1
    )
    ax.add_patch(p)
    ax.text(x + w / 2, y + h - 0.012, label, ha="center", va="top",
            fontsize=10, fontweight="bold", color=ec, zorder=2)


def arrow(ax, x1, y1, x2, y2, label="", color="#334155", ls="-", rad=0.0, lx=0.0, ly=0.015):
    arr = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle="-|>,head_length=7,head_width=4.5",
        connectionstyle=f"arc3,rad={rad}",
        color=color, linewidth=1.8, linestyle=ls, zorder=5
    )
    ax.add_patch(arr)
    if label:
        mx, my = (x1 + x2) / 2 + lx, (y1 + y2) / 2 + ly
        ax.text(mx, my, label, ha="center", va="center", fontsize=8,
                color=color, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.18", facecolor="white", edgecolor="none", alpha=0.88),
                zorder=6)


def main() -> None:
    fig, ax = plt.subplots(figsize=(15, 8.6), dpi=180)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(0.5, 0.965, "KIẾN TRÚC & LUỒNG DỮ LIỆU: QUY TRÌNH 1 (SOURCES → INGESTION)",
            ha="center", va="center", fontsize=15, fontweight="bold", color="#0f172a")
    ax.text(0.5, 0.935, "Đồ án Nhập môn Dữ liệu lớn | Giao thức: Python / HTTP POST / Kafka Protocol (JSON UTF-8, key=user_id)",
            ha="center", va="center", fontsize=9.5, color="#475569")

    # 4 main vertical stages
    group_box(ax, 0.02, 0.30, 0.19, 0.59, "1. SOURCES (TV1)", "#eff6ff", "#2563eb")
    group_box(ax, 0.25, 0.30, 0.21, 0.59, "2. INGESTION ENTRY (TV1)", "#f0fdf4", "#16a34a")
    group_box(ax, 0.50, 0.18, 0.23, 0.71, "3. KAFKA BROKER - KRaft (TV2)", "#fff7ed", "#ea580c")
    group_box(ax, 0.77, 0.18, 0.21, 0.71, "4. KIỂM CHỨNG & ĐO (TV2+TV3)", "#faf5ff", "#7e22ce")

    # Stage 1 boxes
    box(ax, 0.035, 0.62, 0.16, 0.20, "Event Generator",
        "src/generator/generator.py\n• Clickstream & Transaction\n• Seed cố định, burst\n• Tiêm lỗi (--bad-ratio)",
        "#dbeafe", "#2563eb")
    box(ax, 0.035, 0.34, 0.16, 0.20, "Load Clients / App",
        "src/api/load_client.py\n• 1 / 10 / 50 clients (E7)\n• Single & Batch (100)\n• Gọi qua 127.0.0.1:8000",
        "#dbeafe", "#2563eb")

    # Stage 2 boxes
    box(ax, 0.265, 0.62, 0.18, 0.20, "Python Producer",
        "src/producer/producer.py\n• confluent-kafka (MP)\n• key=user_id, seq tăng dần\n• acks=all, idempotence",
        "#dcfce7", "#16a34a")
    box(ax, 0.265, 0.34, 0.18, 0.20, "REST API (FastAPI)",
        "src/api/app.py (:8000)\n• POST /events, /batch\n• GET /health, /stats, /docs\n• 202 OK / 503 Error",
        "#dcfce7", "#16a34a")

    # Stage 3 Kafka topics
    box(ax, 0.52, 0.70, 0.19, 0.12, "web.clickstream (6P)",
        "key=user_id | RF=1\npage_view, search, click, cart",
        "#ffedd5", "#ea580c")
    box(ax, 0.52, 0.54, 0.19, 0.12, "web.transactions (3P)",
        "key=user_id | RF=1\norder_created, payment_*",
        "#ffedd5", "#ea580c")
    box(ax, 0.52, 0.39, 0.19, 0.11, "bench.p1 / p3 / p6",
        "1 / 3 / 6 Partitions\n(Thí nghiệm E1, E4, Baseline)",
        "#fef3c7", "#d97706")
    box(ax, 0.52, 0.21, 0.19, 0.13, "web.events.dlq (1P)",
        "Dead-Letter Queue\npayload gốc + error_reason",
        "#fee2e2", "#dc2626")

    # Infra footer boxes
    box(ax, 0.495, 0.04, 0.105, 0.10, "Kafka UI",
        "localhost:8080\nGiám sát topic",
        "#fef9c3", "#ca8a04", title_size=9.5, body_size=8)
    box(ax, 0.625, 0.04, 0.105, 0.10, "Volume",
        "kafka_data\nRetention 168h",
        "#f1f5f9", "#64748b", title_size=9.5, body_size=8)

    # Stage 4 boxes
    box(ax, 0.785, 0.62, 0.18, 0.21, "Consumer kiểm chứng",
        "consumer/verify_consumer.py\n• Validate Data Contract v1.0\n• Check thiếu/trùng seq\n• Latency = t_read - ts_event",
        "#f3e8ff", "#7e22ce")
    box(ax, 0.785, 0.41, 0.18, 0.16, "Benchmark & Metrics",
        "common/metrics.py (TV3)\nbenchmark/run_benchmark.py\n• Throughput, p50/p95/p99",
        "#ede9fe", "#6d28d9")
    box(ax, 0.785, 0.21, 0.18, 0.16, "Kết quả & Minh chứng",
        "results/*.csv | runs/*.json\ncharts/*.png | evidence/*",
        "#e0f2fe", "#0284c7")

    # Downstream box
    box(ax, 0.77, 0.04, 0.21, 0.10, "Nhóm sau: Storage",
        "Đọc web.clickstream & web.transactions\n(Giao diện: Data Contract v1.0)",
        "#f1f5f9", "#475569", title_size=9.5, body_size=8)

    # Arrows
    arrow(ax, 0.195, 0.72, 0.265, 0.72, "Python", "#2563eb")
    arrow(ax, 0.195, 0.44, 0.265, 0.44, "HTTP POST", "#2563eb")

    arrow(ax, 0.445, 0.74, 0.52, 0.76, "Kafka :29092", "#16a34a", ly=0.025)
    arrow(ax, 0.445, 0.68, 0.52, 0.60, "", "#16a34a")
    arrow(ax, 0.445, 0.48, 0.52, 0.72, "", "#16a34a", rad=-0.12)
    arrow(ax, 0.445, 0.44, 0.52, 0.58, "Kafka JSON", "#16a34a", ly=-0.02)

    arrow(ax, 0.71, 0.76, 0.785, 0.74, "Consume", "#7e22ce", ly=0.02)
    arrow(ax, 0.71, 0.60, 0.785, 0.68, "", "#7e22ce")
    arrow(ax, 0.785, 0.64, 0.71, 0.29, "Bản ghi lỗi\n(DLQ)", "#dc2626", ls="--", rad=0.22, lx=-0.035, ly=0.01)

    arrow(ax, 0.875, 0.62, 0.875, 0.57, "", "#6d28d9")
    arrow(ax, 0.875, 0.41, 0.875, 0.37, "CSV/JSON", "#0284c7", lx=0.045, ly=0.0)

    arrow(ax, 0.71, 0.20, 0.80, 0.14, "Data Contract", "#475569", ls="--", lx=0.02, ly=0.02)

    out = Path("docs/diagrams/architecture.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    print(f"Da xuat {out}")


if __name__ == "__main__":
    main()
