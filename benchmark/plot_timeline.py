"""Ve bieu do so su kien gui moi giay tu file timeline cua mot lan chay Producer.
  python -m benchmark.plot_timeline demo_burst
Doc  : results/runs/<run_id>_timeline.csv
Ghi  : evidence/<run_id>_timeline.png
Bo giay cuoi cung (thuong chua tron mot giay) de bieu do khong bi tut cuoi."""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    sys.exit("Cach dung: python -m benchmark.plot_timeline <run_id>")
run_id = sys.argv[1]
src = f"results/runs/{run_id}_timeline.csv"
if not os.path.exists(src):
    sys.exit(f"Khong thay {src}. Hay chay Producer voi --run-id {run_id} truoc.")

with open(src, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
secs = [int(r["second"]) for r in rows][:-1] or [0]
vals = [int(r["events_sent"]) for r in rows][:-1] or [int(rows[0]["events_sent"])]

fig, ax = plt.subplots(figsize=(9, 4))
ax.step(secs, vals, where="post", linewidth=2)
ax.fill_between(secs, vals, step="post", alpha=0.25)
ax.set_xlabel("Giây kể từ lúc bắt đầu")
ax.set_ylabel("Sự kiện gửi mỗi giây")
ax.set_title(f"Tốc độ gửi theo thời gian ({run_id})")
ax.set_ylim(0, max(vals) * 1.15)
ax.grid(alpha=0.3)
fig.tight_layout()
os.makedirs("evidence", exist_ok=True)
out = f"evidence/{run_id}_timeline.png"
fig.savefig(out, dpi=150)
print(f"Da ghi {out} | min={min(vals)} max={max(vals)} trung binh={sum(vals) / len(vals):.1f} su kien/giay")
