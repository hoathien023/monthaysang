"""common/metrics.py

Thu vien do luong dung chung cho Consumer (TV2) va bo Benchmark Runner (TV3).
Cung cap:
  - throughput(count, seconds): toc do su kien/giay (eps)
  - percentiles(values, qs=(50, 95, 99)): tinh bách phân vị bang noi suy tuyen tinh
  - mean_std(values): trung binh va do lech chuan mau (ddof=1)
  - summarize_series(values): tong hop count, min, max, mean, std, p50, p95, p99
"""

from __future__ import annotations

import math
from typing import Iterable, Optional, Sequence


def throughput(count: int, seconds: float) -> float:
    """Tinh thong luong (su kien/giay). Tra ve 0.0 neu seconds <= 0 hoac count <= 0."""
    if count <= 0 or seconds <= 0:
        return 0.0
    return count / seconds


def percentiles(
    values: Iterable[float],
    qs: Sequence[int | float] = (50, 95, 99),
) -> dict[int | float, Optional[float]]:
    """Tinh cac bach phan vi theo noi suy tuyen tinh (giong numpy.percentile).

    Neu tap gia tri rong, tra ve {q: None} cho moi q trong qs.
    """
    data = sorted(float(v) for v in values)
    if not data:
        return {q: None for q in qs}

    n = len(data)
    out: dict[int | float, Optional[float]] = {}
    for q in qs:
        if q < 0 or q > 100:
            raise ValueError(f"Percentile q phai nam trong [0, 100], nhan duoc: {q}")
        if n == 1:
            out[q] = data[0]
            continue
        pos = (n - 1) * (q / 100.0)
        lo = math.floor(pos)
        hi = math.ceil(pos)
        if lo == hi:
            out[q] = data[lo]
        else:
            frac = pos - lo
            out[q] = data[lo] + (data[hi] - data[lo]) * frac
    return out


def mean_std(values: Iterable[float]) -> tuple[Optional[float], Optional[float]]:
    """Tinh (mean, sample_std). Tap rong -> (None, None); 1 phan tu -> (val, 0.0)."""
    data = [float(v) for v in values]
    n = len(data)
    if n == 0:
        return None, None
    mean = sum(data) / n
    if n == 1:
        return mean, 0.0
    var = sum((x - mean) ** 2 for x in data) / (n - 1)
    return mean, math.sqrt(var)


def summarize_series(values: Iterable[float]) -> dict[str, Optional[float]]:
    """Tong hop day so lieu (vi du danh sach do tre ms hoac throughput nhieu lan chay)."""
    data = sorted(float(v) for v in values)
    if not data:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "p50": None,
            "p95": None,
            "p99": None,
        }
    mean, std = mean_std(data)
    pcts = percentiles(data, (50, 95, 99))
    return {
        "count": len(data),
        "min": data[0],
        "max": data[-1],
        "mean": mean,
        "std": std,
        "p50": pcts[50],
        "p95": pcts[95],
        "p99": pcts[99],
    }
