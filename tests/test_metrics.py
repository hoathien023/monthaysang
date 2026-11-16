"""Kiem thu don vi cho common/metrics.py (chay khong can Kafka)."""

import unittest

from common.metrics import mean_std, percentiles, summarize_series, throughput


class TestMetrics(unittest.TestCase):
    def test_throughput(self):
        self.assertEqual(throughput(0, 10.0), 0.0)
        self.assertEqual(throughput(100, 0.0), 0.0)
        self.assertEqual(throughput(100, -1.0), 0.0)
        self.assertAlmostEqual(throughput(5000, 2.5), 2000.0)

    def test_percentiles_empty_and_single(self):
        self.assertEqual(percentiles([]), {50: None, 95: None, 99: None})
        self.assertEqual(percentiles([42.0]), {50: 42.0, 95: 42.0, 99: 42.0})

    def test_percentiles_linear_interpolation(self):
        vals = [10, 20, 30, 40, 50]
        res = percentiles(vals, (0, 25, 50, 75, 100))
        self.assertAlmostEqual(res[0], 10.0)
        self.assertAlmostEqual(res[25], 20.0)
        self.assertAlmostEqual(res[50], 30.0)
        self.assertAlmostEqual(res[75], 40.0)
        self.assertAlmostEqual(res[100], 50.0)

        p = percentiles([1.0, 2.0, 3.0, 4.0], (50, 95, 99))
        self.assertAlmostEqual(p[50], 2.5)
        self.assertAlmostEqual(p[95], 3.85)
        self.assertAlmostEqual(p[99], 3.97)

    def test_mean_std_and_summary(self):
        m, s = mean_std([10.0, 20.0, 30.0])
        self.assertAlmostEqual(m, 20.0)
        self.assertAlmostEqual(s, 10.0)

        empty_s = summarize_series([])
        self.assertEqual(empty_s["count"], 0)
        self.assertIsNone(empty_s["p95"])

        summary = summarize_series([5.0, 10.0, 15.0, 20.0])
        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["min"], 5.0)
        self.assertEqual(summary["max"], 20.0)
        self.assertAlmostEqual(summary["mean"], 12.5)


if __name__ == "__main__":
    unittest.main()
