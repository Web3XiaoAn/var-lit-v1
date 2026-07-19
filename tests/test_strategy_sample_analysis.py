from __future__ import annotations

import unittest

from tools.analyze_strategy_samples import build_report


class StrategySampleAnalysisTests(unittest.TestCase):
    @staticmethod
    def _row(timestamp_ms: int, *, valid: bool = True) -> dict:
        row = {
            "version": "adaptive-market-sample-v1",
            "sample_timestamp_ms": timestamp_ms,
            "asset": "BTC",
            "market_generation": 1,
            "reference_notional_usd": "500",
            "order_notional_usd": "200",
            "valid": valid,
        }
        if valid:
            row.update(reference_buy_rate="0.001", reference_sell_rate="-0.001")
        else:
            row["rejection_reason"] = "market_data_stale"
        return row

    def test_one_runtime_hour_at_point_two_hz_is_qualified(self) -> None:
        rows = [self._row(index * 5_000) for index in range(721)]
        report = build_report(rows)
        self.assertTrue(report["qualified_runtime_1h"])
        self.assertIn("5m", report["windows"])
        self.assertEqual(report["windows"]["1h"]["count"], 721)

    def test_runtime_gap_rejects_quality(self) -> None:
        rows = [self._row(index * 5_000) for index in range(721)]
        del rows[300:312]
        report = build_report(rows)
        self.assertFalse(report["qualified_runtime_1h"])
        self.assertEqual(report["max_valid_gap_ms"], 65_000)

    def test_malformed_row_rejects_quality(self) -> None:
        rows = [self._row(index * 5_000) for index in range(721)]
        report = build_report(rows, malformed_rows=1)
        self.assertFalse(report["qualified_runtime_1h"])
        self.assertEqual(report["malformed_rows"], 1)


if __name__ == "__main__":
    unittest.main()
