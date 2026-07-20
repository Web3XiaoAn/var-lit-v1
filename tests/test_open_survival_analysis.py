from __future__ import annotations

import unittest

from tools.analyze_open_survival import build_report


class OpenSurvivalAnalysisTests(unittest.TestCase):
    @staticmethod
    def _row(timestamp_ms: int, margin: str = "0.0001") -> dict:
        return {
            "event": "open-survival-observation-v1",
            "session_id": "session-1",
            "mode": "observe",
            "asset": "BTC",
            "model_version": "adaptive-median-v6",
            "sample_timestamp_ms": timestamp_ms,
            "var_age_ms": 1,
            "lighter_age_ms": 20,
            "source_skew_ms": 19,
            "margins": {"BUY": margin, "SELL": margin},
            "snapshots": [
                {
                    "target_offset_ms": target,
                    "actual_offset_ms": target + 2,
                    "available": True,
                    "buy_margin": margin,
                    "sell_margin": margin,
                }
                for target in (0, 100, 250, 450, 1000)
            ],
        }

    def test_one_hour_dense_complete_session_is_qualified(self) -> None:
        report = build_report([self._row(index * 5_000) for index in range(721)])
        self.assertTrue(report["qualified_one_hour"])
        self.assertEqual(report["sides"]["BUY"]["trigger"]["count"], 721)
        self.assertEqual(
            report["sides"]["BUY"]["trigger"]["horizons_ms"]["1000"]["future_positive_rate"],
            "1.000000",
        )

    def test_gap_and_incomplete_snapshots_reject_session(self) -> None:
        rows = [self._row(index * 5_000) for index in range(721)]
        del rows[300:303]
        rows[0]["snapshots"] = []
        report = build_report(rows)
        self.assertFalse(report["qualified_one_hour"])
        self.assertEqual(report["cadence_ms"]["max"], 20_000)

    def test_trigger_and_near_trigger_are_separate(self) -> None:
        report = build_report([self._row(0), self._row(1_000, "-0.00004")])
        self.assertEqual(report["sides"]["SELL"]["trigger"]["count"], 1)
        self.assertEqual(report["sides"]["SELL"]["within_0_5bps"]["count"], 2)


if __name__ == "__main__":
    unittest.main()
