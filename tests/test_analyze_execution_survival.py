from __future__ import annotations

import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "analyze_execution_survival.py"
SPEC = importlib.util.spec_from_file_location("analyze_execution_survival", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExecutionSurvivalAnalysisTests(unittest.TestCase):
    def test_segments_mode_kind_and_reports_survival(self) -> None:
        payload = {
            "event": "open-survival-observation-v2",
            "asset": "BTC",
            "mode": "live",
            "sample_kind": "live_policy_candidate",
            "open_survival_policy_version": "execution-survival-v2",
            "model_version": "adaptive-median-v6",
            "threshold_pass_sides": ["SELL"],
            "policy_pass_sides": ["SELL"],
            "survival_reserve_bps": {"SELL": "0.20"},
            "snapshots": [
                {"target_offset_ms": horizon, "available": True, "sell_margin": margin}
                for horizon, margin in ((100, "0.1"), (250, "0"), (450, "-0.1"), (1000, "0.2"))
            ],
            "microstructure": {"microprice_bps": "0.01"},
            "feature_snapshots": [
                {
                    "target_offset_ms": horizon,
                    "book_flow_usd": "1",
                    "trade_flow_usd": "2",
                    "past_return_bps": "3",
                    "microprice_bps": "0.01",
                    "depth": {
                        "bands_bps": {
                            "1": {"bid_usd": "100", "ask_usd": "90"}
                        }
                    },
                }
                for horizon in MODULE.FLOW_HORIZONS
            ],
        }

        other = {
            **payload,
            "asset": "XAU",
            "mode": "observe",
            "model_version": "asset-specific-model",
        }
        report = MODULE.observation_report([payload, other])
        group = report[
            "BTC/live/live_policy_candidate/adaptive-median-v6/execution-survival-v2"
        ]
        self.assertEqual(group["observations"], 1)
        self.assertEqual(group["sides"]["SELL"]["policy_candidates"], 1)
        self.assertEqual(group["sides"]["SELL"]["reserve_bps_p50"], "0.20")
        self.assertEqual(group["sides"]["SELL"]["economic_survival"]["450"]["rate"], "0.000000")
        self.assertEqual(group["feature_completeness"]["2000"]["book_flow_usd"]["rate"], "1.000000")
        self.assertEqual(group["feature_completeness"]["2000"]["depth_1bps"]["rate"], "1.000000")
        self.assertIn(
            "XAU/observe/live_policy_candidate/asset-specific-model/execution-survival-v2",
            report,
        )

    def test_reports_latency_and_market_quality(self) -> None:
        traces = [
            {"event": "variational_commit_dispatch", "trace_id": "a", "monotonic_ns": 1_000_000},
            {
                "event": "variational_commit_result",
                "trace_id": "a",
                "monotonic_ns": 11_000_000,
                "ok": True,
                "browser_elapsed_ms": 9,
            },
            {"event": "lighter_order_ack", "trace_id": "a", "monotonic_ns": 16_000_000},
        ]
        latency = MODULE.latency_report(traces)
        self.assertEqual(latency["metrics_ms"]["var_commit_ms"]["p50"], "10")
        self.assertEqual(latency["metrics_ms"]["commit_to_lighter_ack_ms"]["p50"], "5")
        quality = MODULE.market_quality_report(
            [
                {"sample_timestamp_ms": 0, "valid": True, "asset": "BTC"},
                {"sample_timestamp_ms": 1_000, "valid": True, "asset": "BTC"},
                {"sample_timestamp_ms": 2_000, "valid": False, "rejection_reason": "stale", "asset": "BTC"},
            ]
        )["BTC/unknown/market_background/unversioned"]
        self.assertEqual(quality["valid_rows"], 2)
        self.assertEqual(quality["max_valid_gap_ms"], 1_000)
        self.assertEqual(quality["invalid_reasons"], {"stale": 1})

    def test_rounds_segment_asset_direction_kind_and_strategy(self) -> None:
        base = {
            "asset": "BTC",
            "direction": "long_var_short_lighter",
            "strategy": "adaptive-median-v6",
            "open_context_json": (
                '{"openSurvivalPolicyVersion":"execution-survival-v2",'
                '"openHedgeRecoveryPolicyVersion":"open-hedge-recovery-v1"}'
            ),
            "payload_json": '{"round_class":"strategy"}',
            "round_pnl_usd": "0.1",
            "open_execution_loss_usd": "0.01",
            "close_execution_loss_usd": "0.02",
            "effective_quality": "unflagged",
        }
        recovery = {
            **base,
            "asset": "XAU",
            "direction": "short_var_long_lighter",
            "strategy": "xau-model",
            "payload_json": '{"round_class":"protective_recovery"}',
        }

        report = MODULE.round_report([base, recovery])

        self.assertIn(
            "BTC/live/strategy/long_var_short_lighter/adaptive-median-v6/"
            "execution-survival-v2/open-hedge-recovery-v1",
            report,
        )
        self.assertIn(
            "XAU/live/protective_recovery/short_var_long_lighter/xau-model/"
            "execution-survival-v2/open-hedge-recovery-v1",
            report,
        )

    def test_build_report_opens_database_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.sqlite3"
            with self.assertRaises(sqlite3.OperationalError):
                MODULE.build_report(missing)
            self.assertFalse(missing.exists())

    def test_cli_accepts_decimal_since_hours(self) -> None:
        with (
            patch.object(sys, "argv", [str(SCRIPT), "db.sqlite3", "--since-hours", "1.5"]),
            patch.object(MODULE, "build_report", return_value={}) as build_report,
            patch("builtins.print"),
        ):
            self.assertEqual(MODULE.main(), 0)
        self.assertIsInstance(build_report.call_args.args[1], int)


if __name__ == "__main__":
    unittest.main()
