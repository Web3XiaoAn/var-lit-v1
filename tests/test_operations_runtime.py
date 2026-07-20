from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

import main as main_module
from main import (
    AccountReconcileOutcome,
    AccountSnapshot,
    OrderLifecycle,
    VarOrderIntent,
    VariationalToLighterRuntime,
)


def record(
    key: str,
    side: str,
    var_price: str,
    lighter_price: str,
) -> OrderLifecycle:
    return OrderLifecycle(
        trade_key=key,
        trade_id=key,
        side=side,
        qty=Decimal("1"),
        asset="BTC",
        auto_hedge_enabled=True,
        last_variational_status="filled",
        var_fill_price=Decimal(var_price),
        lighter_fill_price=Decimal(lighter_price),
    )


class TrackingRuntime(VariationalToLighterRuntime):
    def __init__(self) -> None:
        super().__init__(Namespace(auto_hedge=True, lang="zh"))
        self.scheduled: list[OrderLifecycle] = []
        self.persist_count = 0

    def schedule_lighter_order(self, lifecycle: OrderLifecycle) -> bool:
        self.scheduled.append(lifecycle)
        lifecycle.hedge_status = "queued"
        return True

    async def persist_runtime_state(self) -> None:
        self.persist_count += 1


class OperationsRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="operations-runtime-")
        root = Path(cls.temp_dir.name)
        cls.patchers = [
            patch.object(main_module, "LOG_DIR", root),
            patch.object(main_module, "OUTPUT_DIR", root),
            patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
            patch.object(main_module, "RUNTIME_STATE_FILE", root / "runtime-state.json"),
            patch.object(main_module, "EXECUTION_SAMPLES_FILE", root / "samples.json"),
        ]
        for patcher in cls.patchers:
            patcher.start()

    @classmethod
    def tearDownClass(cls) -> None:
        for patcher in reversed(cls.patchers):
            patcher.stop()
        cls.temp_dir.cleanup()

    def test_snapshot_uses_last_ten_final_two_sided_fill_rounds(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
            for index in range(12):
                open_record = record(f"open-{index}", "buy", "100", "101")
                close_record = record(f"close-{index}", "sell", "99.5", "100")
                runtime.records[open_record.trade_key] = open_record
                runtime.records[close_record.trade_key] = close_record
                runtime.record_order.extend(
                    (open_record.trade_key, close_record.trade_key)
                )

            snapshot = await runtime.operations_dashboard_snapshot()

            self.assertEqual(snapshot["environment"], "runtime")
            self.assertEqual(len(snapshot["recentRounds"]), 10)
            self.assertEqual(snapshot["recentRounds"][0]["number"], 3)
            self.assertEqual(snapshot["metrics"]["totalOpenWear"], "10")
            self.assertEqual(snapshot["metrics"]["totalCloseWear"], "-5.0")
            self.assertEqual(snapshot["metrics"]["totalWear"], "5.0")
            self.assertEqual(snapshot["metrics"]["averageWear"], "0.5")
            self.assertEqual(snapshot["metrics"]["positiveRounds"], 10)
            self.assertEqual(
                snapshot["recentRounds"][0]["direction"],
                "多 Var / 空 Lighter",
            )

        asyncio.run(run_case())

    def test_lighter_only_action_creates_exact_reduce_only_recovery(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0.00782"), 0, main_module.utc_now()
            )
            preview = await runtime.prepare_operations_action(
                "close_lighter_residual", {}
            )
            self.assertTrue(preview["allowed"])

            result = await runtime.execute_operations_action(
                "close_lighter_residual", {}, preview
            )

            self.assertTrue(result["ok"])
            self.assertTrue(runtime.operator_open_paused)
            self.assertEqual(len(runtime.scheduled), 1)
            lifecycle = runtime.scheduled[0]
            self.assertEqual(lifecycle.side, "buy")
            self.assertEqual(lifecycle.qty, Decimal("0.00782"))
            self.assertEqual(
                lifecycle.lighter_target_qty_override,
                Decimal("0.00782"),
            )
            self.assertTrue(lifecycle.lighter_reduce_only)
            self.assertIsNone(lifecycle.var_fill_price)

        asyncio.run(run_case())

    def test_var_only_intent_disables_automatic_lighter_hedge(self) -> None:
        runtime = TrackingRuntime()
        lifecycle = record("operator-close", "sell", "100", "100")
        lifecycle.lighter_reduce_only = True
        intent = VarOrderIntent(
            phase="operator_var_only_close",
            side="SELL",
            amount=Decimal("100"),
            sent_monotonic=1.0,
            market="BTC",
        )

        runtime._apply_intent_metadata_locked(lifecycle, intent)

        self.assertFalse(lifecycle.auto_hedge_enabled)
        self.assertFalse(lifecycle.lighter_reduce_only)
        self.assertEqual(lifecycle.strategy_phase, "operator_var_only_close")

    def test_config_update_preserves_secret_and_stages_restart_values(self) -> None:
        runtime = TrackingRuntime()
        payload = {
            "orderNotionalUsd": "500",
            "maxNormalRoundWearUsd": "-0.02",
            "buyThresholdMinPct": "0.05",
            "sellThresholdMinPct": "-0.073",
            "maxQuoteAgeMs": "600",
            "earlyExitMinutes": "30",
            "executionMode": "observe",
        }
        updates, error, _facts = runtime._config_updates_from_dashboard(payload)
        self.assertIsNone(error)
        assert updates is not None
        self.assertEqual(updates["STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS"], "0.40")
        with tempfile.TemporaryDirectory(prefix="operations-env-") as temp:
            dotenv = Path(temp) / ".env"
            dotenv.write_text(
                "LIGHTER_PRIVATE_KEY=secret-value\n"
                "LIGHTER_API_KEY_INDEX=2\n"
                "LIGHTER_ACCOUNT_INDEX=3\n"
                "STRATEGY_EXECUTION_MODE=observe\n"
                "STRATEGY_ORDER_NOTIONAL_USD=200\n"
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.05\n"
                "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT=-0.073\n",
                encoding="utf-8",
            )
            runtime._write_dotenv_updates(dotenv, updates)
            source = dotenv.read_text(encoding="utf-8")

        self.assertIn("LIGHTER_PRIVATE_KEY=secret-value", source)
        self.assertIn("STRATEGY_ORDER_NOTIONAL_USD=500", source)
        self.assertIn("STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS=0.40", source)

    def test_force_close_requires_fresh_match_and_command_channel(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("1"), Decimal("-1"), 0, main_module.utc_now()
            )
            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
            with patch.object(
                runtime.runtime.command_broker,
                "extension_connected",
                AsyncMock(return_value=True),
            ):
                preview = await runtime.prepare_operations_action(
                    "force_round_close", {}
                )
            self.assertTrue(preview["allowed"])

            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MISMATCH
            with patch.object(
                runtime.runtime.command_broker,
                "extension_connected",
                AsyncMock(return_value=True),
            ):
                denied = await runtime.prepare_operations_action(
                    "force_round_close", {}
                )
            self.assertFalse(denied["allowed"])

        asyncio.run(run_case())

    def test_force_close_reports_failed_var_submission(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("1"), Decimal("-1"), 0, main_module.utc_now()
            )
            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
            with patch.object(
                runtime.runtime.command_broker,
                "extension_connected",
                AsyncMock(return_value=True),
            ):
                preview = await runtime.prepare_operations_action(
                    "force_round_close", {}
                )
            with patch.object(
                runtime,
                "emergency_flatten_var",
                AsyncMock(return_value=False),
            ):
                result = await runtime.execute_operations_action(
                    "force_round_close", {}, preview
                )

            self.assertFalse(result["ok"])
            self.assertIn("未被交易通道受理", result["error"])

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
