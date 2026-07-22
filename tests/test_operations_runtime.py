from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

from adaptive_strategy import WindowStats
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
        self._runtime_state_loaded = True
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

    def test_actions_are_blocked_until_runtime_state_is_loaded(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime._runtime_state_loaded = False

            preview = await runtime.prepare_operations_action("pause_open", {})
            snapshot = await runtime.operations_dashboard_snapshot()

            self.assertFalse(preview["allowed"])
            self.assertIn("恢复持久化状态", preview["reason"])
            self.assertEqual(snapshot["strategy"]["status"], "恢复状态中")
            self.assertTrue(snapshot["strategy"]["openPaused"])
            self.assertFalse(snapshot["strategy"]["automationReady"])
            self.assertIn("恢复持久化状态", snapshot["health"]["headline"])
            self.assertFalse(
                runtime.automation_can_submit_var_order("last_auto_var_order_status")
            )
            self.assertEqual(
                runtime.last_auto_var_order_status,
                "restoring persisted runtime state",
            )

        asyncio.run(run_case())

    def test_confirmation_survives_unchanged_account_snapshot_refresh(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            preview = await runtime.prepare_operations_action("pause_open", {})
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )

            result = await runtime.execute_operations_action(
                "pause_open", {}, preview
            )

            self.assertTrue(result["ok"])
            self.assertTrue(runtime.operator_open_paused)

        asyncio.run(run_case())

    def test_startup_refreshes_var_page_when_forwarder_does_not_reconnect(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            with patch.object(
                main_module.time,
                "monotonic",
                side_effect=[0.0, 0.0, 4.0],
            ), patch.object(
                type(runtime.runtime.monitor),
                "get_trading_state",
                AsyncMock(return_value={"heartbeat_age": None}),
            ), patch.object(
                type(runtime.runtime.command_broker),
                "extension_connected",
                AsyncMock(return_value=True),
            ), patch.object(
                runtime,
                "_refresh_variational_page_via_cdp",
                AsyncMock(),
            ) as refresh:
                await runtime.wait_for_variational_ready()

            refresh.assert_awaited_once()

        asyncio.run(run_case())

    def test_snapshot_uses_fixed_ten_round_batches(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
            captured_at_ms = int(main_module.time.time() * 1000)
            clock = main_module.SourceClock(captured_at_ms, captured_at_ms, 0)
            runtime.last_market_frame = main_module.MarketFrame(
                asset="BTC",
                captured_at_ms=captured_at_ms,
                variational_clock=clock,
                lighter_clock=clock,
                source_skew_ms=0,
                var_bid=Decimal("100"),
                var_ask=Decimal("100.1"),
                lighter_reference_buy_vwap=Decimal("100.2"),
                lighter_reference_sell_vwap=Decimal("100.3"),
                lighter_actual_buy_vwap=Decimal("100.2"),
                lighter_actual_sell_vwap=Decimal("100.3"),
                reference_notional_usd=Decimal("500"),
                actual_notional_usd=Decimal("500"),
                reference_rates=main_module.DirectionalRates(
                    Decimal("0.0007"), Decimal("0.0004")
                ),
                actual_rates=main_module.DirectionalRates(
                    Decimal("0.0007"), Decimal("0.0004")
                ),
            )
            runtime.active_parameter_epoch = main_module.build_parameter_candidate(
                now_ms=captured_at_ms,
                model=runtime.strategy_model,
                config_hash=runtime.strategy_config_hash,
                stats=runtime.strategy_model.calibration_stats,
                reference_notional_usd=runtime.strategy_config.reference_notional_usd,
                order_notional_usd=runtime.strategy_config.order_notional_usd,
                reserve_bps_per_leg=runtime.strategy_config.provisional_reserve_bps_per_leg,
                max_normal_round_wear_bps=runtime.strategy_config.max_normal_round_wear_bps,
            )
            for minutes, ready in ((5, True), (30, False)):
                for side, median in (
                    (main_module.StrategySide.BUY, Decimal("0.0005")),
                    (main_module.StrategySide.SELL, Decimal("0.0006")),
                ):
                    runtime.strategy_window_stats[side][minutes] = (
                        WindowStats(
                            side=side,
                            window_minutes=minutes,
                            median=median,
                            q80=median,
                            mad=Decimal("0"),
                            sample_count=minutes * 60,
                            span_ms=minutes * 60 * 1000,
                            density_per_second=Decimal("1"),
                            max_gap_ms=1000,
                            latest_age_ms=0,
                            ready=ready,
                            reason="ready" if ready else "insufficient_span",
                        )
                    )
            _holding_day, holding_day_start_ms, _holding_day_end_ms = (
                main_module.beijing_day_bounds_ms()
            )
            test_now = datetime.fromtimestamp(
                (holding_day_start_ms + 12 * 60 * 60 * 1_000) / 1_000,
                timezone.utc,
            )
            for index in range(12):
                open_record = record(f"open-{index}", "buy", "100", "101")
                close_record = record(f"close-{index}", "sell", "99.5", "100")
                opened_at = test_now - timedelta(
                    seconds=(12 - index) * 120
                )
                open_record.var_fill_ts_iso = opened_at.isoformat()
                close_record.var_fill_ts_iso = (
                    opened_at + timedelta(seconds=60)
                ).isoformat()
                runtime.records[open_record.trade_key] = open_record
                runtime.records[close_record.trade_key] = close_record
                runtime.record_order.extend(
                    (open_record.trade_key, close_record.trade_key)
                )
            current_open = record("current-open", "buy", "100", "100.1")
            current_open.var_fill_ts_iso = (
                test_now - timedelta(seconds=30)
            ).isoformat()
            runtime.records[current_open.trade_key] = current_open
            runtime.record_order.append(current_open.trade_key)
            runtime.last_strategy_decision = main_module.StrategyDecision(
                main_module.StrategyAction.NO_ACTION,
                "close_floor_not_met",
                close_candidate=main_module.CloseCandidate(
                    close_direction=main_module.StrategySide.SELL,
                    frame_captured_at_ms=int(test_now.timestamp() * 1_000),
                    frozen_epoch_id="snapshot-test",
                    held_seconds=60,
                    actual_close_rate=Decimal("-0.0007"),
                    regression_target_rate=Decimal("0"),
                    expected_close_pnl_usd=Decimal("-0.07"),
                    close_reserve_usd=Decimal("0.01"),
                    round_lower_bound_usd=Decimal("0.02"),
                    required_floor_usd=Decimal("0"),
                    regression_passed=False,
                    max_hold_alert=False,
                ),
            )

            with patch.object(
                main_module.time,
                "time",
                return_value=test_now.timestamp(),
            ):
                snapshot = await runtime.operations_dashboard_snapshot()

            self.assertEqual(snapshot["environment"], "runtime")
            self.assertEqual(len(snapshot["recentRounds"]), 2)
            self.assertEqual(snapshot["recentRounds"][0]["number"], 1)
            self.assertEqual(snapshot["metrics"]["totalOpenWear"], "2")
            self.assertEqual(snapshot["metrics"]["totalCloseWear"], "-1.0")
            self.assertEqual(snapshot["metrics"]["totalWear"], "1.0")
            self.assertEqual(snapshot["metrics"]["averageWear"], "0.5")
            self.assertEqual(snapshot["metrics"]["positiveRounds"], 2)
            self.assertEqual(snapshot["metrics"]["batchHoldingSeconds"], 120)
            self.assertEqual(snapshot["metrics"]["averageHoldingSeconds"], 60)
            self.assertGreaterEqual(snapshot["metrics"]["todayHoldingSeconds"], 750)
            self.assertLess(snapshot["metrics"]["todayHoldingSeconds"], 755)
            self.assertEqual(snapshot["metrics"]["todayTradingVolumeUsd"], "2494.0")
            self.assertEqual(snapshot["metrics"]["todayWear"], "6.0")
            self.assertEqual(snapshot["metrics"]["todayAverageWear"], "0.5")
            self.assertEqual(snapshot["metrics"]["todayCompletedRounds"], 12)
            self.assertEqual(snapshot["metrics"]["holdingTimezone"], "Asia/Shanghai")
            self.assertEqual(snapshot["recentRounds"][0]["heldSeconds"], 60)
            self.assertIsNone(snapshot["positions"]["idleSeconds"])
            current_basis = snapshot["metrics"]["currentBasis"]
            self.assertTrue(current_basis["fresh"])
            self.assertEqual(current_basis["referenceLongVar"], "0.0007")
            self.assertEqual(current_basis["referenceShortVar"], "0.0004")
            self.assertEqual(current_basis["referenceNotionalUsd"], "500")
            self.assertEqual(current_basis["estimatedOpenLongUsd"], "0.3500")
            self.assertEqual(current_basis["estimatedOpenShortUsd"], "0.2000")
            epoch = runtime.active_parameter_epoch
            assert epoch is not None
            thresholds = snapshot["metrics"]["openThresholds"]
            self.assertTrue(thresholds["fresh"])
            self.assertEqual(
                Decimal(thresholds["longVar"]),
                epoch.component(main_module.StrategySide.BUY).final
                + runtime.effective_open_execution_headroom_bps(
                    "BUY", runtime.strategy_config.order_notional_usd
                )
                / Decimal("10000"),
            )
            medians = snapshot["metrics"]["basisMedians"]
            self.assertEqual(medians["5m"]["longVar"], "0.0005")
            self.assertEqual(medians["5m"]["shortVar"], "0.0006")
            self.assertTrue(medians["5m"]["ready"])
            self.assertEqual(medians["30m"]["longVar"], "0.0005")
            self.assertFalse(medians["30m"]["ready"])
            self.assertIsNone(medians["1h"]["longVar"])
            current_pnl = snapshot["metrics"]["currentPositionPnl"]
            self.assertTrue(current_pnl["active"])
            self.assertEqual(current_pnl["open"], "0.1")
            self.assertEqual(current_pnl["openBasis"], "0.001")
            self.assertEqual(current_pnl["closeEstimate"], "-0.07")
            self.assertEqual(current_pnl["closeReserve"], "-0.01")
            self.assertEqual(current_pnl["requiredFloor"], "0")
            runtime.last_market_frame = None
            runtime.active_parameter_epoch = None
            with patch.object(
                main_module.time,
                "time",
                return_value=test_now.timestamp(),
            ):
                cached_snapshot = await runtime.operations_dashboard_snapshot()
            cached_basis = cached_snapshot["metrics"]["currentBasis"]
            self.assertFalse(cached_basis["fresh"])
            self.assertEqual(
                cached_basis["referenceLongVar"],
                current_basis["referenceLongVar"],
            )
            self.assertEqual(
                cached_basis["estimatedOpenLongUsd"],
                current_basis["estimatedOpenLongUsd"],
            )
            cached_thresholds = cached_snapshot["metrics"]["openThresholds"]
            self.assertFalse(cached_thresholds["fresh"])
            self.assertEqual(cached_thresholds["longVar"], thresholds["longVar"])
            self.assertEqual(
                cached_thresholds["shortVar"],
                thresholds["shortVar"],
            )
            self.assertEqual(
                snapshot["recentRounds"][0]["direction"],
                "多 Var / 空 Lighter",
            )

        asyncio.run(run_case())

    def test_snapshot_reports_idle_time_only_while_flat(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
            runtime._last_round_closed_at = 1_000.0

            with patch.object(main_module.time, "time", return_value=1_123.9):
                snapshot = await runtime.operations_dashboard_snapshot()

            self.assertIsNone(snapshot["positions"]["heldSeconds"])
            self.assertEqual(snapshot["positions"]["idleSeconds"], 123)

        asyncio.run(run_case())

    def test_round_batch_survives_parameter_change_and_legacy_id(self) -> None:
        runtime = TrackingRuntime()
        row = {
            "key": "open|close",
            "directionKey": "long_var",
            "direction": "多 Var / 空 Lighter",
            "openWear": "0.01",
            "closeWear": "-0.005",
            "roundWear": "0.005",
            "recovery": False,
            "openedAtMs": 1_000,
            "closedAtMs": 2_000,
        }
        stable_id = runtime._dashboard_round_strategy_id()
        self.assertNotIn(runtime.strategy_config_hash, stable_id)

        runtime._restore_dashboard_round_batch(
            {
                "dashboard_round_strategy_id": (
                    f"{stable_id}:{runtime.strategy_config_hash}"
                ),
                "strategy_config_hash": runtime.strategy_config_hash,
                "dashboard_round_batch": [row],
                "dashboard_round_cursor_ms": 2_000,
            }
        )

        self.assertEqual(len(runtime._dashboard_round_batch), 1)
        self.assertEqual(runtime._dashboard_round_batch[0]["heldSeconds"], 1)

    def test_operator_action_blocks_strategy_order_submission(self) -> None:
        runtime = TrackingRuntime()
        runtime.strategy_config = replace(
            runtime.strategy_config,
            execution_mode="live",
        )
        runtime.automation_ready = True
        runtime._operator_action_inflight = True

        self.assertFalse(
            runtime.automation_can_submit_var_order("last_auto_var_order_status")
        )
        self.assertIn("operator action", runtime.last_auto_var_order_status)
        self.assertEqual(
            runtime.live_open_block_reason(),
            "operator action is in progress",
        )

    def test_scheduled_var_refresh_blocks_orders_only_while_refreshing(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.automation_ready = True

            async def refresh() -> bool:
                self.assertTrue(runtime._variational_refresh_in_progress)
                self.assertFalse(
                    runtime.automation_can_submit_var_order(
                        "last_auto_var_order_status"
                    )
                )

            with patch.object(
                runtime,
                "_refresh_variational_page_via_cdp",
                side_effect=refresh,
            ) as mocked:
                await runtime.refresh_variational_page_when_safe()

            mocked.assert_awaited_once()
            self.assertFalse(runtime._variational_refresh_in_progress)
            self.assertTrue(
                runtime.automation_can_submit_var_order(
                    "last_auto_var_order_status"
                )
            )

        asyncio.run(run_case())

    def test_scheduled_var_refresh_clears_guard_after_failure(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            with patch.object(
                runtime,
                "_refresh_variational_page_via_cdp",
                AsyncMock(side_effect=RuntimeError("refresh failed")),
            ):
                with self.assertRaisesRegex(RuntimeError, "refresh failed"):
                    await runtime.refresh_variational_page_when_safe()
            self.assertFalse(runtime._variational_refresh_in_progress)

        asyncio.run(run_case())

    def test_priority_var_refresh_queues_behind_only_active_var_request(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime._auto_var_order_inflight = True

            async def release_request(delay: float) -> None:
                self.assertEqual(delay, 0.05)
                self.assertTrue(runtime._variational_refresh_in_progress)
                runtime._auto_var_order_inflight = False

            with patch.object(
                main_module.asyncio,
                "sleep",
                AsyncMock(side_effect=release_request),
            ) as sleep, patch.object(
                runtime,
                "_refresh_variational_page_via_cdp",
                AsyncMock(),
            ) as refresh:
                await runtime.refresh_variational_page_when_safe()

            sleep.assert_awaited_once_with(0.05)
            refresh.assert_awaited_once()
            self.assertFalse(runtime._variational_refresh_in_progress)

        asyncio.run(run_case())

    def test_account_refresh_preserves_position_and_reconciles(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)

            async def refresh() -> None:
                self.assertTrue(runtime._variational_refresh_in_progress)

            with patch.object(
                runtime,
                "latest_confirmed_variational_fill_time",
                AsyncMock(return_value=datetime.now(timezone.utc)),
            ), patch.object(
                runtime,
                "_refresh_variational_page_via_cdp",
                side_effect=refresh,
            ), patch.object(
                runtime,
                "wait_for_authoritative_portfolio_after",
                AsyncMock(return_value=True),
            ) as wait, patch.object(
                runtime,
                "reconcile_accounts",
                AsyncMock(return_value=True),
            ) as reconcile:
                matched = await runtime.refresh_variational_account_and_reconcile()

            self.assertTrue(matched)
            self.assertIn(opened.trade_key, runtime.records)
            self.assertFalse(runtime._variational_refresh_in_progress)
            wait.assert_awaited_once()
            reconcile.assert_awaited_once_with(
                allow_resume=True,
                after_page_refresh=True,
            )

        asyncio.run(run_case())

    def test_stale_post_fill_snapshot_auto_refreshes_once_per_cooldown(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_reconcile_outcome = AccountReconcileOutcome.STALE
            filled_at = datetime.now(timezone.utc) - timedelta(seconds=20)
            with patch.object(
                runtime,
                "latest_confirmed_variational_fill_time",
                AsyncMock(return_value=filled_at),
            ), patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                AsyncMock(return_value=True),
            ) as refresh:
                first = await runtime.refresh_stale_variational_portfolio_if_safe()
                second = await runtime.refresh_stale_variational_portfolio_if_safe()

            self.assertTrue(first)
            self.assertFalse(second)
            refresh.assert_awaited_once()

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

    def test_lighter_only_action_closes_short_residual_with_buy(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("-0.00782"), 0, main_module.utc_now()
            )
            preview = await runtime.prepare_operations_action(
                "close_lighter_residual", {}
            )
            result = await runtime.execute_operations_action(
                "close_lighter_residual", {}, preview
            )

            self.assertTrue(result["ok"])
            self.assertEqual(len(runtime.scheduled), 1)
            lifecycle = runtime.scheduled[0]
            self.assertEqual(lifecycle.side, "sell")
            self.assertEqual(lifecycle.qty, Decimal("0.00782"))
            self.assertTrue(lifecycle.lighter_reduce_only)

        asyncio.run(run_case())

    def test_var_only_action_submits_recovery_without_lighter_hedge(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("1"), Decimal("0"), 0, main_module.utc_now()
            )
            with patch.object(
                runtime.runtime.command_broker,
                "extension_connected",
                AsyncMock(return_value=True),
            ), patch.object(
                runtime,
                "emergency_flatten_var",
                AsyncMock(return_value=True),
            ) as flatten:
                preview = await runtime.prepare_operations_action(
                    "close_var_residual", {}
                )
                result = await runtime.execute_operations_action(
                    "close_var_residual", {}, preview
                )

            self.assertTrue(result["ok"])
            self.assertTrue(runtime.operator_open_paused)
            flatten.assert_awaited_once_with(
                opened,
                intent_phase="operator_var_only_close",
            )

        asyncio.run(run_case())

    def test_pause_button_only_changes_operator_open_permission(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.automation_paused = True
            runtime.automation_pause_reason = "independent safety pause"

            first = await runtime.prepare_operations_action("pause_open", {})
            first_result = await runtime.execute_operations_action(
                "pause_open", {}, first
            )
            second = await runtime.prepare_operations_action("pause_open", {})
            second_result = await runtime.execute_operations_action(
                "pause_open", {}, second
            )

            self.assertTrue(first_result["ok"])
            self.assertTrue(second_result["ok"])
            self.assertFalse(runtime.operator_open_paused)
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(
                runtime.automation_pause_reason,
                "independent safety pause",
            )

        asyncio.run(run_case())

    def test_refresh_button_allows_tracked_position_and_preserves_open_state(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("1"), Decimal("-1"), 0, main_module.utc_now()
            )
            with patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                AsyncMock(return_value=True),
            ) as refresh:
                preview = await runtime.prepare_operations_action(
                    "refresh_var", {}
                )
                result = await runtime.execute_operations_action(
                    "refresh_var", {}, preview
                )

            self.assertTrue(result["ok"])
            self.assertFalse(runtime.operator_open_paused)
            refresh.assert_awaited_once()

        asyncio.run(run_case())

    def test_refresh_button_is_available_during_ambiguous_commit(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.pending_var_intent = VarOrderIntent(
                phase="close",
                side="BUY",
                amount=Decimal("500"),
                sent_monotonic=0,
                market="BTC",
                state=main_module.VAR_INTENT_COMMIT_AMBIGUOUS,
            )
            with patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                AsyncMock(return_value=True),
            ) as refresh:
                preview = await runtime.prepare_operations_action(
                    "refresh_var", {}
                )
                result = await runtime.execute_operations_action(
                    "refresh_var", {}, preview
                )

            self.assertTrue(preview["allowed"])
            self.assertTrue(result["ok"])
            refresh.assert_awaited_once()

        asyncio.run(run_case())

    def test_second_button_is_rejected_while_first_action_is_executing(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            preview = await runtime.prepare_operations_action("refresh_var", {})
            entered = asyncio.Event()
            release = asyncio.Event()

            async def refresh() -> bool:
                entered.set()
                await release.wait()
                return True

            with patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                side_effect=refresh,
            ):
                executing = asyncio.create_task(
                    runtime.execute_operations_action("refresh_var", {}, preview)
                )
                await entered.wait()
                denied = await runtime.prepare_operations_action(
                    "pause_open", {}
                )
                release.set()
                result = await executing

            self.assertFalse(denied["allowed"])
            self.assertEqual(denied["reason"], "另一个操作正在执行")
            self.assertTrue(result["ok"])
            self.assertFalse(runtime._operator_action_inflight)
            self.assertIsNone(runtime._operator_action_owner)

        asyncio.run(run_case())

    def test_reconcile_refreshes_stale_snapshot(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.operator_open_paused = True
            runtime.last_reconcile_outcome = AccountReconcileOutcome.STALE
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            with patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                AsyncMock(return_value=True),
            ) as refresh:
                preview = await runtime.prepare_operations_action(
                    "reconcile", {}
                )
                result = await runtime.execute_operations_action(
                    "reconcile", {}, preview
                )

            self.assertTrue(result["ok"])
            refresh.assert_awaited_once()
            self.assertTrue(runtime.operator_open_paused)

        asyncio.run(run_case())

    def test_reconcile_can_resolve_quiet_ambiguous_commit(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.pending_var_intent = VarOrderIntent(
                phase="close",
                side="BUY",
                amount=Decimal("500"),
                sent_monotonic=0,
                market="BTC",
                state=main_module.VAR_INTENT_COMMIT_AMBIGUOUS,
            )

            preview = await runtime.prepare_operations_action("reconcile", {})

            self.assertTrue(preview["allowed"])

        asyncio.run(run_case())

    def test_reconcile_rejects_an_active_non_ambiguous_commit(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.pending_var_intent = VarOrderIntent(
                phase="close",
                side="BUY",
                amount=Decimal("500"),
                sent_monotonic=0,
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTING,
            )

            preview = await runtime.prepare_operations_action("reconcile", {})

            self.assertFalse(preview["allowed"])
            self.assertIn("执行", preview["reason"])

        asyncio.run(run_case())

    def test_reconcile_refreshes_while_position_is_tracked(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            opened = record("open", "buy", "100", "101")
            runtime.records[opened.trade_key] = opened
            runtime.record_order.append(opened.trade_key)
            runtime.last_reconcile_outcome = AccountReconcileOutcome.STALE
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("1"), Decimal("-1"), 0, main_module.utc_now()
            )
            with patch.object(
                runtime,
                "reconcile_accounts",
                AsyncMock(return_value=False),
            ), patch.object(
                runtime,
                "refresh_variational_account_and_reconcile",
                AsyncMock(return_value=True),
            ) as refresh:
                preview = await runtime.prepare_operations_action(
                    "reconcile", {}
                )
                result = await runtime.execute_operations_action(
                    "reconcile", {}, preview
                )

            self.assertTrue(result["ok"])
            refresh.assert_awaited_once()

        asyncio.run(run_case())

    def test_config_commit_rechecks_account_freshness(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            payload = {
                "orderNotionalUsd": "500",
                "maxNormalRoundWearUsd": "-0.02",
                "buyThresholdMinPct": "0.05",
                "sellThresholdMinPct": "-0.073",
                "maxQuoteAgeMs": "600",
                "earlyExitMinutes": "30",
                "executionMode": "observe",
            }
            preview = await runtime.prepare_operations_action(
                "stage_config", payload
            )
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"),
                Decimal("0"),
                0,
                (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
            )
            with patch.object(runtime, "_write_dotenv_updates") as write:
                result = await runtime.execute_operations_action(
                    "stage_config", payload, preview
                )

            self.assertFalse(result["ok"])
            self.assertIn("快照已过期", result["error"])
            write.assert_not_called()

        asyncio.run(run_case())

    def test_config_button_writes_only_the_external_runtime_file(self) -> None:
        async def run_case() -> None:
            runtime = TrackingRuntime()
            runtime.last_account_snapshot = AccountSnapshot(
                Decimal("0"), Decimal("0"), 0, main_module.utc_now()
            )
            payload = {
                "orderNotionalUsd": "500",
                "maxNormalRoundWearUsd": "-0.02",
                "buyThresholdMinPct": "0.058",
                "sellThresholdMinPct": "-0.073",
                "maxQuoteAgeMs": "600",
                "earlyExitMinutes": "30",
                "executionMode": "observe",
            }
            with tempfile.TemporaryDirectory(prefix="operations-config-") as temp:
                dotenv = Path(temp) / "runtime.env"
                dotenv.write_text(
                    "LIGHTER_PRIVATE_KEY=secret\n"
                    "STRATEGY_EXECUTION_MODE=live\n"
                    "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.05\n",
                    encoding="utf-8",
                )
                with patch.object(main_module, "DOTENV_FILE", dotenv):
                    preview = await runtime.prepare_operations_action(
                        "stage_config", payload
                    )
                    result = await runtime.execute_operations_action(
                        "stage_config", payload, preview
                    )
                source = dotenv.read_text(encoding="utf-8")

            self.assertTrue(result["ok"])
            self.assertIn("LIGHTER_PRIVATE_KEY=secret", source)
            self.assertIn(
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.058",
                source,
            )

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
            ), patch.object(
                runtime.runtime.command_broker,
                "extension_connected",
                AsyncMock(return_value=True),
            ):
                result = await runtime.execute_operations_action(
                    "force_round_close", {}, preview
                )

            self.assertFalse(result["ok"])
            self.assertIn("未被交易通道受理", result["error"])

        asyncio.run(run_case())

    def test_force_close_success_pauses_new_opens_and_submits_once(self) -> None:
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
            ), patch.object(
                runtime,
                "emergency_flatten_var",
                AsyncMock(return_value=True),
            ) as flatten:
                preview = await runtime.prepare_operations_action(
                    "force_round_close", {}
                )
                result = await runtime.execute_operations_action(
                    "force_round_close", {}, preview
                )

            self.assertTrue(result["ok"])
            self.assertTrue(runtime.operator_open_paused)
            flatten.assert_awaited_once_with(opened)

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
