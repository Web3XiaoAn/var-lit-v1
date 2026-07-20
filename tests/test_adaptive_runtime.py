from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

import main as main_module
from main import (
    AccountSnapshot,
    CANARY_SESSION_ARMED,
    CANARY_SESSION_HALTED,
    VariationalToLighterRuntime,
    utc_now,
)
from tests.test_dashboard_calculations import make_open_candidate, make_record
from adaptive_strategy.serialization import open_candidate_to_payload
from adaptive_strategy.model_config import load_model_config


class AdaptiveRuntimeTests(unittest.TestCase):
    @staticmethod
    def _empty_runtime_payload(runtime: VariationalToLighterRuntime) -> dict:
        return {
            "version": 2,
            "asset": "BTC",
            "lighter_ticker": "BTC",
            "strategy_model": main_module.ADAPTIVE_MODEL_VERSION,
            "strategy_model_hash": runtime.strategy_model.model_hash,
            "records": [],
            "pending_var_intent": None,
            "automation_paused": False,
            "automation_pause_reason": "-",
            "last_round_closed_at": 0,
            "canary_session": {
                "round_count": 0,
                "cumulative_loss_usd": "0",
                "consecutive_losses": 0,
                "state": "OBSERVING",
            },
        }

    def test_observe_mode_exposes_candidate_but_returns_no_order_signal(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            runtime.active_parameter_epoch = candidate.epoch
            runtime._strategy_parameter_block_reason = None
            runtime.strategy_config.execution_mode = "observe"

            with patch.object(
                runtime,
                "recent_directional_rate_range",
                return_value=Decimal("0"),
            ):
                self.assertIsNone(
                    await runtime._auto_var_signal_for_current_open(None)
                )
            self.assertIsNotNone(runtime._selected_open_candidate)
            self.assertIn("observe candidate", runtime.last_auto_var_order_status)

        asyncio.run(run_case())

    def test_low_edge_candidate_reaches_existing_firm_quote_guard(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            low_edge_rate = candidate.threshold + Decimal("0.00005")
            low_edge = replace(
                candidate,
                reference_rate=low_edge_rate,
                actual_rate=low_edge_rate,
                actual_open_pnl_usd=(
                    low_edge_rate * candidate.order_notional_usd
                ),
            )
            runtime.strategy_config.execution_mode = "live"
            with patch.object(
                runtime,
                "evaluate_adaptive_open",
                return_value=main_module.StrategyDecision(
                    main_module.StrategyAction.OPEN,
                    "candidate",
                    open_candidate=low_edge,
                ),
            ), patch.object(
                runtime,
                "recent_directional_rate_range",
                return_value=Decimal("0"),
            ):
                signal = await runtime._auto_var_signal_for_current_open(None)

            self.assertEqual(signal, ("BUY", low_edge.actual_open_pnl_usd))
            self.assertIs(runtime._selected_open_candidate, low_edge)

        asyncio.run(run_case())

    def test_real_open_evaluation_does_not_add_a_second_signal_margin(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            low_edge_rate = candidate.threshold + Decimal("0.00005")
            assert runtime.last_market_frame is not None
            runtime.last_market_frame = replace(
                runtime.last_market_frame,
                reference_rates=main_module.DirectionalRates(
                    buy=low_edge_rate,
                    sell=runtime.last_market_frame.reference_rates.sell,
                ),
                actual_rates=main_module.DirectionalRates(
                    buy=low_edge_rate,
                    sell=runtime.last_market_frame.actual_rates.sell,
                ),
            )
            runtime.active_parameter_epoch = candidate.epoch
            runtime._strategy_parameter_block_reason = None
            runtime.strategy_config.execution_mode = "live"

            with patch.object(
                runtime,
                "recent_directional_rate_range",
                return_value=Decimal("0"),
            ):
                signal = await runtime._auto_var_signal_for_current_open(None)
            self.assertIsNotNone(signal)
            self.assertIsNotNone(runtime._selected_open_candidate)
            self.assertIsNotNone(runtime.last_strategy_decision.open_candidate)

        asyncio.run(run_case())

    def test_five_second_directional_range_requires_coverage_and_uses_extremes(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        now_ms = 10_000
        runtime._opportunity_samples[main_module.StrategySide.BUY].extend(
            main_module.OpportunitySample(timestamp_ms, rate)
            for timestamp_ms, rate in (
                (5_000, Decimal("0.0010")),
                (6_000, Decimal("0.0012")),
                (7_000, Decimal("0.0011")),
                (8_000, Decimal("0.0013")),
                (9_000, Decimal("0.0011")),
                (10_000, Decimal("0.0012")),
            )
        )
        self.assertEqual(
            runtime.recent_directional_rate_range(
                main_module.StrategySide.BUY,
                now_ms=now_ms,
            ),
            Decimal("0.0003"),
        )
        runtime._opportunity_samples[main_module.StrategySide.SELL].extend(
            (
                main_module.OpportunitySample(9_000, Decimal("-0.0010")),
                main_module.OpportunitySample(10_000, Decimal("-0.0011")),
            )
        )
        self.assertIsNone(
            runtime.recent_directional_rate_range(
                main_module.StrategySide.SELL,
                now_ms=now_ms,
            )
        )

    def test_zero_wear_close_stability_accepts_continuous_or_recent_sum(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

        def observe(
            trade_key: str,
            now_ms: int,
            above: bool,
            quote_number: int,
            received_ms: int,
        ) -> tuple[bool, int, int]:
            return runtime._update_close_zero_wear_stability(
                trade_key=trade_key,
                above_zero_wear=above,
                now_ms=now_ms,
                quote_key=(quote_number, received_ms),
                quote_received_ms=received_ms,
            )

        passed, continuous, accumulated = observe(
            "continuous", 10_000, True, 1, 10_000
        )
        self.assertFalse(passed)
        self.assertEqual((continuous, accumulated), (0, 0))
        for quote_number, now_ms in enumerate(
            (10_500, 11_000, 11_500, 12_000),
            start=2,
        ):
            passed, continuous, accumulated = observe(
                "continuous",
                now_ms,
                True,
                quote_number,
                now_ms,
            )
        self.assertTrue(passed)
        self.assertEqual((continuous, accumulated), (2_000, 2_000))

        for now_ms, above, quote_number, received_ms in (
            (20_000, True, 1, 20_000),
            (20_500, True, 1, 20_000),
            (20_600, False, 1, 20_000),
            (20_700, True, 2, 20_700),
            (21_200, True, 2, 20_700),
            (21_300, False, 2, 20_700),
            (21_400, True, 3, 21_400),
            (21_900, True, 3, 21_400),
            (21_950, False, 3, 21_400),
            (22_000, True, 4, 22_000),
            (22_500, True, 4, 22_000),
        ):
            passed, continuous, accumulated = observe(
                "summed",
                now_ms,
                above,
                quote_number,
                received_ms,
            )
        self.assertTrue(passed)
        self.assertEqual(continuous, 500)
        self.assertEqual(accumulated, 2_000)

        passed, _continuous, accumulated = observe(
            "summed", 22_600, False, 4, 22_000
        )
        self.assertFalse(passed)
        self.assertEqual(accumulated, 2_000)

        passed, continuous, accumulated = observe(
            "summed", 32_601, True, 5, 32_601
        )
        self.assertFalse(passed)
        self.assertEqual((continuous, accumulated), (0, 0))

    def test_zero_wear_close_does_not_reuse_one_stale_var_quote(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

        for now_ms in (10_000, 10_300, 10_600, 11_000, 12_500):
            passed, continuous, accumulated = (
                runtime._update_close_zero_wear_stability(
                    trade_key="one-quote",
                    above_zero_wear=True,
                    now_ms=now_ms,
                    quote_key=(1, 10_000),
                    quote_received_ms=10_000,
                )
            )

        self.assertFalse(passed)
        self.assertEqual(continuous, 0)
        self.assertLessEqual(accumulated, 600)

    def test_early_close_cannot_bypass_zero_wear_stability(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            frozen = make_open_candidate(runtime)
            open_record = make_record("stable-gate-open", "buy", "2", "100", "100.2")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen)
            open_record.var_fill_ts_iso = (
                datetime.now(timezone.utc) - timedelta(seconds=120)
            ).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            runtime.base_amount_multiplier = 1_000_000
            assert runtime.last_market_frame is not None

            close_candidate = main_module.CloseCandidate(
                close_direction=main_module.StrategySide.SELL,
                frame_captured_at_ms=time.time_ns() // 1_000_000,
                frozen_epoch_id=frozen.epoch.epoch_id,
                held_seconds=120,
                actual_close_rate=Decimal("-0.0005"),
                regression_target_rate=Decimal("-1"),
                expected_close_pnl_usd=Decimal("-0.1"),
                close_reserve_usd=Decimal("0.01"),
                round_lower_bound_usd=Decimal("0.29"),
                required_floor_usd=Decimal("0"),
                regression_passed=True,
                max_hold_alert=False,
            )

            async def current_frame(**_kwargs):
                return runtime.last_market_frame, {}

            runtime.current_adaptive_market_frame = current_frame
            with patch.object(
                runtime,
                "automation_can_submit_var_order",
                return_value=True,
            ), patch.object(
                runtime.strategy_engine,
                "evaluate_close",
                return_value=main_module.StrategyDecision(
                    main_module.StrategyAction.CLOSE,
                    "close_floor_passed",
                    close_candidate=close_candidate,
                ),
            ):
                signal = await runtime._auto_var_close_signal_for_current_open(
                    open_record
                )

            self.assertIsNone(signal)
            self.assertEqual(
                runtime.last_strategy_decision.reason,
                "close_zero_wear_stability_pending",
            )
            assert runtime.last_strategy_decision.close_candidate is not None
            self.assertFalse(
                runtime.last_strategy_decision.close_candidate
                .zero_wear_stability_passed
            )

        asyncio.run(run_case())

    def test_live_requires_fresh_flat_account_and_continues_without_token(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        candidate = make_open_candidate(runtime)
        runtime.active_parameter_epoch = replace(
            candidate.epoch,
            window_source="live",
        )
        runtime.strategy_window_stats = {
            side: {
                minutes: replace(window, source="live")
                for minutes, window in windows.items()
            }
            for side, windows in runtime.strategy_model.calibration_stats.items()
        }
        runtime.strategy_config.execution_mode = "live"
        runtime._strategy_started_at_ms = time.time_ns() // 1_000_000 - 3_600_001

        self.assertEqual(
            runtime.live_open_block_reason(),
            "dedicated Lighter order-entry WebSocket is not ready",
        )
        runtime.lighter_order_entry = type(
            "ReadyOrderEntry",
            (),
            {"is_ready": True},
        )()
        self.assertEqual(
            runtime.live_open_block_reason(),
            "fresh account reconciliation is required",
        )
        runtime.last_account_snapshot = AccountSnapshot(
            var_position=Decimal("0"),
            lighter_position=Decimal("0"),
            lighter_active_orders=0,
            captured_at=utc_now(),
        )
        self.assertIsNone(runtime.live_open_block_reason())
        self.assertEqual(runtime._canary_session_state, CANARY_SESSION_ARMED)

        runtime._canary_session_state = CANARY_SESSION_HALTED
        runtime._canary_cumulative_loss_usd = Decimal("12.34")
        runtime._canary_consecutive_losses = 7
        self.assertIsNone(runtime.live_open_block_reason())
        self.assertEqual(runtime._canary_session_state, CANARY_SESSION_ARMED)

    def test_live_never_opens_when_lighter_auto_hedge_is_disabled(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=False, lang="zh"))
        runtime.strategy_config.execution_mode = "live"
        self.assertEqual(
            runtime.live_open_block_reason(),
            "automatic Lighter hedge is disabled",
        )

    def test_live_open_commit_rechecks_low_latency_order_entry(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.strategy_config.execution_mode = "live"
            runtime._canary_session_state = CANARY_SESSION_ARMED
            intent = runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            intent.state = main_module.VAR_INTENT_PREPARED
            intent.trace_id = "low-latency-precommit"
            runtime.lighter_order_entry = type(
                "NotReadyOrderEntry",
                (),
                {"is_ready": False},
            )()

            error = await runtime.pending_var_commit_precondition_error(
                intent=intent,
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="low-latency-precommit",
                expected_open_trade_key=None,
                base_qty=None,
                require_live_ready=True,
            )
            self.assertIn("order-entry WebSocket disconnected", error or "")
            self.assertEqual(intent.state, main_module.VAR_INTENT_PREPARED)

            runtime.lighter_order_entry = type(
                "ReadyOrderEntry",
                (),
                {"is_ready": True},
            )()
            self.assertIsNone(
                await runtime.pending_var_commit_precondition_error(
                    intent=intent,
                    phase="open",
                    side="BUY",
                    amount=Decimal("200"),
                    trace_id="low-latency-precommit",
                    expected_open_trade_key=None,
                    base_qty=None,
                    require_live_ready=True,
                )
            )
            self.assertEqual(intent.state, main_module.VAR_INTENT_COMMITTING)

        asyncio.run(run_case())

    def test_sixty_second_frame_gap_discards_active_epoch(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            fresh_frame = runtime.last_market_frame
            assert fresh_frame is not None
            runtime.active_parameter_epoch = candidate.epoch
            runtime.strategy_epoch_activator.active = candidate.epoch
            runtime._strategy_parameter_block_reason = None
            runtime._last_valid_strategy_frame_ms = (
                fresh_frame.captured_at_ms - 60_000
            )

            async def current_frame():
                return fresh_frame, {"valid": True, "rejection_reason": None}

            runtime.current_adaptive_market_frame = current_frame
            self.assertTrue(await runtime.refresh_adaptive_market_frame_for_decision())
            self.assertIsNone(runtime.active_parameter_epoch)
            self.assertIsNone(runtime.strategy_epoch_activator.active)
            self.assertEqual(
                runtime._strategy_parameter_block_reason,
                "strategy_market_frame_gap",
            )

        asyncio.run(run_case())

    def test_restart_history_loader_accepts_one_hour_and_rejects_stale_tail(self) -> None:
        now_ms = 10_000_000
        rows = []
        for second in range(3_602):
            timestamp_ms = now_ms - 3_601_000 + second * 1_000
            rows.append(
                json.dumps(
                    {
                        "version": main_module.STRATEGY_MARKET_SAMPLE_VERSION,
                        "valid": True,
                        "asset": "BTC",
                        "reference_notional_usd": "500",
                        "order_notional_usd": "200",
                        "reference_buy_rate": "0.001",
                        "reference_sell_rate": "-0.001",
                        "sample_timestamp_ms": timestamp_ms,
                    }
                )
            )
        with tempfile.TemporaryDirectory(prefix="adaptive-history-") as tmp:
            history = Path(tmp) / "strategy_market_samples.jsonl"
            history.write_text("\n".join(rows) + "\n", encoding="utf-8")
            state, samples, gap_ms = VariationalToLighterRuntime._read_strategy_sample_history(
                history,
                asset="BTC",
                reference_notional_usd=Decimal("500"),
                order_notional_usd=Decimal("200"),
                now_ms=now_ms,
            )
            self.assertEqual(state, "pending_first_live_frame")
            self.assertGreaterEqual(samples[-1][0] - samples[0][0], 3_600_000)
            self.assertEqual(gap_ms, 0)

            stale_state, stale_samples, _ = (
                VariationalToLighterRuntime._read_strategy_sample_history(
                    history,
                    asset="BTC",
                    reference_notional_usd=Decimal("500"),
                    order_notional_usd=Decimal("200"),
                    now_ms=now_ms + main_module.STRATEGY_HISTORY_RESUME_MAX_GAP_MS + 1,
                )
            )
            self.assertEqual(stale_state, "history_stale_over_5m")
            self.assertEqual(stale_samples, [])

    def test_restart_history_loader_resumes_partial_current_session(self) -> None:
        now_ms = 15_000_000
        session_start_ms = now_ms - 40 * 60 * 1_000
        older_start_ms = session_start_ms - 20 * 60 * 1_000
        rows = []
        for second in range(60 * 60 + 1):
            timestamp_ms = older_start_ms + second * 1_000
            rows.append(
                json.dumps(
                    {
                        "version": main_module.STRATEGY_MARKET_SAMPLE_VERSION,
                        "valid": True,
                        "asset": "BTC",
                        "reference_notional_usd": "500",
                        "order_notional_usd": "200",
                        "reference_buy_rate": (
                            "9" if timestamp_ms < session_start_ms else "0.001"
                        ),
                        "reference_sell_rate": (
                            "-9" if timestamp_ms < session_start_ms else "-0.001"
                        ),
                        "sample_timestamp_ms": timestamp_ms,
                    }
                )
            )
        with tempfile.TemporaryDirectory(prefix="adaptive-partial-history-") as tmp:
            history = Path(tmp) / "strategy_market_samples.jsonl"
            history.write_text("\n".join(rows) + "\n", encoding="utf-8")

            state, samples, gap_ms = VariationalToLighterRuntime._read_strategy_sample_history(
                history,
                asset="BTC",
                reference_notional_usd=Decimal("500"),
                order_notional_usd=Decimal("200"),
                now_ms=now_ms,
                minimum_timestamp_ms=session_start_ms,
            )

            self.assertEqual(state, "pending_partial_history")
            self.assertEqual(gap_ms, 0)
            self.assertEqual(samples[0][0], session_start_ms)
            self.assertEqual(samples[-1][0], now_ms)
            self.assertEqual(samples[-1][0] - samples[0][0], 40 * 60 * 1_000)
            self.assertTrue(all(rates.buy == Decimal("0.001") for _, rates in samples))

    def test_runtime_reads_current_sample_session_cutoff(self) -> None:
        with tempfile.TemporaryDirectory(prefix="adaptive-session-cutoff-") as tmp:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.strategy_market_samples_file = (
                Path(tmp) / main_module.STRATEGY_MARKET_SAMPLES_FILE_NAME
            )
            marker = Path(tmp) / main_module.STRATEGY_SAMPLE_SESSION_FILE_NAME
            marker.write_text(
                json.dumps(
                    {
                        "asset": "BTC",
                        "minimum_sample_timestamp_ms": 12_345,
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                runtime._strategy_sample_session_cutoff_ms("BTC"),
                12_345,
            )
            self.assertIsNone(runtime._strategy_sample_session_cutoff_ms("ETH"))

    def test_restart_history_loader_returns_only_latest_rolling_hour(self) -> None:
        now_ms = 20_000_000
        rows = []
        for second in range(2 * 60 * 60 + 1):
            timestamp_ms = now_ms - 2 * 60 * 60 * 1_000 + second * 1_000
            old = timestamp_ms < now_ms - main_module.STRATEGY_STATISTICS_WINDOW_MS
            rows.append(
                json.dumps(
                    {
                        "version": main_module.STRATEGY_MARKET_SAMPLE_VERSION,
                        "valid": True,
                        "asset": "BTC",
                        "reference_notional_usd": "500",
                        "order_notional_usd": "200",
                        "reference_buy_rate": "9" if old else "0.001",
                        "reference_sell_rate": "-9" if old else "-0.001",
                        "sample_timestamp_ms": timestamp_ms,
                    }
                )
            )
        with tempfile.TemporaryDirectory(prefix="adaptive-rolling-history-") as tmp:
            history = Path(tmp) / "strategy_market_samples.jsonl"
            history.write_text("\n".join(rows) + "\n", encoding="utf-8")

            state, samples, gap_ms = VariationalToLighterRuntime._read_strategy_sample_history(
                history,
                asset="BTC",
                reference_notional_usd=Decimal("500"),
                order_notional_usd=Decimal("200"),
                now_ms=now_ms,
            )

            self.assertEqual(state, "pending_first_live_frame")
            self.assertEqual(gap_ms, 0)
            self.assertEqual(samples[0][0], now_ms - main_module.STRATEGY_STATISTICS_WINDOW_MS)
            self.assertEqual(samples[-1][0], now_ms)
            self.assertEqual(len(samples), 3_601)
            self.assertTrue(all(rates.buy == Decimal("0.001") for _, rates in samples))

    def test_firm_amount_outside_target_tolerance_rejects_without_halting(self) -> None:
        async def run_case() -> None:
            class Broker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return {
                        "type": "ORDER_RESULT",
                        "requestId": "quote",
                        "ok": True,
                        "detail": {
                            "quote": {
                                "quoteId": "over-cap",
                                "firmPrice": "100",
                                "firmQty": "2.02",
                            }
                        },
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.strategy_config.execution_mode = "live"
            runtime.runtime.command_broker = Broker()
            candidate = make_open_candidate(runtime)

            async def fresh_open_vwaps(**_kwargs):
                return Decimal("100.2"), Decimal("100.2"), 0

            runtime.get_fresh_lighter_open_vwaps = fresh_open_vwaps
            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=candidate,
            )

            self.assertFalse(result["ok"])
            self.assertIn("target_amount", result["error"])
            self.assertNotEqual(runtime._canary_session_state, CANARY_SESSION_HALTED)
            self.assertEqual(len(runtime.runtime.command_broker.calls), 1)

        asyncio.run(run_case())

    def test_completed_losing_round_records_metrics_and_remains_armed(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.strategy_config.execution_mode = "live"
            open_record = make_record("open", "buy", "2", "100", "99")
            close_record = make_record("close", "sell", "2", "100", "101")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            close_record.hedge_status = "submitted"
            async with runtime._record_lock:
                runtime.records = {
                    open_record.trade_key: open_record,
                    close_record.trade_key: close_record,
                }
                runtime.record_order.extend(
                    [open_record.trade_key, close_record.trade_key]
                )

            self.assertFalse(await runtime.record_completed_canary_round(close_record))
            self.assertEqual(runtime._last_round_closed_at, 0)
            close_record.hedge_status = "filled"
            close_record.lighter_filled_qty = Decimal("2")
            self.assertTrue(await runtime.record_completed_canary_round(close_record))
            self.assertGreater(runtime._last_round_closed_at, 0)
            self.assertEqual(runtime._canary_session_state, CANARY_SESSION_ARMED)
            self.assertGreater(runtime._canary_cumulative_loss_usd, Decimal("0"))
            self.assertEqual(runtime._canary_round_count, 1)
            self.assertIn("continuous execution", runtime.last_auto_var_order_status)

        asyncio.run(run_case())

    def test_firm_reference_depth_must_still_cover_opening_gate(self) -> None:
        async def run_case() -> None:
            class Broker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return {
                        "type": "ORDER_RESULT",
                        "requestId": "quote",
                        "ok": True,
                        "detail": {
                            "quote": {
                                "quoteId": "reference-regressed",
                                "firmPrice": "100",
                                "firmQty": "2",
                            }
                        },
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.runtime.command_broker = Broker()
            candidate = make_open_candidate(runtime)

            async def fresh_open_vwaps(**_kwargs):
                # Target-size depth still passes, but 500U reference depth has
                # already fallen below the frozen threshold.
                return Decimal("100.2"), Decimal("100.005"), 0

            runtime.get_fresh_lighter_open_vwaps = fresh_open_vwaps
            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=candidate,
            )

            self.assertFalse(result["ok"])
            self.assertIn("firm_reference_rate_below", result["error"])
            self.assertEqual(len(runtime.runtime.command_broker.calls), 1)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_firm_reference_depth_uses_frozen_threshold_without_extra_headroom(self) -> None:
        async def run_case() -> None:
            class Broker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return {
                        "type": "ORDER_RESULT",
                        "requestId": "quote",
                        "ok": True,
                        "detail": {
                            "quote": {
                                "quoteId": "reference-headroom",
                                "firmPrice": "100",
                                "firmQty": "2",
                            }
                        },
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.runtime.command_broker = Broker()
            candidate = make_open_candidate(runtime)

            async def fresh_open_vwaps(**_kwargs):
                actual = Decimal("100") * (
                    Decimal("1") + candidate.threshold + Decimal("0.001")
                )
                reference = Decimal("100") * (
                    Decimal("1") + candidate.threshold + Decimal("0.00005")
                )
                return actual, reference, 0

            runtime.get_fresh_lighter_open_vwaps = fresh_open_vwaps
            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=candidate,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(len(runtime.runtime.command_broker.calls), 2)
            self.assertIsNotNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_transient_missing_frame_does_not_halt_existing_position_close(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            frozen = make_open_candidate(runtime)
            open_record = make_record("adaptive-open", "buy", "2", "100", "100.2")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen)
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            runtime.base_amount_multiplier = 1
            runtime.last_market_frame = None

            async def missing_frame(**_kwargs):
                return None, {"valid": False, "rejection_reason": "test"}

            runtime.current_adaptive_market_frame = missing_frame

            signal = await runtime._auto_var_close_signal_for_current_open(open_record)

            self.assertIsNone(signal)
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(
                runtime.last_auto_var_close_status,
                "PAUSE: exact_close_market_frame_unavailable",
            )

        asyncio.run(run_case())

    def test_close_precheck_uses_exact_held_qty_not_synthetic_200u_qty(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            frozen = make_open_candidate(runtime)
            open_record = make_record("adaptive-exact-close", "buy", "2", "100", "100")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen)
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")

            runtime.variational_ticker = "BTC"
            runtime.ticker = "BTC"
            runtime.market_generation = 1
            runtime.base_amount_multiplier = 1_000_000
            runtime.price_multiplier = 100
            now = datetime.now(timezone.utc)
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.quotes["BTC"] = {
                    "asset": "BTC",
                    "bid": Decimal("150"),
                    "ask": Decimal("150.01"),
                    "received_monotonic": time.monotonic(),
                    "captured_at": now.isoformat(),
                }
            async with runtime.lighter_order_book_lock:
                # A synthetic current-200U close only consumes the cheap 1.5
                # BTC tier.  The actual frozen 2 BTC position must consume the
                # next tier and therefore fails the zero-loss early floor.
                runtime.lighter_order_book = {
                    "bids": {Decimal("149.90"): Decimal("10")},
                    "asks": {
                        Decimal("149.98"): Decimal("1.5"),
                        Decimal("150.20"): Decimal("10"),
                    },
                }
                runtime.lighter_order_book_ticks = {"bids": {}, "asks": {}}
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_sequence_gap = False
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            signal = await runtime._auto_var_close_signal_for_current_open(open_record)

            self.assertIsNone(signal)
            candidate = runtime.last_strategy_decision.close_candidate
            self.assertIsNotNone(candidate)
            self.assertEqual(candidate.expected_close_pnl_usd, Decimal("-0.07"))
            self.assertLess(candidate.round_lower_bound_usd, Decimal("0"))
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_restore_derived_halt_cannot_be_downgraded_by_saved_canary_state(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.strategy_config.execution_mode = "live"
            payload = self._empty_runtime_payload(runtime)
            payload["automation_paused"] = True
            payload["automation_pause_reason"] = "Recovered safety pause"
            payload["canary_session"]["state"] = "ARMED"
            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))
            self.assertEqual(runtime._canary_session_state, CANARY_SESSION_HALTED)

        asyncio.run(run_case())

    def test_restore_legacy_loss_halt_keeps_metrics_but_resumes_live(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.strategy_config.execution_mode = "live"
            payload = self._empty_runtime_payload(runtime)
            payload["canary_session"].update(
                {
                    "round_count": 3,
                    "cumulative_loss_usd": "1.25",
                    "consecutive_losses": 2,
                    "state": "HALTED",
                }
            )
            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))

            self.assertFalse(runtime.automation_paused)
            self.assertEqual(runtime._canary_session_state, main_module.CANARY_SESSION_OBSERVING)
            self.assertEqual(runtime._canary_round_count, 3)
            self.assertEqual(runtime._canary_cumulative_loss_usd, Decimal("1.25"))
            self.assertEqual(runtime._canary_consecutive_losses, 2)

        asyncio.run(run_case())

    def test_v4_upgrade_clears_only_flat_empty_v3_halt(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            old_model = load_model_config(
                Path(main_module.__file__).resolve().parent
                / "adaptive_strategy"
                / "models"
                / "adaptive-median-v3.json"
            )
            payload["strategy_model"] = old_model.model_version
            payload["strategy_model_hash"] = old_model.model_hash
            payload["canary_session"]["state"] = "HALTED"
            with tempfile.TemporaryDirectory(prefix="adaptive-v4-migration-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertFalse(await runtime.load_runtime_state("BTC"))

            self.assertEqual(
                runtime._canary_session_state,
                main_module.CANARY_SESSION_OBSERVING,
            )
            self.assertFalse(runtime.automation_paused)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_v4_upgrade_clears_flat_v3_loss_halt_but_not_loss_metrics(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            old_model = load_model_config(
                Path(main_module.__file__).resolve().parent
                / "adaptive_strategy"
                / "models"
                / "adaptive-median-v3.json"
            )
            payload["strategy_model"] = old_model.model_version
            payload["strategy_model_hash"] = old_model.model_hash
            payload["canary_session"].update(
                {"state": "HALTED", "cumulative_loss_usd": "0.01"}
            )
            with tempfile.TemporaryDirectory(prefix="adaptive-v4-loss-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertFalse(await runtime.load_runtime_state("BTC"))

            self.assertFalse(runtime.automation_paused)
            self.assertEqual(runtime._canary_session_state, main_module.CANARY_SESSION_OBSERVING)

        asyncio.run(run_case())

    def test_v5_upgrade_restores_only_manual_exposure_and_reconciliation_pause(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            old_model = load_model_config(
                Path(main_module.__file__).resolve().parent
                / "adaptive_strategy"
                / "models"
                / "adaptive-median-v4.json"
            )
            payload["strategy_model"] = old_model.model_version
            payload["strategy_model_hash"] = old_model.model_hash
            record = main_module.OrderLifecycle(
                trade_key="manual-open",
                trade_id="manual-open",
                side="sell",
                qty=Decimal("0.003091"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                var_fill_price=Decimal("63560.96"),
                var_fill_ts_iso=datetime.now(timezone.utc).isoformat(),
                var_fill_source="event",
                strategy_phase="open",
                strategy_tag=main_module.MANUAL_STRATEGY_TAG,
                open_notional_usd=Decimal("196.46692736"),
                lighter_side="BUY",
                lighter_client_order_id=42,
                lighter_client_order_ids=[42],
                lighter_fill_price=Decimal("63604.9"),
                lighter_filled_qty=Decimal("0.00309"),
                lighter_filled_quote=Decimal("196.539141"),
                lighter_fill_ts_iso=datetime.now(timezone.utc).isoformat(),
                lighter_outcome_final=True,
                hedge_status="filled",
                execution_state="HEDGED",
            )
            payload["records"] = [record.to_payload()]
            payload["lighter_order_cumulative"] = [
                {
                    "client_order_id": 42,
                    "filled_base": "0.00309",
                    "filled_quote": "196.539141",
                    "terminal": True,
                }
            ]
            pause_reason = (
                "Account reconciliation failed: FRESH_MISMATCH: "
                "Var 0/-0.003091, Lighter 0/0.00309, active=0"
            )
            payload["automation_paused"] = True
            payload["automation_pause_reason"] = pause_reason
            with tempfile.TemporaryDirectory(prefix="adaptive-v5-manual-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))

            self.assertIn("manual-open", runtime.records)
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime._reconcile_pause_reason, pause_reason)

        asyncio.run(run_case())

    def test_flat_v4_recalibration_preserves_completed_round_metrics(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            payload["strategy_model_hash"] = next(
                iter(main_module.MIGRATABLE_FLAT_V4_MODEL_HASHES)
            )
            payload["last_round_closed_at"] = 123.5
            payload["canary_session"].update(
                {
                    "round_count": 7,
                    "cumulative_loss_usd": "0.4429522",
                    "consecutive_losses": 7,
                    "state": "ARMED",
                }
            )
            with tempfile.TemporaryDirectory(prefix="adaptive-v4-recalibration-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))

            self.assertEqual(runtime._canary_round_count, 7)
            self.assertEqual(
                runtime._canary_cumulative_loss_usd,
                Decimal("0.4429522"),
            )
            self.assertEqual(runtime._canary_consecutive_losses, 7)
            self.assertEqual(runtime._last_round_closed_at, 123.5)
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_skipped_close_hedge_pause_is_reconcilable_after_restart(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            reason = (
                "Skipped Lighter close hedge: matching open Lighter hedge was not filled."
            )
            payload["automation_paused"] = True
            payload["automation_pause_reason"] = reason
            with tempfile.TemporaryDirectory(prefix="skipped-close-hedge-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))

            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime._reconcile_pause_reason, reason)

        asyncio.run(run_case())

    def test_protective_close_hedge_pause_is_reconcilable_after_restart(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            reason = (
                "Protective Lighter close hedge: matching open Lighter hedge "
                "filled only 0.00100; closing that exact residual."
            )
            payload["automation_paused"] = True
            payload["automation_pause_reason"] = reason
            with tempfile.TemporaryDirectory(prefix="protective-close-hedge-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    self.assertTrue(await runtime.load_runtime_state("BTC"))

            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime._reconcile_pause_reason, reason)

        asyncio.run(run_case())

    def test_unreadable_or_malformed_runtime_state_fails_closed_without_rewrite(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                original = "{not-json"
                state_file.write_text(original, encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    with self.assertRaises(RuntimeError):
                        await runtime.load_runtime_state("BTC")
                self.assertEqual(state_file.read_text(encoding="utf-8"), original)

                payload = self._empty_runtime_payload(runtime)
                payload["records"] = ["invalid-row"]
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    with self.assertRaises(RuntimeError):
                        await runtime.load_runtime_state("BTC")

        asyncio.run(run_case())

    def test_runtime_state_preserves_each_lighter_attempt_total(self) -> None:
        async def run_case() -> None:
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.ticker = "BTC"
            record = make_record("multi-ioc", "buy", "2", "100", "100")
            record.lighter_side = "SELL"
            record.lighter_client_order_id = 102
            record.lighter_client_order_ids = [101, 102]
            record.lighter_filled_qty = Decimal("1.5")
            record.lighter_filled_quote = Decimal("150")
            record.lighter_fill_price = Decimal("100")
            record.hedge_status = "partial"
            record.lighter_outcome_final = False
            async with source._record_lock:
                source.records[record.trade_key] = record
                source.record_order.append(record.trade_key)
                source.lighter_client_order_to_trade_key.update(
                    {101: record.trade_key, 102: record.trade_key}
                )
                source.lighter_order_fill_totals.update(
                    {
                        101: (Decimal("1"), Decimal("100")),
                        102: (Decimal("0.5"), Decimal("50")),
                    }
                )
                source.lighter_order_terminal_ids.add(101)

            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    await source.persist_runtime_state()
                    restored = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restored.variational_ticker = "BTC"
                    self.assertTrue(await restored.load_runtime_state("BTC"))

            self.assertEqual(
                restored.lighter_order_fill_totals,
                {
                    101: (Decimal("1"), Decimal("100")),
                    102: (Decimal("0.5"), Decimal("50")),
                },
            )
            self.assertEqual(restored.lighter_order_terminal_ids, {101})
            self.assertEqual(
                restored.lighter_client_order_to_trade_key,
                {101: record.trade_key, 102: record.trade_key},
            )

        asyncio.run(run_case())

    def test_ambiguous_old_multi_ioc_partial_state_is_rejected(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            record = make_record("ambiguous-ioc", "buy", "2", "100", "100")
            record.lighter_client_order_id = 102
            record.lighter_client_order_ids = [101, 102]
            record.lighter_filled_qty = Decimal("1.5")
            record.lighter_filled_quote = Decimal("150")
            record.lighter_fill_price = Decimal("100")
            record.hedge_status = "partial"
            payload = self._empty_runtime_payload(runtime)
            payload["records"] = [record.to_payload()]

            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "cannot safely reconstruct multi-order",
                    ):
                        await runtime.load_runtime_state("BTC")

        asyncio.run(run_case())

    def test_restored_committing_intent_becomes_recoverable_ambiguous(self) -> None:
        async def run_case() -> None:
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.ticker = "BTC"
            frozen = make_open_candidate(source)
            source.mark_var_intent_sent("open", "BUY", Decimal("200"))
            intent = await source.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="restore-committing",
                firm_quote={
                    "quoteId": "firm-restore-committing",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "adaptiveStrategy": open_candidate_to_payload(frozen),
                    "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                },
            )
            assert intent is not None
            self.assertTrue(
                await source.mark_pending_var_intent_committing(
                    phase="open",
                    side="BUY",
                    trace_id="restore-committing",
                    expected_intent=intent,
                )
            )

            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    await source.persist_runtime_state()
                    restored = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restored.variational_ticker = "BTC"
                    restored.ticker = "BTC"
                    restored.accepted_assets = {"BTC"}
                    self.assertTrue(await restored.load_runtime_state("BTC"))
                    assert restored.pending_var_intent is not None
                    self.assertEqual(
                        restored.pending_var_intent.state,
                        main_module.VAR_INTENT_COMMIT_AMBIGUOUS,
                    )
                    restored.pending_var_intent.sent_monotonic -= 2
                    async with restored.runtime.monitor._lock:
                        restored.runtime.monitor.positions["BTC"] = {
                            "qty": Decimal("2"),
                            "avg_entry_price": Decimal("100"),
                            "updated_at": utc_now(),
                        }
                        restored.runtime.monitor._portfolio_received_monotonic = (
                            time.monotonic()
                        )
                    self.assertTrue(
                        await restored.recover_pending_var_intent_from_portfolio()
                    )

            self.assertIsNone(restored.pending_var_intent)
            record = next(iter(restored.records.values()))
            self.assertEqual(record.var_fill_source, "portfolio")
            self.assertEqual(record.hedge_status, "recovery_check")
            self.assertEqual(
                restored.lighter_client_order_to_trade_key[
                    record.lighter_reserved_client_order_id
                ],
                record.trade_key,
            )

        asyncio.run(run_case())

    def test_real_199_7438201u_firm_intent_survives_persist_and_restart(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="adaptive-firm-tolerance-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    source = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    source.variational_ticker = "BTC"
                    source.ticker = "BTC"
                    frozen = make_open_candidate(source)
                    source.mark_var_intent_sent("open", "BUY", Decimal("200"))
                    prepared = await source.prepare_pending_var_intent(
                        phase="open",
                        side="BUY",
                        amount=Decimal("200"),
                        trace_id="real-firm-tolerance",
                        firm_quote={
                            "quoteId": "real-firm-199-7438201",
                            "firmPrice": "64621.1",
                            "firmQty": "0.003091",
                            "adaptiveStrategy": open_candidate_to_payload(frozen),
                            "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                        },
                    )
                    assert prepared is not None
                    self.assertEqual(
                        prepared.firm_price * prepared.firm_qty,
                        Decimal("199.7438201"),
                    )

                    restored = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restored.variational_ticker = "BTC"
                    restored.ticker = "BTC"
                    self.assertTrue(await restored.load_runtime_state("BTC"))
                    assert restored.pending_var_intent is not None
                    self.assertEqual(
                        restored.pending_var_intent.firm_price
                        * restored.pending_var_intent.firm_qty,
                        Decimal("199.7438201"),
                    )

        asyncio.run(run_case())

    def test_captured_500u_template_target_survives_persist_and_restart(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="adaptive-firm-500-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    source = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    source.variational_ticker = "BTC"
                    source.ticker = "BTC"
                    frozen = make_open_candidate(source)
                    source.mark_var_intent_sent("open", "BUY", Decimal("200"))
                    prepared = await source.prepare_pending_var_intent(
                        phase="open",
                        side="BUY",
                        amount=Decimal("200"),
                        trace_id="firm-target-500",
                        firm_quote={
                            "quoteId": "firm-target-500",
                            "firmPrice": "100",
                            "firmQty": "5.006",
                            "targetNotionalUsd": "500",
                            "adaptiveStrategy": open_candidate_to_payload(frozen),
                            "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                        },
                    )
                    assert prepared is not None
                    self.assertEqual(
                        prepared.firm_price * prepared.firm_qty,
                        Decimal("500.600"),
                    )
                    self.assertEqual(
                        prepared.firm_target_notional_usd,
                        Decimal("500"),
                    )

                    restored = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restored.variational_ticker = "BTC"
                    restored.ticker = "BTC"
                    self.assertTrue(await restored.load_runtime_state("BTC"))
                    assert restored.pending_var_intent is not None
                    self.assertEqual(
                        restored.pending_var_intent.firm_price
                        * restored.pending_var_intent.firm_qty,
                        Decimal("500.600"),
                    )
                    self.assertEqual(
                        restored.pending_var_intent.firm_target_notional_usd,
                        Decimal("500"),
                    )

        asyncio.run(run_case())

    def test_emergency_close_survives_commit_crash_and_second_restart(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    source = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    source.variational_ticker = "BTC"
                    source.ticker = "BTC"
                    frozen = make_open_candidate(source)
                    open_record = make_record(
                        "emergency-open",
                        "buy",
                        "2",
                        "100",
                        "100",
                    )
                    open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
                    open_record.strategy_phase = "open"
                    open_record.adaptive_strategy_context = (
                        open_candidate_to_payload(frozen)
                    )
                    open_record.var_fill_ts_iso = utc_now()
                    open_record.open_notional_usd = Decimal("200")
                    open_record.lighter_fill_price = None
                    open_record.hedge_status = "error"
                    async with source._record_lock:
                        source.records[open_record.trade_key] = open_record
                        source.record_order.append(open_record.trade_key)

                    expected = source.mark_var_intent_sent(
                        "emergency_close",
                        "SELL",
                        Decimal("200"),
                    )
                    context = {
                        "schema": "adaptive-emergency-close-context-v1",
                        "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                        "openTradeKey": open_record.trade_key,
                        "openQty": "2",
                        "requestedCloseNotionalUsd": "200",
                    }
                    prepared = await source.prepare_pending_var_intent(
                        phase="emergency_close",
                        side="SELL",
                        amount=Decimal("200"),
                        trace_id="emergency-commit-crash",
                        firm_quote={
                            "quoteId": "firm-emergency-crash",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "adaptiveStrategy": context,
                            "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                        },
                        expected_intent=expected,
                    )
                    assert prepared is not None
                    self.assertTrue(
                        await source.mark_pending_var_intent_committing(
                            phase="emergency_close",
                            side="SELL",
                            trace_id="emergency-commit-crash",
                            expected_intent=prepared,
                        )
                    )
                    await source.persist_runtime_state()

                    recovered = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    recovered.variational_ticker = "BTC"
                    recovered.ticker = "BTC"
                    recovered.accepted_assets = {"BTC"}
                    self.assertTrue(await recovered.load_runtime_state("BTC"))
                    assert recovered.pending_var_intent is not None
                    self.assertEqual(
                        recovered.pending_var_intent.state,
                        main_module.VAR_INTENT_COMMIT_AMBIGUOUS,
                    )
                    recovered.pending_var_intent.sent_monotonic -= 2
                    async with recovered.runtime.monitor._lock:
                        recovered.runtime.monitor.positions["BTC"] = {
                            "qty": Decimal("0"),
                            "avg_entry_price": None,
                            "updated_at": utc_now(),
                        }
                        recovered.runtime.monitor.quotes["BTC"] = {
                            "asset": "BTC",
                            "bid": Decimal("99"),
                            "ask": Decimal("101"),
                            "received_monotonic": time.monotonic(),
                            "captured_at": utc_now(),
                        }
                        recovered.runtime.monitor._portfolio_received_monotonic = (
                            time.monotonic()
                        )

                    self.assertTrue(
                        await recovered.recover_pending_var_intent_from_portfolio()
                    )
                    self.assertIsNone(recovered.pending_var_intent)
                    close_record = recovered.records[
                        next(
                            key
                            for key in recovered.record_order
                            if key != open_record.trade_key
                        )
                    ]
                    self.assertEqual(close_record.strategy_phase, "emergency_close")
                    self.assertEqual(close_record.adaptive_strategy_context, context)
                    self.assertTrue(close_record.lighter_reduce_only)
                    self.assertEqual(close_record.hedge_status, "skipped")
                    self.assertEqual(
                        close_record.adaptive_strategy_context["openTradeKey"],
                        open_record.trade_key,
                    )

                    # The post-recovery file is itself restartable.  This
                    # catches a close record that was recoverable once but
                    # lost its source-position relationship on the next boot.
                    restarted = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restarted.variational_ticker = "BTC"
                    self.assertTrue(await restarted.load_runtime_state("BTC"))
                    self.assertIsNone(restarted.pending_var_intent)
                    self.assertEqual(len(restarted.records), 2)
                    restarted_close = next(
                        record
                        for record in restarted.records.values()
                        if record.strategy_phase == "emergency_close"
                    )
                    self.assertEqual(
                        restarted_close.adaptive_strategy_context["openTradeKey"],
                        open_record.trade_key,
                    )

        asyncio.run(run_case())

    def test_trade_event_without_stable_identity_fails_closed(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "",
                    "event_seq": True,
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "timestamp": utc_now(),
                }
            )

            self.assertEqual(runtime.records, {})
            self.assertTrue(runtime.automation_paused)
            self.assertIn("stable trade_id/event_seq", runtime.automation_pause_reason)

        asyncio.run(run_case())

    def test_restored_open_hedge_checks_reserved_id_before_resuming(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            record = make_record("restore-open-hedge", "buy", "2", "100", "100")
            record.lighter_fill_price = None
            record.lighter_reserved_client_order_id = 123456
            record.hedge_status = "not_started"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client = object()
            scheduled: list[str] = []

            async def persist_noop() -> None:
                return None

            async def no_orders(_target_ids):
                return [], True

            def schedule(candidate) -> bool:
                candidate.hedge_status = "queued"
                scheduled.append(candidate.trade_key)
                return True

            runtime.persist_runtime_state = persist_noop
            runtime._fetch_lighter_orders_for_reconciliation = no_orders
            runtime.schedule_lighter_order = schedule

            without_probe = await runtime.prepare_restored_lighter_recovery()
            self.assertEqual(without_probe, [])
            self.assertEqual(record.hedge_status, "recovery_check")
            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(scheduled, [record.trade_key])
            self.assertEqual(record.hedge_status, "queued")

        asyncio.run(run_case())

    def test_restored_close_with_only_reserved_id_fails_closed(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            record = make_record("restore-close-hedge", "sell", "2", "100", "100")
            record.lighter_fill_price = None
            record.lighter_reduce_only = True
            record.lighter_reserved_client_order_id = 654321
            record.hedge_status = "not_started"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            async def persist_noop() -> None:
                return None

            runtime.persist_runtime_state = persist_noop
            without_probe = await runtime.prepare_restored_lighter_recovery()

            self.assertEqual(without_probe, [])
            self.assertEqual(record.hedge_status, "recovery_required")
            self.assertTrue(runtime.automation_paused)
            self.assertIn(
                "lacks a durable Lighter submission",
                runtime.automation_pause_reason,
            )

        asyncio.run(run_case())

    def test_restored_unconfirmed_var_commit_never_starts_new_hedge(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            record = make_record(
                "unconfirmed-restored-open",
                "buy",
                "2",
                "100",
                "100",
            )
            record.lighter_fill_price = None
            record.var_fill_source = "http_commit"
            record.last_variational_status = "accepted"
            record.lighter_reserved_client_order_id = 777777
            record.hedge_status = "not_started"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            intent = runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            intent.provisional_trade_key = record.trade_key
            intent.state = main_module.VAR_INTENT_COMMITTED

            without_probe = await runtime.prepare_restored_lighter_recovery()

            self.assertEqual(without_probe, [])
            self.assertEqual(record.hedge_status, "not_started")
            self.assertNotEqual(record.hedge_status, "recovery_check")

        asyncio.run(run_case())

    def test_emergency_flatten_never_reverses_an_already_closed_var_leg(self) -> None:
        async def run_case() -> None:
            class Broker:
                def __init__(self) -> None:
                    self.calls = 0

                async def extension_connected(self) -> bool:
                    return True

                async def request_place_order(self, **_kwargs):
                    self.calls += 1
                    raise AssertionError("no emergency order should be sent")

            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.runtime.command_broker = Broker()
            open_record = make_record("already-closed-open", "buy", "2", "100", "100")
            close_record = make_record("already-closed-close", "sell", "2", "101", "101")
            runtime.records = {
                open_record.trade_key: open_record,
                close_record.trade_key: close_record,
            }
            runtime.record_order.extend(
                [open_record.trade_key, close_record.trade_key]
            )

            await runtime.emergency_flatten_var(open_record)

            self.assertEqual(runtime.runtime.command_broker.calls, 0)
            self.assertIsNone(runtime.pending_var_intent)
            self.assertTrue(runtime.automation_paused)
            self.assertIn(
                "source position is no longer",
                runtime.automation_pause_reason,
            )

        asyncio.run(run_case())

    def test_corrupt_canary_loss_counter_is_never_reset_to_zero(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            payload = self._empty_runtime_payload(runtime)
            payload["canary_session"]["cumulative_loss_usd"] = "NaN"
            with tempfile.TemporaryDirectory(prefix="adaptive-state-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                state_file.write_text(json.dumps(payload), encoding="utf-8")
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    with self.assertRaisesRegex(RuntimeError, "loss counter"):
                        await runtime.load_runtime_state("BTC")

        asyncio.run(run_case())

    def test_intent_timeout_does_not_overwrite_original_halt_reason(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.variational_ticker = "BTC"
        intent = runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
        intent.sent_monotonic -= main_module.AUTO_VAR_FILL_TIMEOUT_SECONDS + 1
        runtime.pause_automation("original ambiguous execution reason")

        self.assertTrue(runtime.expire_pending_var_intent())
        self.assertEqual(
            runtime.automation_pause_reason,
            "original ambiguous execution reason",
        )

    def test_parameter_compile_does_not_block_event_loop(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

            class SlowFrozenStore:
                def snapshot(self, **_kwargs):
                    time.sleep(0.05)
                    return runtime.strategy_model.calibration_stats

            class Store:
                @staticmethod
                def frozen_copy():
                    return SlowFrozenStore()

            runtime.strategy_window_store = Store()
            started = time.perf_counter()
            compile_task = asyncio.create_task(
                runtime._refresh_parameter_epoch(time.time_ns() // 1_000_000)
            )
            await asyncio.sleep(0.005)
            elapsed = time.perf_counter() - started
            self.assertLess(elapsed, 0.03)
            self.assertFalse(compile_task.done())
            await compile_task
            self.assertIsNotNone(runtime.active_parameter_epoch)

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
