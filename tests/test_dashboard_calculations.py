import unittest
import asyncio
import io
import inspect
import json
import os
import tempfile
import time
from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from rich.console import Console

import main as main_module
from adaptive_strategy import (
    CloseCandidate,
    DirectionalRates,
    MarketFrame,
    Side,
    SourceClock,
    build_parameter_candidate,
)
from adaptive_strategy.serialization import open_candidate_to_payload
from variational.listener import (
    COMMAND_EXTENSION_BUILD,
    COMMAND_PROTOCOL_VERSION,
    CommandBroker,
    EventSink,
)

from main import (
    DASHBOARD_REFRESH_SECONDS,
    FirmQuoteDecision,
    OrderLifecycle,
    StrategyConfig,
    VariationalToLighterRuntime,
    build_trade_rounds,
    calculate_lighter_vwap,
    evaluate_firm_quote_guard,
    load_runtime_env,
    market_data_fresh,
    normalize_var_base_qty,
    opposite_var_order_side,
    strategy_config_from_env,
    strategy_config_from_payload,
    var_open_notional_usd,
    var_result_is_ambiguous,
)

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


def make_record(
    trade_key: str,
    side: str,
    qty: str,
    var_fill_price: str,
    lighter_fill_price: str,
) -> OrderLifecycle:
    return OrderLifecycle(
        trade_key=trade_key,
        trade_id=trade_key,
        side=side,
        qty=Decimal(qty),
        asset="BTC",
        auto_hedge_enabled=True,
        last_variational_status="filled",
        var_fill_price=Decimal(var_fill_price),
        lighter_fill_price=Decimal(lighter_fill_price),
    )


def make_open_candidate(runtime: VariationalToLighterRuntime, *, now_ms: int | None = None):
    captured_at_ms = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    epoch = build_parameter_candidate(
        now_ms=captured_at_ms,
        model=runtime.strategy_model,
        config_hash=runtime.strategy_config_hash,
        stats=runtime.strategy_model.calibration_stats,
        reference_notional_usd=runtime.strategy_config.reference_notional_usd,
        order_notional_usd=runtime.strategy_config.order_notional_usd,
        reserve_bps_per_leg=runtime.strategy_config.provisional_reserve_bps_per_leg,
        max_normal_round_wear_bps=runtime.strategy_config.max_normal_round_wear_bps,
    )
    clock = SourceClock(captured_at_ms, captured_at_ms, 0)
    buy_rate = epoch.thresholds.buy.final + (
        Decimal("2") * epoch.thresholds.buy.mad_30m
        if epoch.model_version in {"adaptive-median-v5", "adaptive-median-v6"}
        else Decimal("0.001")
    )
    sell_rate = epoch.thresholds.sell.final - Decimal("0.001")
    frame = MarketFrame(
        asset="BTC",
        captured_at_ms=captured_at_ms,
        variational_clock=clock,
        lighter_clock=clock,
        source_skew_ms=0,
        var_bid=Decimal("100"),
        var_ask=Decimal("100"),
        lighter_reference_buy_vwap=Decimal("100.1"),
        lighter_reference_sell_vwap=Decimal("100.2"),
        lighter_actual_buy_vwap=Decimal("100.1"),
        lighter_actual_sell_vwap=Decimal("100.2"),
        reference_notional_usd=runtime.strategy_config.reference_notional_usd,
        actual_notional_usd=runtime.strategy_config.order_notional_usd,
        reference_rates=DirectionalRates(buy_rate, sell_rate),
        actual_rates=DirectionalRates(buy_rate, sell_rate),
    )
    runtime.last_market_frame = frame
    decision = runtime.strategy_engine.evaluate_open(
        frame=frame,
        epoch=epoch,
        now_ms=captured_at_ms,
    )
    assert decision.open_candidate is not None
    assert decision.open_candidate.direction is Side.BUY
    return decision.open_candidate


def mark_fresh_variational_portfolio(
    runtime: VariationalToLighterRuntime,
    *,
    asset: str = "BTC",
    qty: Decimal = Decimal("0"),
) -> None:
    """Give reconciliation tests the same fresh monitor evidence as production."""

    now = datetime.now(timezone.utc).isoformat()
    monitor = runtime.runtime.monitor
    monitor.positions[asset] = {
        "qty": str(qty),
        "updated_at": now,
    }
    monitor.portfolio_summary = {"published_at": now}
    monitor.portfolio_request_id = "test-portfolio"
    monitor.portfolio_captured_at = now
    monitor.portfolio_fingerprint = "test-portfolio"
    monitor.portfolio_content_revision += 1
    monitor._portfolio_received_monotonic = time.monotonic()


class DashboardCalculationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temp_dir = tempfile.TemporaryDirectory(prefix="variational-tests-")
        root = Path(cls._temp_dir.name)
        cls._path_patchers = [
            patch.object(main_module, "LOG_DIR", root),
            patch.object(main_module, "OUTPUT_DIR", root),
            patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
            patch.object(main_module, "RUNTIME_STATE_FILE", root / "runtime_state.json"),
            patch.object(main_module, "EXECUTION_SAMPLES_FILE", root / "execution_samples.json", create=True),
        ]
        for path_patcher in cls._path_patchers:
            path_patcher.start()

    @classmethod
    def tearDownClass(cls) -> None:
        for path_patcher in reversed(cls._path_patchers):
            path_patcher.stop()
        cls._temp_dir.cleanup()

    def test_aborted_browser_fetch_is_treated_as_ambiguous(self) -> None:
        self.assertTrue(var_result_is_ambiguous({"ok": False, "error": "The operation was aborted"}))
        self.assertFalse(var_result_is_ambiguous({"ok": False, "error": "Var fetch HTTP 422"}))

    def test_status_uses_firm_guard_estimate_instead_of_preliminary_signal(self) -> None:
        extractor = getattr(main_module, "firm_guard_pnl_from_result", None)
        self.assertIsNotNone(extractor)
        result = {"detail": {"quote": {"guardPnl": "0.11142355"}}}
        self.assertEqual(extractor(result), Decimal("0.11142355"))

    def test_pairs_matching_opposite_records_into_one_completed_round(self) -> None:
        open_record = make_record(
            "open",
            "buy",
            "0.004753",
            "63123.58",
            "63153.918105263",
        )
        close_record = make_record(
            "close",
            "sell",
            "0.004753",
            "63058.21",
            "63119.10",
        )

        current_open, history = build_trade_rounds([open_record, close_record])

        self.assertIsNone(current_open)
        self.assertEqual(len(history), 1)
        self.assertIs(history[0].open_record, open_record)
        self.assertIs(history[0].close_record, close_record)
        self.assertEqual(
            history[0].round_pnl.quantize(Decimal("0.000001")),
            Decimal("-0.145213"),
        )

    def test_mismatched_partial_close_is_not_reinterpreted_as_reverse_open(self) -> None:
        open_record = make_record("open", "buy", "0.005", "1000", "1001")
        partial_close = make_record("partial-close", "sell", "0.002", "1002", "1003")

        current_open, history = build_trade_rounds([open_record, partial_close])

        self.assertIs(current_open, open_record)
        self.assertEqual(history, [])

    def test_strategy_config_payload_sanitizes_values(self) -> None:
        config = strategy_config_from_payload(
            {
                "executionMode": "live",
                "referenceNotionalUsd": "500",
                "orderNotionalUsd": "200",
                "buyDynamicThresholdMinPct": "0.06",
                "sellDynamicThresholdMinPct": "-0.08",
                "provisionalReserveBpsPerLeg": "0.50",
                "maxNormalRoundWearBps": "1.0",
                "parameterRefreshMinutes": "5",
                "parameterConfirmations": "1",
                "earlyExitMinutes": "30",
                "maxHoldMinutes": "120",
                "maxQuoteAgeMs": "350",
                "dashboardRefreshMs": "250",
                "samplingEnabled": True,
                "varOrderResultTimeoutMs": "20000",
            },
            current=StrategyConfig(),
        )

        self.assertEqual(config.execution_mode, "live")
        self.assertEqual(config.reference_notional_usd, Decimal("500"))
        self.assertEqual(config.order_notional_usd, Decimal("200"))
        self.assertEqual(config.buy_dynamic_threshold_min_pct, Decimal("0.06"))
        self.assertEqual(config.sell_dynamic_threshold_min_pct, Decimal("-0.08"))
        self.assertEqual(config.provisional_reserve_bps_per_leg, Decimal("0.50"))
        self.assertEqual(config.parameter_refresh_seconds, 300)
        self.assertEqual(config.parameter_confirmations, 1)
        self.assertEqual(config.early_exit_seconds, 1800)
        self.assertEqual(config.max_hold_seconds, 7200)
        self.assertEqual(config.max_quote_age_ms, 350)
        self.assertEqual(config.dashboard_refresh_seconds, 0.25)
        self.assertTrue(config.sampling_enabled)
        self.assertEqual(config.var_order_result_timeout_ms, 20000)

    def test_strategy_config_payload_rejects_invalid_values(self) -> None:
        current = StrategyConfig(reference_notional_usd=Decimal("500"))

        config = strategy_config_from_payload(
            {
                "executionMode": "invalid",
                "referenceNotionalUsd": "-1",
                "orderNotionalUsd": "-1",
                "buyDynamicThresholdMinPct": "0",
                "sellDynamicThresholdMinPct": "0",
                "parameterConfirmations": "2",
                "maxQuoteAgeMs": "0",
                "dashboardRefreshMs": "25",
                "varOrderResultTimeoutMs": "100",
                "roundCooldownSeconds": "-1",
            },
            current=current,
        )

        self.assertEqual(config.execution_mode, "observe")
        self.assertEqual(config.reference_notional_usd, Decimal("500"))
        self.assertEqual(config.order_notional_usd, Decimal("200"))
        self.assertEqual(config.buy_dynamic_threshold_min_pct, Decimal("0.05"))
        self.assertEqual(config.sell_dynamic_threshold_min_pct, Decimal("-0.073"))
        self.assertEqual(config.parameter_confirmations, 1)
        self.assertEqual(config.max_quote_age_ms, 600)
        self.assertEqual(config.dashboard_refresh_seconds, DASHBOARD_REFRESH_SECONDS)
        self.assertEqual(config.round_cooldown_seconds, 30)
        self.assertEqual(config.var_order_result_timeout_ms, 5000)

    def test_strategy_config_reads_env_values(self) -> None:
        env_values = {
            "STRATEGY_EXECUTION_MODE": "live",
            "STRATEGY_REFERENCE_NOTIONAL_USD": "500",
            "STRATEGY_ORDER_NOTIONAL_USD": "200",
            "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": "0.06",
            "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": "-0.08",
            "STRATEGY_PROVISIONAL_RESERVE_BPS_PER_LEG": "0.50",
            "STRATEGY_PARAMETER_REFRESH_MINUTES": "1",
            "STRATEGY_PARAMETER_CONFIRMATIONS": "1",
            "STRATEGY_EARLY_EXIT_MINUTES": "30",
            "STRATEGY_MAX_HOLD_MINUTES": "120",
            "STRATEGY_MAX_QUOTE_AGE_MS": "450",
            "STRATEGY_DASHBOARD_REFRESH_MS": "200",
            "STRATEGY_ROUND_COOLDOWN_SECONDS": "30",
            "STRATEGY_VAR_ORDER_RESULT_TIMEOUT_MS": "5000",
            "VARIATIONAL_COMMAND_AUTH_TOKEN": "test-command-auth-token-0123456789abcdef",
        }
        previous = {key: os.environ.get(key) for key in env_values}
        try:
            os.environ.update(env_values)
            config = strategy_config_from_env()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(config.execution_mode, "live")
        self.assertEqual(config.reference_notional_usd, Decimal("500"))
        self.assertEqual(config.order_notional_usd, Decimal("200"))
        self.assertEqual(config.buy_dynamic_threshold_min_pct, Decimal("0.06"))
        self.assertEqual(config.sell_dynamic_threshold_min_pct, Decimal("-0.08"))
        self.assertEqual(config.max_quote_age_ms, 450)
        self.assertEqual(config.dashboard_refresh_seconds, 0.2)
        self.assertEqual(config.round_cooldown_seconds, 30)
        self.assertEqual(config.var_order_result_timeout_ms, 5000)

    def test_runtime_env_file_overrides_shell_and_rejects_ambiguity(self) -> None:
        content = "\n".join(
            (
                "LIGHTER_PRIVATE_KEY=file-private-key",
                "LIGHTER_API_KEY_INDEX=2",
                "LIGHTER_ACCOUNT_INDEX=3",
                "STRATEGY_EXECUTION_MODE=live",
                "STRATEGY_ORDER_NOTIONAL_USD=250",
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.06",
                "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT=-0.08",
                "VARIATIONAL_RUNTIME_DIR=/tmp/variational-runtime",
                "RESEARCH_DATABASE_FILE=/tmp/variational-research.sqlite3",
            )
        )
        with tempfile.TemporaryDirectory(prefix="runtime-env-") as tmp:
            dotenv_path = Path(tmp) / ".env"
            dotenv_path.write_text(content, encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "LIGHTER_PRIVATE_KEY": "stale-shell-private-key",
                    "STRATEGY_ORDER_NOTIONAL_USD": "999",
                    "STRATEGY_MAX_QUOTE_AGE_MS": "9999",
                    "API_KEY_PRIVATE_KEY": "legacy-private-key-alias",
                },
                clear=False,
            ):
                load_runtime_env(dotenv_path)
                self.assertEqual(os.environ["LIGHTER_PRIVATE_KEY"], "file-private-key")
                self.assertEqual(os.environ["STRATEGY_ORDER_NOTIONAL_USD"], "250")
                self.assertEqual(
                    os.environ["VARIATIONAL_RUNTIME_DIR"],
                    "/tmp/variational-runtime",
                )
                self.assertEqual(
                    os.environ["RESEARCH_DATABASE_FILE"],
                    "/tmp/variational-research.sqlite3",
                )
                self.assertNotIn("STRATEGY_MAX_QUOTE_AGE_MS", os.environ)
                self.assertNotIn("API_KEY_PRIVATE_KEY", os.environ)

            dotenv_path.write_text(
                content + "\nSTRATEGY_ORDER_NOTIONAL_USD=300\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "Duplicate .env keys"):
                load_runtime_env(dotenv_path)

            dotenv_path.write_text(
                content.replace(
                    "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT=0.06\n",
                    "",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "Required .env keys are missing"):
                load_runtime_env(dotenv_path)

            dotenv_path.write_text(
                content + "\nSTRATEGY_UNKNOWN_OPTION=500\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "Unsupported .env keys"):
                load_runtime_env(dotenv_path)

    def test_configure_runtime_paths_uses_loaded_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="runtime-paths-") as tmp:
            runtime_dir = Path(tmp) / "runtime"
            with (
                patch.dict(
                    os.environ,
                    {"VARIATIONAL_RUNTIME_DIR": str(runtime_dir)},
                    clear=False,
                ),
                patch.object(main_module, "LOG_DIR", main_module.LOG_DIR),
                patch.object(main_module, "OUTPUT_DIR", main_module.OUTPUT_DIR),
                patch.object(main_module, "APP_LOG_FILE", main_module.APP_LOG_FILE),
                patch.object(
                    main_module,
                    "RUNTIME_STATE_FILE",
                    main_module.RUNTIME_STATE_FILE,
                ),
                patch.object(
                    main_module,
                    "EXECUTION_SAMPLES_FILE",
                    main_module.EXECUTION_SAMPLES_FILE,
                ),
            ):
                main_module.configure_runtime_paths()
                self.assertEqual(main_module.LOG_DIR, runtime_dir)
                self.assertEqual(main_module.OUTPUT_DIR, runtime_dir)
                self.assertEqual(
                    main_module.RUNTIME_STATE_FILE,
                    runtime_dir / "runtime_state.json",
                )
                self.assertEqual(
                    main_module.EXECUTION_SAMPLES_FILE,
                    runtime_dir / "execution_samples.json",
                )

    def test_strategy_accepts_any_positive_target_amount(self) -> None:
        for target in ("50", "500", "1234.56"):
            with self.subTest(target=target), patch.dict(
                os.environ,
                {"STRATEGY_ORDER_NOTIONAL_USD": target},
                clear=False,
            ):
                config = strategy_config_from_env()
                self.assertEqual(config.order_notional_usd, Decimal(target))

    def test_runtime_converts_env_threshold_percentages_once_to_engine_rates(self) -> None:
        with patch.dict(
            os.environ,
            {
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": "0.06",
                "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": "-0.08",
            },
            clear=False,
        ):
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )

        self.assertEqual(
            runtime.strategy_engine.buy_open_dynamic_threshold_minimum,
            Decimal("0.0006"),
        )
        self.assertEqual(
            runtime.strategy_engine.sell_open_dynamic_threshold_minimum,
            Decimal("-0.0008"),
        )

    def test_removed_strategy_env_is_a_hard_migration_error(self) -> None:
        with patch.dict(os.environ, {"AUTO_VAR_OPEN_ENABLED": "true"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "Removed strategy environment"):
                strategy_config_from_env()

    def test_live_env_does_not_require_command_auth_token(self) -> None:
        with patch.dict(
            os.environ,
            {
                "STRATEGY_EXECUTION_MODE": "live",
                "VARIATIONAL_COMMAND_AUTH_TOKEN": "",
            },
            clear=False,
        ):
            config = strategy_config_from_env()
            self.assertEqual(config.execution_mode, "live")

    def test_invalid_explicit_strategy_env_never_falls_back_to_trade_defaults(self) -> None:
        with patch.dict(
            os.environ,
            {
                "STRATEGY_EXECUTION_MODE": "canray",
                "STRATEGY_ORDER_NOTIONAL_USD": "2OO",
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": "0",
                "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": "0.01",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "STRATEGY_EXECUTION_MODE.*STRATEGY_ORDER_NOTIONAL_USD.*"
                "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT.*"
                "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT",
            ):
                strategy_config_from_env()

    def test_legacy_canary_mode_is_rejected_instead_of_becoming_continuous_live(self) -> None:
        with patch.dict(
            os.environ,
            {"STRATEGY_EXECUTION_MODE": "canary"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "STRATEGY_EXECUTION_MODE must be observe or live",
            ):
                strategy_config_from_env()

    def test_runtime_rejects_reference_notional_that_mismatches_sealed_model(self) -> None:
        with patch.dict(
            os.environ,
            {"STRATEGY_REFERENCE_NOTIONAL_USD": "501"},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "fixed contract|sealed model basis"):
                VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

    def test_strategy_config_hash_covers_epoch_activation(self) -> None:
        model_hash = "a" * 64
        base_hash = main_module.adaptive_strategy_config_hash(
            StrategyConfig(),
            model_hash=model_hash,
        )
        changed_configs = (
            StrategyConfig(parameter_refresh_seconds=600),
            StrategyConfig(parameter_confirmations=3),
            StrategyConfig(buy_dynamic_threshold_min_pct=Decimal("0.06")),
            StrategyConfig(sell_dynamic_threshold_min_pct=Decimal("-0.08")),
        )
        for config in changed_configs:
            with self.subTest(config=config):
                self.assertNotEqual(
                    main_module.adaptive_strategy_config_hash(
                        config,
                        model_hash=model_hash,
                    ),
                    base_hash,
                )

    def test_opposite_var_order_side_for_closing(self) -> None:
        self.assertEqual(opposite_var_order_side("buy"), "SELL")
        self.assertEqual(opposite_var_order_side("sell"), "BUY")
        self.assertIsNone(opposite_var_order_side("unknown"))

    def test_market_data_fresh_requires_both_raw_quote_ages_under_limit(self) -> None:
        self.assertTrue(market_data_fresh(100, 250, 500))
        self.assertFalse(market_data_fresh(501, 250, 500))
        self.assertFalse(market_data_fresh(100, 501, 500))
        self.assertFalse(market_data_fresh(None, 250, 500))

    def test_lighter_order_book_update_requires_nonce_continuity(self) -> None:
        os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
        os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
        os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.lighter_order_book_offset = 10
        runtime.lighter_order_book_nonce = 100

        self.assertTrue(runtime.validate_order_book_update({"offset": 11, "begin_nonce": 100}))
        self.assertFalse(runtime.validate_order_book_update({"offset": 11, "begin_nonce": 99}))
        self.assertFalse(runtime.validate_order_book_update({"offset": 10, "begin_nonce": 100}))

    def test_lighter_account_query_uses_sdk_104_auth_header_parameter(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class FakeOrderApi:
                def __init__(self) -> None:
                    self.kwargs = None

                async def account_active_orders(self, **kwargs):
                    self.kwargs = kwargs
                    return SimpleNamespace(orders=[])

            class FakeClient:
                def __init__(self) -> None:
                    self.api_client = object()
                    self.order_api = FakeOrderApi()

                def create_auth_token_with_expiry(self, **kwargs):
                    return "signed-token", None

            class FakeAccountApi:
                def __init__(self, api_client) -> None:
                    pass

                async def account(self, **kwargs):
                    return SimpleNamespace(accounts=[SimpleNamespace(positions=[])])

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.lighter_client = FakeClient()
            runtime.lighter_market_index = 1
            with patch("main.AccountApi", FakeAccountApi):
                position, active_orders = await runtime.get_lighter_account_snapshot()

            self.assertEqual(position, Decimal("0"))
            self.assertEqual(active_orders, 0)
            self.assertEqual(runtime.lighter_client.order_api.kwargs["auth"], "signed-token")
            self.assertNotIn("authorization", runtime.lighter_client.order_api.kwargs)

        asyncio.run(run_case())

    def test_preferred_btc_quote_never_falls_back_to_current_xau_quote(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.current_quote_asset = "XAU"
                runtime.runtime.monitor.quotes["XAU"] = {
                    "asset": "XAU",
                    "bid": "3300",
                    "ask": "3301",
                }

            bid, ask, asset = await runtime.get_variational_best_bid_ask("BTC")
            self.assertIsNone(bid)
            self.assertIsNone(ask)
            self.assertIsNone(asset)
            self.assertIsNone(await runtime.get_variational_quote_age_ms("BTC"))

        asyncio.run(run_case())

    def test_stale_display_frame_updates_values_but_trading_frame_stays_blocked(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.market_generation = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("100")
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.quotes["BTC"] = {
                    "asset": "BTC",
                    "bid": "100",
                    "ask": "100.1",
                    "received_monotonic": time.monotonic() - 2,
                    "captured_at": None,
                }
            async with runtime.lighter_order_book_lock:
                runtime.lighter_order_book = {
                    "bids": {Decimal("100.2"): Decimal("10")},
                    "asks": {Decimal("100.3"): Decimal("10")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_sequence_gap = False
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            blocked, blocked_observation = await runtime.current_adaptive_market_frame()
            display, display_observation = await runtime.current_adaptive_market_frame(
                allow_stale_for_display=True
            )

            self.assertIsNone(blocked)
            self.assertEqual(blocked_observation["rejection_reason"], "market_data_stale")
            self.assertIsNotNone(display)
            self.assertFalse(display_observation["valid"])
            self.assertEqual(
                display_observation["rejection_reason"],
                "market_data_stale",
            )

        asyncio.run(run_case())

    def test_var_open_notional_uses_variational_open_fill(self) -> None:
        open_record = make_record(
            "open",
            "buy",
            "0.004764",
            "62974.34",
            "63014.315336134",
        )

        self.assertEqual(
            var_open_notional_usd(open_record).quantize(Decimal("0.01")),
            Decimal("300.01"),
        )

    def test_dashboard_labels_open_notional_instead_of_qty(self) -> None:
        async def render_text() -> str:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.ticker = "BTC"
            runtime.variational_ticker = "BTC"
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.current_quote_asset = "BTC"
                runtime.runtime.monitor.quotes["BTC"] = {
                    "asset": "BTC",
                    "bid": "62900.19",
                    "ask": "62904.82",
                }
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("62955.6")
                runtime.lighter_best_ask = Decimal("62957.3")
            async with runtime._record_lock:
                runtime.records = {
                    "open": make_record("open", "buy", "0.004764", "62974.34", "63014.315336134"),
                    "close": make_record("close", "sell", "0.004764", "63009.18", "63053.0"),
                }
                runtime.record_order.extend(["open", "close"])

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            return console.export_text()

        text = asyncio.run(render_text())

        self.assertIn("金额与风控", text)
        self.assertIn("参考 500.00U", text)
        self.assertIn("实盘 200.00U", text)
        self.assertIn("5m：", text)
        self.assertIn("30m：", text)
        self.assertIn("1h：", text)
        self.assertNotIn("4h：", text)
        self.assertIn("周期权重", text)
        self.assertIn("5m 25% | 30m 45% | 1h 30%", text)
        self.assertIn("刷新节奏", text)
        self.assertIn("行情/价差 200ms | 中位数采样 Var事件（1s兜底） | 门槛更新 1m", text)
        self.assertNotIn("开仓余量", text)
        self.assertIn("平仓预留 0.250bps/leg", text)
        self.assertIn("1.0bps/round", text)
        self.assertIn("三窗门槛", text)
        self.assertIn("价差、预估盈亏与开仓信号", text)
        self.assertIn("预估开仓PnL", text)
        self.assertIn("5m中位数", text)
        self.assertIn("30m中位数", text)
        self.assertIn("1h中位数", text)
        self.assertNotIn("4h中位数", text)
        self.assertIn("做多 Var / 做空 Lighter", text)
        self.assertIn("做空 Var / 做多 Lighter", text)
        self.assertIn("影子样本", text)
        self.assertIn("开平规则", text)
        self.assertIn("轮次结束后冷却", text)
        self.assertIn("账户对账", text)
        self.assertIn("自动化就绪", text)
        self.assertIn("自动对冲", text)
        self.assertIn("Lighter 下单通道", text)
        self.assertIn("REST FALLBACK", text)
        self.assertIn("策略自动开仓", text)
        self.assertIn("策略自动平仓", text)
        self.assertIn("自动化保护", text)
        self.assertIn("300.01U", text)
        self.assertIn("轮次盈亏", text)
        self.assertRegex(text, r"[+-]\d+\.\d{4}U")
        self.assertNotIn("63014.315336134", text)
        self.assertNotIn("Synchronized Market", text)
        self.assertNotIn("Windows & Epoch", text)
        self.assertNotIn("Theory, Firm & Actual", text)
        self.assertNotIn("Execution & Risk", text)
        self.assertNotIn("毫秒", text)
        self.assertNotIn("基点/", text)
        self.assertNotIn("80分位", text)

    def test_dashboard_market_columns_do_not_move_when_age_reaches_four_digits(self) -> None:
        async def render_with_ages(var_age: int, lighter_age: int) -> str:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.get_variational_best_bid_ask = AsyncMock(
                return_value=(Decimal("64744.75"), Decimal("64749.43"), "BTC")
            )
            runtime.get_lighter_best_bid_ask = AsyncMock(
                return_value=(Decimal("64762.1"), Decimal("64762.2"))
            )
            runtime.get_variational_quote_age_ms = AsyncMock(return_value=var_age)
            runtime.get_lighter_quote_age_ms = AsyncMock(return_value=lighter_age)

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            return console.export_text()

        short_age = asyncio.run(render_with_ages(999, 14))
        four_digit_age = asyncio.run(render_with_ages(1_000, 1_234))

        def market_dividers(text: str) -> list[int]:
            header = next(line for line in text.splitlines() if "平台" in line)
            return [index for index, char in enumerate(header) if char == "│"]

        self.assertEqual(market_dividers(short_age), market_dividers(four_digit_age))
        self.assertIn("999ms", short_age)
        self.assertIn("1.0s", four_digit_age)
        self.assertIn("1.2s", four_digit_age)

    def test_dashboard_keeps_last_valid_rates_during_a_brief_quote_gap(self) -> None:
        async def render_twice() -> tuple[str, str, str]:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            candidate = make_open_candidate(runtime)
            runtime.last_strategy_decision = main_module.StrategyDecision(
                main_module.StrategyAction.OPEN,
                "frozen_threshold_passed",
                open_candidate=candidate,
            )
            runtime.get_variational_best_bid_ask = AsyncMock(
                return_value=(Decimal("100"), Decimal("100.1"), "BTC")
            )
            runtime.get_lighter_best_bid_ask = AsyncMock(
                return_value=(Decimal("100.2"), Decimal("100.3"))
            )
            runtime.get_variational_quote_age_ms = AsyncMock(return_value=100)
            runtime.get_lighter_quote_age_ms = AsyncMock(return_value=100)

            first_console = Console(record=True, width=177, file=io.StringIO())
            first_console.print(await runtime.render_dashboard())
            first_text = first_console.export_text()
            expected_rate = runtime._fmt_rate_percent(
                runtime.last_market_frame.reference_rates.buy
            )

            runtime.last_market_frame = None
            second_console = Console(record=True, width=177, file=io.StringIO())
            second_console.print(await runtime.render_dashboard())
            return first_text, second_console.export_text(), expected_rate

        first, second, expected_rate = asyncio.run(render_twice())
        self.assertIn(expected_rate, first)
        self.assertIn(expected_rate, second)
        self.assertIn("等待新报价（显示最新已知值）", second)

        def spread_dividers(text: str) -> list[int]:
            header = next(line for line in text.splitlines() if "500U当前" in line)
            return [index for index, char in enumerate(header) if char == "│"]

        self.assertEqual(spread_dividers(first), spread_dividers(second))

    def test_dashboard_prices_and_spreads_use_the_same_fresh_render_pass(self) -> None:
        async def render_once() -> tuple[str, MarketFrame]:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            runtime.active_parameter_epoch = candidate.epoch
            old_frame = runtime.last_market_frame
            assert old_frame is not None
            fresh_frame = replace(
                old_frame,
                var_bid=Decimal("65432.10"),
                var_ask=Decimal("65436.20"),
                reference_rates=DirectionalRates(
                    Decimal("0.012345"),
                    Decimal("-0.012345"),
                ),
                actual_rates=DirectionalRates(
                    Decimal("0.012300"),
                    Decimal("-0.012300"),
                ),
            )
            runtime.current_adaptive_market_frame = AsyncMock(
                return_value=(fresh_frame, {"valid": True})
            )
            runtime.get_variational_best_bid_ask = AsyncMock(
                return_value=(Decimal("1"), Decimal("2"), "BTC")
            )
            runtime.get_lighter_best_bid_ask = AsyncMock(
                return_value=(Decimal("65440"), Decimal("65441"))
            )
            runtime.get_variational_quote_age_ms = AsyncMock(return_value=10)
            runtime.get_lighter_quote_age_ms = AsyncMock(return_value=10)

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            return console.export_text(), fresh_frame

        text, fresh = asyncio.run(render_once())
        self.assertIn("65432.10", text)
        self.assertIn("65436.20", text)
        self.assertIn(
            main_module.VariationalToLighterRuntime._fmt_rate_percent(
                fresh.reference_rates.buy
            ),
            text,
        )

    def test_dashboard_recalculates_latest_spreads_even_when_trade_frame_is_stale(self) -> None:
        async def render_once() -> tuple[str, MarketFrame]:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            candidate = make_open_candidate(runtime)
            runtime.active_parameter_epoch = candidate.epoch
            old_frame = runtime.last_market_frame
            assert old_frame is not None
            stale_latest = replace(
                old_frame,
                var_bid=Decimal("65500"),
                var_ask=Decimal("65501"),
                reference_rates=DirectionalRates(
                    Decimal("0.023456"),
                    Decimal("-0.023456"),
                ),
                actual_rates=DirectionalRates(
                    Decimal("0.023400"),
                    Decimal("-0.023400"),
                ),
            )
            frame_mock = AsyncMock(
                return_value=(
                    stale_latest,
                    {"valid": False, "rejection_reason": "market_data_stale"},
                )
            )
            runtime.current_adaptive_market_frame = frame_mock
            runtime.get_variational_best_bid_ask = AsyncMock(
                return_value=(Decimal("65500"), Decimal("65501"), "BTC")
            )
            runtime.get_lighter_best_bid_ask = AsyncMock(
                return_value=(Decimal("65510"), Decimal("65511"))
            )
            runtime.get_variational_quote_age_ms = AsyncMock(return_value=1_200)
            runtime.get_lighter_quote_age_ms = AsyncMock(return_value=20)

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            frame_mock.assert_awaited_once_with(allow_stale_for_display=True)
            return console.export_text(), stale_latest

        text, stale_latest = asyncio.run(render_once())
        self.assertIn(
            main_module.VariationalToLighterRuntime._fmt_rate_percent(
                stale_latest.reference_rates.buy
            ),
            text,
        )
        self.assertIn("仅显示：行情已过期", text)
        self.assertIn("行情已过期（显示最新已知值）", text)

    def test_dashboard_window_statistics_publish_on_every_one_second_sample(self) -> None:
        async def sample_twice() -> tuple[int, Decimal]:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            make_open_candidate(runtime)
            first = runtime.last_market_frame
            assert first is not None
            second = replace(
                first,
                reference_rates=DirectionalRates(Decimal("0.003"), Decimal("-0.003")),
            )
            runtime.current_adaptive_market_frame = AsyncMock(
                side_effect=[
                    (first, {"valid": True}),
                    (second, {"valid": True}),
                ]
            )
            runtime.strategy_market_sample_writer = None
            await runtime.capture_strategy_sample_once(now_ms=1_000_000)
            await runtime.capture_strategy_sample_once(now_ms=1_001_000)
            window = runtime.strategy_window_stats[Side.BUY][5]
            return window.sample_count, window.median

        count, median = asyncio.run(sample_twice())
        self.assertEqual(count, 2)
        self.assertGreater(median, Decimal("0"))

    def test_dashboard_signal_dot_uses_dynamic_threshold_and_200u_economics(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        candidate = make_open_candidate(runtime)
        epoch = candidate.epoch
        preview = runtime._dashboard_direction_preview(
            side=Side.BUY,
            frame=runtime.last_market_frame,
            epoch=epoch,
            current_open=None,
            live_frame=True,
            frame_rejection_reason=None,
            submission_block_reason=None,
            command_connected=True,
            is_zh=True,
        )

        self.assertTrue(preview["allowed"])
        self.assertIsNotNone(preview["open_pnl"])
        self.assertIsNotNone(preview["round_lower_bound"])
        self.assertIn("observe", preview["reason"])
        self.assertIn("green", runtime._fmt_signal_dot(preview["allowed"]))

        blocked = runtime._dashboard_direction_preview(
            side=Side.SELL,
            frame=runtime.last_market_frame,
            epoch=epoch,
            current_open=None,
            live_frame=True,
            frame_rejection_reason=None,
            submission_block_reason=None,
            command_connected=True,
            is_zh=True,
        )
        self.assertFalse(blocked["allowed"])
        self.assertIn("-0.073%", blocked["reason"])
        self.assertIn("red", runtime._fmt_signal_dot(blocked["allowed"]))
        self.assertIn("green", runtime._fmt_colored_rate(Decimal("0.001")))
        self.assertIn("red", runtime._fmt_colored_rate(Decimal("-0.001")))

    def test_variational_event_does_not_wait_for_slow_lighter_hedge(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class SlowHedgeRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.hedge_started = asyncio.Event()
                    self.hedge_release = asyncio.Event()

                async def place_lighter_order(self, record: OrderLifecycle) -> None:
                    self.hedge_started.set()
                    await self.hedge_release.wait()

            runtime = SlowHedgeRuntime()
            event = {
                "trade_id": "slow-hedge-open",
                "side": "buy",
                "qty": "0.004789",
                "asset": "BTC",
                "status": "filled",
                "price": "62637.78",
                "timestamp": "2026-07-08T06:51:00.977Z",
            }

            task = asyncio.create_task(runtime.process_variational_trade_event(event))
            await asyncio.wait_for(runtime.hedge_started.wait(), timeout=0.1)
            await asyncio.wait_for(task, timeout=0.1)
            runtime.hedge_release.set()
            if runtime.hedge_tasks:
                await asyncio.wait_for(asyncio.gather(*runtime.hedge_tasks), timeout=0.1)

            async with runtime._record_lock:
                record = runtime.records["id:slow-hedge-open"]
                self.assertEqual(record.var_fill_price, Decimal("62637.78"))

        asyncio.run(run_case())

    def test_pending_var_event_does_not_start_lighter_until_filled(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

            runtime = TrackingRuntime()
            pending = {
                "trade_id": "pending-then-filled",
                "side": "buy",
                "qty": "0.3",
                "asset": "BTC",
                "status": "pending",
                "price": "1000",
                "timestamp": "2026-07-09T00:00:00Z",
            }
            await runtime.process_variational_trade_event(pending)
            self.assertEqual(runtime.scheduled, [])
            self.assertIsNone(runtime.records["id:pending-then-filled"].var_fill_price)

            await runtime.process_variational_trade_event({**pending, "status": "filled"})
            self.assertEqual(runtime.scheduled, ["id:pending-then-filled"])

        asyncio.run(run_case())

    def test_portfolio_fallback_recovers_one_fill_and_deduplicates_late_event(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.ticker = "BTC"
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("300"))
            runtime.pending_var_intent.sent_monotonic -= 2
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.positions["BTC"] = {
                    "qty": "0.3",
                    "avg_entry_price": "1000",
                }
                runtime.runtime.monitor._portfolio_received_monotonic = (
                    asyncio.get_running_loop().time()
                )

            self.assertTrue(await runtime.recover_pending_var_intent_from_portfolio())
            self.assertIsNone(runtime.pending_var_intent)
            self.assertEqual(runtime.scheduled, [runtime.record_order[0]])
            self.assertEqual(len(runtime.records), 1)
            recovered = runtime.records[runtime.record_order[0]]
            self.assertEqual(recovered.var_fill_source, "portfolio")

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "late-real-fill",
                    "side": "buy",
                    "qty": "0.3",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "1000.25",
                    "timestamp": recovered.var_fill_ts_iso,
                }
            )
            self.assertEqual(len(runtime.records), 1)
            self.assertEqual(runtime.scheduled, [runtime.record_order[0]])
            self.assertEqual(recovered.var_fill_source, "event")
            self.assertEqual(recovered.var_fill_price, Decimal("1000.25"))

        asyncio.run(run_case())

    def test_startup_trade_event_drain_processes_events_after_saved_cursor(self) -> None:
        async def run_case() -> None:
            class FakeMonitor:
                async def get_trade_events_since(self, cursor: int, limit: int = 500):
                    return [
                        {
                            "event_seq": 6,
                            "trade_id": "startup-fill",
                            "side": "buy",
                            "qty": "2",
                            "asset": "BTC",
                            "price": "100",
                            "status": "filled",
                        }
                    ] if cursor < 6 else []

            class DrainRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.processed: list[str] = []

                async def process_variational_trade_event(self, event: dict) -> None:
                    self.processed.append(str(event["trade_id"]))

            runtime = DrainRuntime()
            runtime.runtime.monitor = FakeMonitor()
            runtime.trade_event_cursor = 5

            count = await runtime.drain_pending_trade_events()

            self.assertEqual(count, 1)
            self.assertEqual(runtime.trade_event_cursor, 6)
            self.assertEqual(runtime.processed, ["startup-fill"])

        asyncio.run(run_case())

    def test_startup_trade_cursor_is_captured_after_initial_portfolio_baseline(self) -> None:
        source = inspect.getsource(VariationalToLighterRuntime.run)
        cursor_assignment = source.index(
            "self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()"
        )
        receiver_start = source.index("await self.runtime.start()")
        portfolio_ready = source.index("await self.wait_for_variational_portfolio_ready()")
        asset_activation = source.index("await self.activate_asset(initial_asset, reason=\"startup\")")

        self.assertGreater(cursor_assignment, receiver_start)
        self.assertGreater(cursor_assignment, portfolio_ready)
        self.assertLess(cursor_assignment, asset_activation)

    def test_startup_replayed_trade_uses_exchange_time_and_never_hedges(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

            runtime = TrackingRuntime()
            runtime.var_event_accept_after = datetime.now(timezone.utc)
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "manual-template-close-replay",
                    "side": "sell",
                    "qty": "0.008",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62500",
                    "timestamp": "2026-07-13T13:00:00Z",
                    # The extension wrapper is fresh even though the exchange
                    # trade itself predates this process.
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(runtime.records, {})
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_fresh_manual_var_open_hedges_once_from_flat(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

            runtime = TrackingRuntime()
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            now = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "manual-template-close-live",
                    "side": "sell",
                    "qty": "0.008",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62500",
                    "timestamp": now,
                    "captured_at": now,
                }
            )

            self.assertEqual(runtime.scheduled, ["id:manual-template-close-live"])
            record = runtime.records["id:manual-template-close-live"]
            self.assertEqual(record.var_event_origin, "MANUAL_LIVE")
            self.assertEqual(record.strategy_phase, "open")
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_pending_intent_amount_mismatch_never_binds_or_hedges(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.variational_ticker = "BTC"
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

            runtime = TrackingRuntime()
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            intent = runtime.mark_var_intent_sent("open", "SELL", Decimal("500"))
            # Once Commit is in flight, a differently-sized fill cannot be
            # assumed to be an unrelated manual order.
            intent.state = main_module.VAR_INTENT_COMMITTING
            intent.firm_price = Decimal("62500")
            intent.firm_qty = Decimal("0.008")
            intent.prepared_at_iso = datetime.now(timezone.utc).isoformat()
            now = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "wrong-manual-amount",
                    "side": "sell",
                    "qty": "0.0032",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62500",
                    "timestamp": now,
                    "captured_at": now,
                }
            )

            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(len(runtime.records), 1)
            recovery = runtime.records["id:wrong-manual-amount"]
            self.assertEqual(recovery.execution_state, "RECOVERY_REQUIRED")
            self.assertEqual(recovery.var_event_origin, "MANUAL_LIVE")
            self.assertIsNotNone(runtime.pending_var_intent)
            self.assertTrue(runtime.automation_paused)

        asyncio.run(run_case())

    def test_matching_fresh_intent_can_still_schedule_lighter(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.variational_ticker = "BTC"
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"
                    return True

            runtime = TrackingRuntime()
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            intent = runtime.mark_var_intent_sent("open", "SELL", Decimal("500"))
            intent.state = main_module.VAR_INTENT_COMMIT_AMBIGUOUS
            intent.firm_price = Decimal("62500")
            intent.firm_qty = Decimal("0.008")
            intent.prepared_at_iso = datetime.now(timezone.utc).isoformat()
            now = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "matched-auto-fill",
                    "side": "sell",
                    "qty": "0.008",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62500",
                    "timestamp": now,
                    "captured_at": now,
                }
            )

            self.assertEqual(runtime.scheduled, ["id:matched-auto-fill"])
            self.assertIsNone(runtime.pending_var_intent)
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_current_round_status_shows_lighter_hedge_failure(self) -> None:
        async def render_text() -> str:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.ticker = "BTC"
            runtime.variational_ticker = "BTC"
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.current_quote_asset = "BTC"
                runtime.runtime.monitor.quotes["BTC"] = {
                    "asset": "BTC",
                    "bid": "62600.00",
                    "ask": "62601.00",
                }
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("62640.00")
                runtime.lighter_best_ask = Decimal("62641.00")
            failed_open = make_record("open", "buy", "0.004789", "62637.78", "62680.00")
            failed_open.lighter_fill_price = None
            failed_open.hedge_status = "error"
            failed_open.hedge_error = "Cannot connect to host"
            async with runtime._record_lock:
                runtime.records = {"open": failed_open}
                runtime.record_order.extend(["open"])

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            return console.export_text()

        text = asyncio.run(render_text())

        self.assertIn("Lighter 对冲失败", text)
        self.assertIn("持仓与盈亏评估", text)
        self.assertIn("状态", text)
        self.assertIn("方向", text)
        self.assertIn("持仓时间", text)
        self.assertIn("平仓倒计时", text)
        self.assertIn("开仓金额", text)
        self.assertIn("Var 开仓价", text)
        self.assertIn("Lighter 开仓价", text)
        self.assertIn("开仓收益", text)
        self.assertIn("此时平仓磨损", text)

    def test_dashboard_round_estimate_is_net_of_visible_close_reserve(self) -> None:
        async def render_text() -> str:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.ticker = "BTC"
            runtime.variational_ticker = "BTC"
            open_record = make_record("net-open", "buy", "2", "100", "100.05")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            async with runtime._record_lock:
                runtime.records = {open_record.trade_key: open_record}
                runtime.record_order.append(open_record.trade_key)
            close_candidate = CloseCandidate(
                close_direction=Side.SELL,
                frame_captured_at_ms=time.time_ns() // 1_000_000,
                frozen_epoch_id="net-epoch",
                held_seconds=1,
                actual_close_rate=Decimal("-0.00035"),
                regression_target_rate=Decimal("-1"),
                expected_close_pnl_usd=Decimal("-0.07"),
                close_reserve_usd=Decimal("0.01"),
                round_lower_bound_usd=Decimal("0.02"),
                required_floor_usd=Decimal("0"),
                regression_passed=True,
                max_hold_alert=False,
            )
            runtime.last_strategy_decision = main_module.StrategyDecision(
                main_module.StrategyAction.NO_ACTION,
                "close_floor_not_met",
                close_candidate=close_candidate,
            )
            runtime.current_adaptive_market_frame = AsyncMock(
                return_value=(None, {"valid": False})
            )
            runtime.get_variational_best_bid_ask = AsyncMock(
                return_value=(Decimal("100"), Decimal("100.1"), "BTC")
            )
            runtime.get_lighter_best_bid_ask = AsyncMock(
                return_value=(Decimal("100.05"), Decimal("100.15"))
            )
            runtime.get_variational_quote_age_ms = AsyncMock(return_value=10)
            runtime.get_lighter_quote_age_ms = AsyncMock(return_value=10)

            console = Console(record=True, width=177, file=io.StringIO())
            console.print(await runtime.render_dashboard())
            return console.export_text()

        text = asyncio.run(render_text())
        self.assertIn("平仓预留", text)
        self.assertIn("-0.0100U", text)
        self.assertIn("当前整轮净估", text)
        self.assertIn("+0.0200U", text)

    def test_auto_order_pending_blocks_duplicate_var_opens_until_fill_or_timeout(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

            intent = runtime.mark_var_intent_sent("open", "BUY", Decimal("300"))
            intent.state = main_module.VAR_INTENT_COMMIT_AMBIGUOUS
            intent.firm_price = Decimal("1000")
            intent.firm_qty = Decimal("0.3")
            self.assertTrue(runtime.has_pending_var_intent())
            self.assertIsNone(await runtime._auto_var_signal_for_current_open(None))

            event = {
                "trade_id": "open-confirmed",
                "side": "buy",
                "qty": "0.3",
                "asset": "BTC",
                "status": "filled",
                "price": "1000",
                "timestamp": "2026-07-09T00:00:00Z",
            }
            runtime.accepted_assets = {"BTC"}
            await runtime.process_variational_trade_event(event)
            self.assertFalse(runtime.has_pending_var_intent())

        asyncio.run(run_case())

    def test_close_persists_final_runtime_state(self) -> None:
        async def run_case() -> None:
            class ClosingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.persist_count = 0

                async def persist_runtime_state(self) -> None:
                    self.persist_count += 1

            runtime = ClosingRuntime()

            async def stop_runtime() -> None:
                return None

            runtime.runtime.stop = stop_runtime
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))

            await runtime.close()

            self.assertEqual(runtime.persist_count, 1)

        asyncio.run(run_case())

    def test_opposite_var_fill_keeps_active_intent_and_requires_reconciliation(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=False, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "SELL", Decimal("300"))

            await runtime.process_variational_trade_event({
                "trade_id": "wrong-direction-fill",
                "side": "buy",
                "qty": "0.3",
                "asset": "BTC",
                "status": "filled",
                "price": "1000",
                "timestamp": "2026-07-09T00:00:00Z",
            })

            self.assertIsNotNone(runtime.pending_var_intent)
            self.assertTrue(runtime.automation_paused)
            self.assertIn("automatic intent was active", runtime.automation_pause_reason)
            self.assertEqual(runtime._reconcile_pause_reason, runtime.automation_pause_reason)

        asyncio.run(run_case())

    def test_lighter_hedge_error_pauses_automation(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class FailingHedgeRuntime(VariationalToLighterRuntime):
                async def place_lighter_order(self, record: OrderLifecycle) -> None:
                    async with self._record_lock:
                        record.hedge_status = "error"
                        record.hedge_error = "simulated hedge failure"
                        payload = record.to_payload()
                    await self.append_order_log("lighter_error", payload)
                    self.pause_automation("Lighter hedge failed: simulated hedge failure")

            runtime = FailingHedgeRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.accepted_assets = {"BTC"}
            event = {
                "trade_id": "hedge-fails",
                "side": "buy",
                "qty": "0.3",
                "asset": "BTC",
                "status": "filled",
                "price": "1000",
                "timestamp": "2026-07-09T00:00:00Z",
            }

            await runtime.process_variational_trade_event(event)
            if runtime.hedge_tasks:
                await asyncio.gather(*runtime.hedge_tasks)

            self.assertTrue(runtime.automation_paused)
            self.assertIn("Lighter hedge failed", runtime.automation_pause_reason)
            current_open = await runtime._current_open_record()
            self.assertIsNotNone(current_open)
            self.assertIsNone(
                await runtime._auto_var_close_signal_for_current_open(current_open)
            )

        asyncio.run(run_case())

    def test_manual_var_close_skips_lighter_when_open_hedge_failed(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "error"
                    record.hedge_error = "simulated open hedge failure"

            runtime = TrackingRuntime()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "open-failed-hedge",
                    "side": "buy",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60000",
                    "timestamp": "2026-07-09T00:00:00Z",
                }
            )
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "manual-close-after-failed-hedge",
                    "side": "sell",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60010",
                    "timestamp": "2026-07-09T00:01:00Z",
                }
            )

            self.assertEqual(runtime.scheduled, ["id:open-failed-hedge"])
            close_record = runtime.records["id:manual-close-after-failed-hedge"]
            self.assertTrue(close_record.lighter_reduce_only)
            self.assertEqual(close_record.hedge_status, "skipped")
            self.assertIn("matching open Lighter hedge", close_record.hedge_error or "")
            self.assertTrue(runtime.automation_paused)

        asyncio.run(run_case())

    def test_manual_var_close_hedges_when_open_hedge_filled(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "filled"
                    record.lighter_fill_price = Decimal("60020")

            runtime = TrackingRuntime()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "open-filled-hedge",
                    "side": "buy",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60000",
                    "timestamp": "2026-07-09T00:00:00Z",
                }
            )
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "manual-close-after-filled-hedge",
                    "side": "sell",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60010",
                    "timestamp": "2026-07-09T00:01:00Z",
                }
            )

            self.assertEqual(
                runtime.scheduled,
                ["id:open-filled-hedge", "id:manual-close-after-filled-hedge"],
            )
            self.assertFalse(runtime.records["id:open-filled-hedge"].lighter_reduce_only)
            self.assertTrue(runtime.records["id:manual-close-after-filled-hedge"].lighter_reduce_only)

        asyncio.run(run_case())

    def test_late_open_hedge_fill_schedules_reduce_only_close(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    if record.side == "buy":
                        record.hedge_status = "uncertain"
                        record.lighter_client_order_id = 12345
                        record.lighter_client_order_ids.append(12345)
                        self.lighter_client_order_to_trade_key[12345] = record.trade_key
                    else:
                        record.hedge_status = "queued"

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "late-open",
                    "side": "buy",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60000",
                    "timestamp": "2026-07-09T00:00:00Z",
                }
            )
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "close-before-open-hedge-fill",
                    "side": "sell",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60010",
                    "timestamp": "2026-07-09T00:01:00Z",
                }
            )

            close_record = runtime.records["id:close-before-open-hedge-fill"]
            self.assertEqual(close_record.hedge_status, "waiting_open_hedge")
            self.assertTrue(close_record.lighter_reduce_only)
            self.assertEqual(runtime.scheduled, ["id:late-open"])

            await runtime.handle_lighter_fill_update(
                {
                    "status": "filled",
                    "client_order_id": 12345,
                    "filled_quote_amount": "180",
                    "filled_base_amount": "0.003",
                }
            )

            self.assertEqual(
                runtime.scheduled,
                ["id:late-open", "id:close-before-open-hedge-fill"],
            )

        asyncio.run(run_case())

    def test_queued_open_hedge_without_order_id_blocks_close_hedge(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"
                    return True

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "queued-open-no-id",
                    "side": "buy",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60000",
                }
            )
            open_record = runtime.records["id:queued-open-no-id"]
            self.assertEqual(open_record.hedge_status, "queued")
            self.assertIsNone(open_record.lighter_client_order_id)

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "close-while-open-queued",
                    "side": "sell",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60010",
                }
            )

            close_record = runtime.records["id:close-while-open-queued"]
            self.assertEqual(close_record.hedge_status, "waiting_open_hedge")
            self.assertEqual(runtime.scheduled, ["id:queued-open-no-id"])
            self.assertTrue(runtime.automation_paused)

        asyncio.run(run_case())

    def test_confirmed_unfilled_lighter_error_does_not_send_protective_close(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    if record.side == "buy":
                        record.hedge_status = "error"
                        record.lighter_client_order_id = 67890
                        self.lighter_client_order_to_trade_key[67890] = record.trade_key
                    else:
                        record.hedge_status = "queued"

            runtime = TrackingRuntime()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "error-with-order-id",
                    "side": "buy",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60000",
                    "timestamp": "2026-07-09T00:00:00Z",
                }
            )
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "protective-close",
                    "side": "sell",
                    "qty": "0.003",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "60010",
                    "timestamp": "2026-07-09T00:01:00Z",
                }
            )

            close_record = runtime.records["id:protective-close"]
            self.assertTrue(close_record.lighter_reduce_only)
            self.assertEqual(
                runtime.scheduled,
                ["id:error-with-order-id"],
            )

        asyncio.run(run_case())

    def test_partial_open_hedge_protective_close_targets_only_confirmed_residual(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.accepted_assets = {"BTC"}
                    self.scheduled: list[tuple[str, Decimal | None]] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(
                        (record.trade_key, record.lighter_target_qty_override)
                    )
                    if record.side == "buy":
                        record.hedge_status = "partial"
                        record.lighter_client_order_id = 67901
                        record.lighter_client_order_ids = [67901]
                        record.lighter_filled_qty = Decimal("0.0012")
                        record.lighter_filled_quote = Decimal("72.024")
                        record.lighter_fill_price = Decimal("60020")
                        record.lighter_outcome_final = True
                    else:
                        record.hedge_status = "queued"
                    return True

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "partial-open-protective",
                    "side": "buy",
                    "qty": "0.0032",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62500",
                    "timestamp": "2026-07-09T00:00:00Z",
                }
            )
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "full-var-close-after-partial-hedge",
                    "side": "sell",
                    "qty": "0.0032",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "62510",
                    "timestamp": "2026-07-09T00:01:00Z",
                }
            )

            close_record = runtime.records["id:full-var-close-after-partial-hedge"]
            self.assertTrue(close_record.lighter_reduce_only)
            self.assertEqual(
                close_record.lighter_target_qty_override,
                Decimal("0.0012"),
            )
            self.assertEqual(
                main_module.lighter_order_target_qty(
                    close_record,
                    runtime.base_amount_multiplier,
                ),
                Decimal("0.0012"),
            )
            self.assertEqual(
                runtime.scheduled,
                [
                    ("id:partial-open-protective", None),
                    ("id:full-var-close-after-partial-hedge", Decimal("0.0012")),
                ],
            )

            restored = OrderLifecycle.from_payload(close_record.to_payload())
            self.assertIsNotNone(restored)
            assert restored is not None
            self.assertEqual(restored.lighter_target_qty_override, Decimal("0.0012"))

        asyncio.run(run_case())

    def test_lighter_reduce_only_flag_is_sent_for_close_hedge(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class FakeLighterClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def create_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return (
                        None,
                        SimpleNamespace(
                            code=200,
                            message={"ratelimit": "didn't use volume quota"},
                            tx_hash="0xtest",
                        ),
                        None,
                    )

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            fake_client = FakeLighterClient()
            runtime.lighter_client = fake_client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("60000")
                runtime.lighter_best_ask = Decimal("60001")
                runtime.lighter_order_book = {
                    "bids": {Decimal("60000"): Decimal("1")},
                    "asks": {Decimal("60001"): Decimal("1")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_sequence_gap = True
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic() - 10

            record = make_record("close", "sell", "0.003", "60010", "60020")
            record.lighter_reduce_only = True
            await runtime.place_lighter_order(record)

            self.assertEqual(len(fake_client.calls), 1)
            self.assertTrue(fake_client.calls[0]["reduce_only"])
            self.assertEqual(fake_client.calls[0]["price"], 1_200_020)
            self.assertEqual(record.hedge_status, "submitted")
            self.assertEqual(record.lighter_tx_hash, "0xtest")

        asyncio.run(run_case())

    def test_lighter_fill_arriving_before_sendtx_response_stays_filled(self) -> None:
        async def run_case() -> None:
            class EarlyFillClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.runtime: VariationalToLighterRuntime | None = None

                async def create_order(self, **kwargs):
                    assert self.runtime is not None
                    await self.runtime.handle_lighter_fill_update(
                        {
                            "client_order_id": str(kwargs["client_order_index"]),
                            "status": "filled",
                            "filled_base_amount": "2",
                            "filled_quote_amount": "200.2",
                        }
                    )
                    return None, SimpleNamespace(code=200, message={}, tx_hash="0xearly"), None

            class NoDiskRuntime(VariationalToLighterRuntime):
                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = NoDiskRuntime(Namespace(auto_hedge=True, lang="zh"))
            fake_client = EarlyFillClient()
            fake_client.runtime = runtime
            runtime.lighter_client = fake_client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("100")
                runtime.lighter_best_ask = Decimal("100.1")
                runtime.lighter_order_book = {
                    "bids": {Decimal("100"): Decimal("3")},
                    "asks": {Decimal("100.1"): Decimal("3")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            record = make_record("ws-before-http", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            await runtime.place_lighter_order(record)

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(record.lighter_filled_qty, Decimal("2"))
            self.assertEqual(record.lighter_fill_price, Decimal("100.1"))

        asyncio.run(run_case())

    def test_lighter_fill_wins_over_late_sendtx_exception(self) -> None:
        async def run_case() -> None:
            class EarlyFillThenErrorClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.runtime: VariationalToLighterRuntime | None = None

                async def create_order(self, **kwargs):
                    assert self.runtime is not None
                    await self.runtime.handle_lighter_fill_update(
                        {
                            "client_order_id": str(kwargs["client_order_index"]),
                            "status": "filled",
                            "filled_base_amount": "2",
                            "filled_quote_amount": "200.2",
                        }
                    )
                    raise RuntimeError("sendTx connection dropped")

            class NoDiskRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.emergency_closes = 0

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

                async def emergency_flatten_var(self, record: OrderLifecycle) -> None:
                    self.emergency_closes += 1

            runtime = NoDiskRuntime()
            fake_client = EarlyFillThenErrorClient()
            fake_client.runtime = runtime
            runtime.lighter_client = fake_client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("100")
                runtime.lighter_best_ask = Decimal("100.1")
                runtime.lighter_order_book = {
                    "bids": {Decimal("100"): Decimal("3")},
                    "asks": {Decimal("100.1"): Decimal("3")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            record = make_record("ws-before-error", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            await runtime.place_lighter_order(record)

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(record.lighter_filled_qty, Decimal("2"))
            self.assertEqual(runtime.emergency_closes, 0)

        asyncio.run(run_case())

    def test_late_fill_between_remaining_calc_and_send_prevents_new_ioc(self) -> None:
        async def run_case() -> None:
            class CaptureClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def create_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return None, SimpleNamespace(code=200, message={}, tx_hash="0xlate"), None

            class HookedLock:
                def __init__(self, hook) -> None:
                    self._lock = asyncio.Lock()
                    self._hook = hook
                    self._entries = 0

                async def __aenter__(self):
                    self._entries += 1
                    if self._entries == 3:
                        self._hook()
                    await self._lock.acquire()
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> None:
                    self._lock.release()

            class NoDiskRuntime(VariationalToLighterRuntime):
                async def persist_runtime_state(self) -> None:
                    return None

            runtime = NoDiskRuntime(Namespace(auto_hedge=True, lang="zh"))
            fake_client = CaptureClient()
            runtime.lighter_client = fake_client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("100")
                runtime.lighter_best_ask = Decimal("100.1")
                runtime.lighter_order_book = {
                    "bids": {Decimal("100"): Decimal("3")},
                    "asks": {Decimal("100.1"): Decimal("3")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            record = make_record("fill-before-register", "buy", "2", "100", "100.1")
            record.lighter_fill_price = Decimal("100.1")
            record.lighter_filled_qty = Decimal("1")
            record.lighter_filled_quote = Decimal("100.1")
            record.lighter_client_order_ids = [501]
            record.hedge_status = "partial"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            def complete_old_ioc() -> None:
                record.lighter_filled_qty = Decimal("2")
                record.lighter_filled_quote = Decimal("200.2")
                record.lighter_fill_price = Decimal("100.1")
                record.hedge_status = "filled"
                record.hedge_error = None

            runtime._record_lock = HookedLock(complete_old_ioc)

            await runtime.place_lighter_order(record)

            self.assertEqual(fake_client.calls, [])
            self.assertEqual(record.hedge_status, "filled")

        asyncio.run(run_case())

    def test_late_partial_fill_reduces_ioc_size_at_signer_boundary(self) -> None:
        async def run_case() -> None:
            class CaptureClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def create_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return None, SimpleNamespace(code=200, message={}, tx_hash="0xpartial"), None

            class HookedLock:
                def __init__(self, hook) -> None:
                    self._lock = asyncio.Lock()
                    self._hook = hook
                    self._entries = 0

                async def __aenter__(self):
                    self._entries += 1
                    if self._entries == 3:
                        self._hook()
                    await self._lock.acquire()
                    return self

                async def __aexit__(self, exc_type, exc, tb) -> None:
                    self._lock.release()

            class NoDiskRuntime(VariationalToLighterRuntime):
                async def persist_runtime_state(self) -> None:
                    return None

            runtime = NoDiskRuntime(Namespace(auto_hedge=True, lang="zh"))
            fake_client = CaptureClient()
            runtime.lighter_client = fake_client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("100")
                runtime.lighter_best_ask = Decimal("100.1")
                runtime.lighter_order_book = {
                    "bids": {Decimal("100"): Decimal("3")},
                    "asks": {Decimal("100.1"): Decimal("3")},
                }
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_nonce = 1
                runtime.lighter_book_received_monotonic = time.monotonic()

            record = make_record("partial-before-send", "buy", "2", "100", "100.1")
            record.lighter_fill_price = Decimal("100.1")
            record.lighter_filled_qty = Decimal("1")
            record.lighter_filled_quote = Decimal("100.1")
            record.lighter_client_order_ids = [502]
            record.hedge_status = "partial"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            def add_late_partial_fill() -> None:
                record.lighter_filled_qty = Decimal("1.5")
                record.lighter_filled_quote = Decimal("150.15")
                record.lighter_fill_price = Decimal("100.1")
                record.hedge_status = "partial"

            runtime._record_lock = HookedLock(add_late_partial_fill)

            await runtime.place_lighter_order(record)

            self.assertEqual(len(fake_client.calls), 1)
            self.assertEqual(fake_client.calls[0]["base_amount"], 500_000)

        asyncio.run(run_case())

    def test_late_fill_wins_when_order_book_lookup_then_fails(self) -> None:
        async def run_case() -> None:
            class LateFillRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.emergency_closes = 0

                async def capture_lighter_hedge_dispatch_snapshot(self, **_kwargs):
                    # The snapshot runs under _record_lock so the simulated
                    # private fill must update the same locked state directly.
                    record.lighter_filled_qty = Decimal("2")
                    record.lighter_filled_quote = Decimal("200.2")
                    record.lighter_fill_price = Decimal("100.1")
                    record.hedge_status = "filled"
                    return None, "Lighter depth unavailable"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

                async def emergency_flatten_var(self, record: OrderLifecycle) -> None:
                    self.emergency_closes += 1

            runtime = LateFillRuntime()
            runtime.base_amount_multiplier = Decimal("1000000")
            record = make_record("fill-before-book-failure", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            record.lighter_client_order_ids = [601]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[601] = record.trade_key

            await runtime.place_lighter_order(record)

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(runtime.emergency_closes, 0)

        asyncio.run(run_case())

    def test_prepared_lighter_client_order_id_sends_before_post_commit_persist(self) -> None:
        async def run_case() -> None:
            events: list[str] = []

            class FakeLighterClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                def __init__(self) -> None:
                    self.orders: list[dict] = []

                async def create_order(self, **kwargs):
                    self.orders.append(kwargs)
                    events.append("sendtx")
                    return None, SimpleNamespace(code=200, message={}, tx_hash="0xtest"), None

            class WriteAheadRuntime(VariationalToLighterRuntime):
                async def persist_runtime_state(self) -> None:
                    events.append("persist")

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

            runtime = WriteAheadRuntime(Namespace(auto_hedge=True, lang="zh"))
            client = FakeLighterClient()
            runtime.lighter_client = client
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = 1_000_000
            runtime.price_multiplier = 10
            runtime.lighter_best_bid = Decimal("60000")
            runtime.lighter_best_ask = Decimal("60001")
            runtime.lighter_order_book = {
                "bids": {Decimal("60000"): Decimal("1")},
                "asks": {Decimal("60001"): Decimal("1")},
            }
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_book_received_monotonic = time.monotonic()
            record = make_record("lighter-write-ahead", "buy", "0.003", "60000", "60001")
            record.firm_quote_id = "firm-deterministic-id"
            record.lighter_reserved_client_order_id = 424242
            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic(),
                market="BTC",
                state="VAR_COMMITTED",
                firm_quote_id="firm-deterministic-id",
                lighter_client_order_index=424242,
            )

            await runtime.place_lighter_order(record)

            self.assertGreaterEqual(len(events), 2)
            self.assertEqual(events[:2], ["sendtx", "persist"])
            self.assertEqual(record.lighter_client_order_id, 424242)
            self.assertEqual(client.orders[0]["client_order_index"], 424242)
            self.assertEqual(record.execution_state, "HEDGE_SUBMITTED")

        asyncio.run(run_case())

    def test_firm_guard_prepares_durable_intent_with_deterministic_lighter_id(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            candidate = make_open_candidate(runtime)
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))

            prepared = await runtime.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="trace-prepared-intent",
                firm_quote={
                    "quoteId": "firm-prepared-1",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "guardPnl": "0.80",
                    "guardMinPnl": "0.40",
                    "executionReserveUsd": "0.02",
                    "lighterVwap": "100.10",
                    "lighterQuoteAgeMs": 12,
                    "adaptiveStrategy": open_candidate_to_payload(candidate),
                    "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                },
            )

            self.assertIsNotNone(prepared)
            self.assertEqual(prepared.state, "PREPARED")
            self.assertEqual(prepared.trace_id, "trace-prepared-intent")
            self.assertIsNotNone(prepared.lighter_client_order_index)
            self.assertGreater(prepared.lighter_client_order_index, 0)
            self.assertLess(prepared.lighter_client_order_index, 1 << 48)

            persisted = json.loads(main_module.RUNTIME_STATE_FILE.read_text(encoding="utf-8"))
            raw_intent = persisted["pending_var_intent"]
            self.assertEqual(raw_intent["state"], "PREPARED")
            self.assertEqual(raw_intent["firm_quote_id"], "firm-prepared-1")
            self.assertEqual(
                raw_intent["lighter_client_order_index"],
                prepared.lighter_client_order_index,
            )

        asyncio.run(run_case())

    def test_deterministic_lighter_client_order_index_is_stable_and_uint48(self) -> None:
        first = main_module.deterministic_lighter_client_order_index(
            account_index=1,
            market="BTC",
            firm_quote_id="firm-deterministic",
            phase="open",
            side="BUY",
        )
        second = main_module.deterministic_lighter_client_order_index(
            account_index=1,
            market="BTC",
            firm_quote_id="firm-deterministic",
            phase="open",
            side="BUY",
        )
        retry = main_module.deterministic_lighter_client_order_index(
            account_index=1,
            market="BTC",
            firm_quote_id="firm-deterministic",
            phase="open",
            side="BUY",
            attempt=1,
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, retry)
        self.assertGreater(first, 0)
        self.assertLess(first, 1 << 48)

    def test_startup_restores_prepared_intent_for_reconciliation(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            candidate = make_open_candidate(runtime)
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            prepared = await runtime.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="trace-recovery-prepared",
                firm_quote={
                    "quoteId": "firm-recovery-1",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "guardPnl": "0.80",
                    "guardMinPnl": "0.40",
                    "executionReserveUsd": "0.02",
                    "lighterVwap": "100.10",
                    "lighterQuoteAgeMs": 12,
                    "adaptiveStrategy": open_candidate_to_payload(candidate),
                    "strategyTag": main_module.ADAPTIVE_MODEL_VERSION,
                },
            )

            recovered = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            recovered.variational_ticker = "BTC"
            self.assertTrue(await recovered.load_runtime_state("BTC"))
            self.assertIsNotNone(recovered.pending_var_intent)
            self.assertEqual(recovered.pending_var_intent.state, "PREPARED")
            self.assertEqual(recovered.pending_var_intent.firm_quote_id, "firm-recovery-1")
            self.assertEqual(
                recovered.pending_var_intent.lighter_client_order_index,
                prepared.lighter_client_order_index,
            )
            self.assertTrue(recovered.automation_paused)

        asyncio.run(run_case())

    def test_recovered_prepared_intent_checks_lighter_before_sending_same_id(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record)
                    record.hedge_status = "queued"
                    return True

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            class EmptyOrderApi:
                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

                async def account_inactive_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = EmptyOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic(),
                market="BTC",
                state="PREPARED",
                trace_id="trace-recover-check",
                firm_quote_id="firm-recover-check",
                firm_price=Decimal("100"),
                firm_qty=Decimal("2"),
                lighter_client_order_index=777777,
            )
            runtime.lighter_client = FakeLighterClient()

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "portfolio-recovered",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "recovered_from_portfolio": True,
                }
            )

            record = runtime.records["id:portfolio-recovered"]
            self.assertEqual(record.hedge_status, "recovery_check")
            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(runtime.lighter_client_order_to_trade_key[777777], record.trade_key)

            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(runtime.scheduled, [record])
            self.assertEqual(record.lighter_reserved_client_order_id, 777777)
            self.assertEqual(record.lighter_client_order_ids, [])

        asyncio.run(run_case())

    def test_late_portfolio_recovery_does_not_overwrite_terminal_lighter_fill(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.logged_events: list[str] = []

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    self.logged_events.append(event_type)

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.base_amount_multiplier = 100_000

            record = make_record(
                "commit:terminal-before-portfolio",
                "sell",
                "0.003091",
                "64771.24",
                "64817.8",
            )
            record.last_variational_status = "accepted"
            record.var_fill_source = "http_commit"
            record.var_fill_ts_iso = main_module.utc_now()
            record.lighter_reserved_client_order_id = 151029027838852
            record.lighter_client_order_id = 151029027838852
            record.lighter_client_order_ids = [151029027838852]
            record.lighter_filled_qty = Decimal("0.00309")
            record.lighter_filled_quote = Decimal("200.287002")
            record.lighter_outcome_final = True
            record.hedge_status = "filled"
            record.execution_state = main_module.EXECUTION_STATE_HEDGED
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_order_fill_totals[151029027838852] = (
                Decimal("0.00309"),
                Decimal("200.287002"),
            )
            runtime.lighter_order_terminal_ids.add(151029027838852)
            runtime.lighter_client_order_to_trade_key[151029027838852] = (
                record.trade_key
            )

            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="SELL",
                amount=Decimal("200.20790284"),
                sent_monotonic=time.monotonic(),
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTED,
                trace_id="late-portfolio-recovery",
                firm_quote_id="firm-late-portfolio-recovery",
                firm_price=Decimal("64773.9"),
                firm_qty=Decimal("0.003091"),
                lighter_client_order_index=151029027838852,
                provisional_trade_key=record.trade_key,
            )

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "portfolio-open-late",
                    "side": "sell",
                    "qty": "0.003091",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "64771.24",
                    "timestamp": main_module.utc_now(),
                    "recovered_from_portfolio": True,
                }
            )

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(
                record.execution_state,
                main_module.EXECUTION_STATE_HEDGED,
            )
            self.assertIsNone(record.hedge_error)
            self.assertNotIn("lighter_recovery_check", runtime.logged_events)

        asyncio.run(run_case())

    def test_refresh_uses_terminal_fill_without_rest_reconciliation(self) -> None:
        async def run_case() -> None:
            class NoDiskRuntime(VariationalToLighterRuntime):
                async def persist_runtime_state(self) -> None:
                    return None

            runtime = NoDiskRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = 100_000
            runtime.lighter_client = object()
            record = make_record(
                "terminal-recovery-check",
                "sell",
                "0.003091",
                "64771.24",
                "64817.8",
            )
            record.lighter_reserved_client_order_id = 123456
            record.lighter_client_order_id = 123456
            record.lighter_client_order_ids = [123456]
            record.lighter_filled_qty = Decimal("0.00309")
            record.lighter_filled_quote = Decimal("200.287002")
            record.lighter_outcome_final = True
            record.hedge_status = "recovery_check"
            record.execution_state = main_module.EXECUTION_STATE_RECOVERY_REQUIRED
            record.hedge_error = "stale recovery state"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            async def unexpected_rest_query(_pending_ids):
                raise AssertionError("terminal local fill must not query Lighter REST")

            runtime._fetch_lighter_orders_for_reconciliation = unexpected_rest_query

            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(
                record.execution_state,
                main_module.EXECUTION_STATE_HEDGED,
            )
            self.assertIsNone(record.hedge_error)

        asyncio.run(run_case())

    def test_restart_normalizes_persisted_terminal_recovery_check(self) -> None:
        async def run_case() -> None:
            source = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            source.variational_ticker = "BTC"
            source.ticker = "BTC"
            source.base_amount_multiplier = 100_000
            record = make_record(
                "persisted-terminal-recovery",
                "sell",
                "0.003091",
                "64771.24",
                "64817.8",
            )
            record.lighter_reserved_client_order_id = 654321
            record.lighter_client_order_id = 654321
            record.lighter_client_order_ids = [654321]
            record.lighter_filled_qty = Decimal("0.00309")
            record.lighter_filled_quote = Decimal("200.287002")
            record.lighter_outcome_final = True
            record.hedge_status = "recovery_check"
            record.execution_state = main_module.EXECUTION_STATE_RECOVERY_REQUIRED
            record.hedge_error = "stale recovery state"
            source.records[record.trade_key] = record
            source.record_order.append(record.trade_key)
            source.lighter_order_fill_totals[654321] = (
                Decimal("0.00309"),
                Decimal("200.287002"),
            )
            source.lighter_order_terminal_ids.add(654321)

            with tempfile.TemporaryDirectory(prefix="terminal-recovery-") as tmp:
                state_file = Path(tmp) / "runtime_state.json"
                with patch.object(main_module, "RUNTIME_STATE_FILE", state_file):
                    await source.persist_runtime_state()
                    restored = VariationalToLighterRuntime(
                        Namespace(auto_hedge=True, lang="zh")
                    )
                    restored.variational_ticker = "BTC"
                    restored.ticker = "BTC"
                    restored.base_amount_multiplier = 100_000
                    self.assertTrue(await restored.load_runtime_state("BTC"))

            restored_record = restored.records[record.trade_key]
            self.assertEqual(restored_record.hedge_status, "filled")
            self.assertEqual(
                restored_record.execution_state,
                main_module.EXECUTION_STATE_HEDGED,
            )
            self.assertIsNone(restored_record.hedge_error)

        asyncio.run(run_case())

    def test_quantity_close_fill_correlates_to_firm_notional_after_price_move(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.variational_ticker = "BTC"
        intent = main_module.VarOrderIntent(
            phase="close",
            side="SELL",
            amount=Decimal("200"),
            sent_monotonic=time.monotonic(),
            market="BTC",
            state="VAR_COMMITTED",
            firm_quote_id="firm-close-price-move",
            firm_price=Decimal("105.10"),
            firm_qty=Decimal("2"),
        )

        self.assertTrue(
            runtime.var_event_matches_intent(
                intent,
                {
                    "trade_id": "close-after-price-move",
                    "side": "sell",
                    "qty": "2",
                    "price": "105.10",
                    "asset": "BTC",
                },
            )
        )

    def test_recovery_check_adopts_existing_lighter_order_without_resending(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("recovery-existing-order", "buy", "2", "100", "100.1")
            record.firm_quote_id = "firm-recovery-existing"
            record.lighter_reserved_client_order_id = 888888
            record.hedge_status = "recovery_check"
            record.execution_state = "RECOVERY_REQUIRED"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[888888] = record.trade_key

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "888888",
                    "status": "open",
                    "filled_base_amount": "0",
                    "filled_quote_amount": "0",
                }
            )

            self.assertEqual(record.lighter_client_order_ids, [888888])
            self.assertEqual(record.hedge_status, "submitted")
            self.assertEqual(record.execution_state, "HEDGE_SUBMITTED")

        asyncio.run(run_case())

    def test_prepared_crash_recovery_adopts_existing_lighter_order_without_resend(self) -> None:
        async def run_case() -> None:
            reserved_id = 919191

            class ExistingOrderApi:
                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(
                        orders=[
                            {
                                "client_order_id": str(reserved_id),
                                "status": "open",
                                "filled_base_amount": "0",
                                "filled_quote_amount": "0",
                            }
                        ]
                    )

                async def account_inactive_orders(self, **_kwargs):
                    raise AssertionError(
                        "active deterministic order should end the history search"
                    )

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = ExistingOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic(),
                market="BTC",
                state="PREPARED",
                trace_id="crash-after-lighter-send",
                firm_quote_id="firm-crash-after-lighter-send",
                firm_price=Decimal("100"),
                firm_qty=Decimal("2"),
                lighter_client_order_index=reserved_id,
            )
            runtime.lighter_client = FakeLighterClient()

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "portfolio-crash-recovery",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "recovered_from_portfolio": True,
                }
            )
            record = runtime.records["id:portfolio-crash-recovery"]
            self.assertEqual(record.hedge_status, "recovery_check")

            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(record.lighter_client_order_ids, [reserved_id])
            self.assertEqual(record.hedge_status, "submitted")
            self.assertEqual(record.execution_state, "HEDGE_SUBMITTED")

        asyncio.run(run_case())

    def test_lighter_ioc_cancel_reason_retries_remaining_quantity(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = Decimal("1000000")
            record = make_record("ioc-cancel", "buy", "0.003", "60000", "60001")
            record.lighter_fill_price = None
            record.lighter_client_order_id = 123
            record.lighter_client_order_ids = [123]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[123] = record.trade_key

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "123",
                    "status": "canceled-not-enough-liquidity",
                    "filled_base_amount": "0.001",
                    "filled_quote_amount": "60",
                }
            )

            self.assertEqual(record.lighter_filled_qty, Decimal("0.001"))
            self.assertEqual(runtime.scheduled, ["ioc-cancel"])
            self.assertEqual(record.hedge_status, "queued")

        asyncio.run(run_case())

    def test_multiple_ioc_terminal_updates_are_aggregated_before_retry(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("multi-ioc", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            record.lighter_client_order_ids = [201, 202]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key.update(
                {201: record.trade_key, 202: record.trade_key}
            )

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "201",
                    "status": "filled",
                    "filled_base_amount": "1",
                    "filled_quote_amount": "100.1",
                }
            )
            self.assertEqual(runtime.scheduled, [])

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "202",
                    "status": "filled",
                    "filled_base_amount": "1",
                    "filled_quote_amount": "100.2",
                }
            )

            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(record.lighter_filled_qty, Decimal("2"))

        asyncio.run(run_case())

    def test_late_terminal_fill_update_does_not_queue_duplicate_retry(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("retry-latch", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            record.lighter_client_order_ids = [301, 302]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key.update(
                {301: record.trade_key, 302: record.trade_key}
            )

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "301",
                    "status": "filled",
                    "filled_base_amount": "1",
                    "filled_quote_amount": "100.1",
                }
            )
            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "302",
                    "status": "canceled-not-enough-liquidity",
                    "filled_base_amount": "0",
                    "filled_quote_amount": "0",
                }
            )
            self.assertEqual(runtime.scheduled, ["retry-latch"])

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "301",
                    "status": "filled",
                    "filled_base_amount": "1.5",
                    "filled_quote_amount": "150.15",
                }
            )

            self.assertEqual(runtime.scheduled, ["retry-latch"])

        asyncio.run(run_case())

    def test_retry_is_rechecked_when_late_fill_completes_during_persist(self) -> None:
        async def run_case() -> None:
            class GatedRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []
                    self.block_partial_log = False
                    self.partial_log_entered = asyncio.Event()
                    self.release_partial_log = asyncio.Event()

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record.trade_key)
                    record.hedge_status = "queued"

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    if self.block_partial_log and event_type == "lighter_partial":
                        self.partial_log_entered.set()
                        await self.release_partial_log.wait()

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = GatedRuntime()
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("late-fill-before-retry", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            record.lighter_client_order_ids = [401, 402]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key.update(
                {401: record.trade_key, 402: record.trade_key}
            )

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "401",
                    "status": "filled",
                    "filled_base_amount": "1",
                    "filled_quote_amount": "100.1",
                }
            )

            runtime.block_partial_log = True
            stale_retry = asyncio.create_task(
                runtime.handle_lighter_fill_update(
                    {
                        "client_order_id": "402",
                        "status": "canceled-not-enough-liquidity",
                        "filled_base_amount": "0",
                        "filled_quote_amount": "0",
                    }
                )
            )
            await runtime.partial_log_entered.wait()

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "401",
                    "status": "filled",
                    "filled_base_amount": "2",
                    "filled_quote_amount": "200.2",
                }
            )
            runtime.release_partial_log.set()
            await stale_retry

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(runtime.scheduled, [])

        asyncio.run(run_case())

    def test_stale_smaller_terminal_total_cannot_degrade_full_lighter_fill(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("monotonic-full", "buy", "0.0032", "62500", "62501")
            record.lighter_fill_price = None
            record.lighter_client_order_id = 777
            record.lighter_client_order_ids = [777]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[777] = record.trade_key

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "777",
                    "status": "filled",
                    "filled_base_amount": "0.0032",
                    "filled_quote_amount": "200.0032",
                }
            )
            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "777",
                    "status": "canceled-not-enough-liquidity",
                    "filled_base_amount": "0.0031",
                    "filled_quote_amount": "193.7531",
                }
            )

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(record.lighter_filled_qty, Decimal("0.0032"))
            self.assertEqual(
                runtime.lighter_order_fill_totals[777],
                (Decimal("0.0032"), Decimal("200.0032")),
            )
            self.assertEqual(runtime.scheduled, [])

        asyncio.run(run_case())

    def test_lighter_scheduler_coalesces_requeue_without_concurrent_submission(self) -> None:
        async def run_case() -> None:
            class SequentialRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls = 0
                    self.active = 0
                    self.max_active = 0
                    self.first_started = asyncio.Event()
                    self.release_first = asyncio.Event()

                async def place_lighter_order(self, record: OrderLifecycle) -> None:
                    self.calls += 1
                    call_number = self.calls
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    try:
                        if call_number == 1:
                            self.first_started.set()
                            await self.release_first.wait()
                    finally:
                        self.active -= 1

            runtime = SequentialRuntime()
            runtime.base_amount_multiplier = 1
            record = make_record("coalesced-retry", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            runtime.schedule_lighter_order(record)
            await runtime.first_started.wait()

            runtime.queue_lighter_retry_after_current(record)
            await asyncio.sleep(0)
            runtime.release_first.set()
            await asyncio.gather(*list(runtime.hedge_tasks))

            self.assertEqual(runtime.calls, 2)
            self.assertEqual(runtime.max_active, 1)

        asyncio.run(run_case())

    def test_lighter_scheduler_ignores_duplicate_ensure_request(self) -> None:
        async def run_case() -> None:
            class BlockingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls = 0
                    self.started = asyncio.Event()
                    self.release = asyncio.Event()

                async def place_lighter_order(self, record: OrderLifecycle) -> None:
                    self.calls += 1
                    self.started.set()
                    await self.release.wait()

            runtime = BlockingRuntime()
            record = make_record("duplicate-ensure", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            self.assertTrue(runtime.schedule_lighter_order(record))
            await runtime.started.wait()

            self.assertFalse(runtime.schedule_lighter_order(record))
            runtime.release.set()
            await asyncio.gather(*list(runtime.hedge_tasks))

            self.assertEqual(runtime.calls, 1)

        asyncio.run(run_case())

    def test_late_ambiguous_ioc_fill_detects_and_corrects_overhedge(self) -> None:
        async def run_case() -> None:
            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("ambiguous-overfill", "buy", "2", "100", "100.1")
            record.lighter_fill_price = None
            record.lighter_client_order_ids = [101, 102]
            record.hedge_status = "submitted"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key.update(
                {101: record.trade_key, 102: record.trade_key}
            )

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "101",
                    "status": "filled",
                    "filled_base_amount": "2",
                    "filled_quote_amount": "200.2",
                }
            )
            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "102",
                    "status": "filled",
                    "filled_base_amount": "2",
                    "filled_quote_amount": "200.4",
                }
            )

            self.assertEqual(record.lighter_filled_qty, Decimal("4"))
            self.assertEqual(record.hedge_status, "overfilled")
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(len(runtime.scheduled), 1)
            correction = runtime.scheduled[0]
            self.assertEqual(correction.qty, Decimal("2"))
            self.assertEqual(correction.var_fill_source, "lighter_qty_correction")
            self.assertTrue(correction.lighter_reduce_only)

        asyncio.run(run_case())

    def test_reconciliation_expects_lighter_position_from_var_qty_not_overfill(self) -> None:
        async def run_case() -> None:
            class ReconcileRuntime(VariationalToLighterRuntime):
                async def get_variational_position(self, asset: str) -> Decimal:
                    return Decimal("2")

                async def get_lighter_account_snapshot(self):
                    return Decimal("-4"), 0

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = ReconcileRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000
            record = make_record("overfilled-position", "buy", "2", "100", "100.1")
            record.lighter_filled_qty = Decimal("4")
            record.hedge_status = "filled"
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            mark_fresh_variational_portfolio(runtime, qty=Decimal("2"))

            matched = await runtime.reconcile_accounts(allow_resume=True)

            self.assertFalse(matched)
            self.assertIn("Lighter -4/-2", runtime.last_reconcile_status)

        asyncio.run(run_case())

    def test_definitive_lighter_open_rejection_triggers_var_emergency_close(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

            class RejectingLighterClient:
                ORDER_TYPE_MARKET = 1
                ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 0
                DEFAULT_IOC_EXPIRY = 0

                async def create_order(self, **kwargs):
                    raise RuntimeError("not enough margin to create the order")

            class TrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.emergency_records: list[str] = []

                async def emergency_flatten_var(self, record: OrderLifecycle) -> None:
                    self.emergency_records.append(record.trade_key)

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = TrackingRuntime()
            runtime.lighter_client = RejectingLighterClient()
            runtime.lighter_market_index = 1
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.price_multiplier = Decimal("10")
            async with runtime.lighter_order_book_lock:
                runtime.lighter_best_bid = Decimal("60000")
                runtime.lighter_best_ask = Decimal("60001")
            record = make_record("rejected-open", "buy", "0.003", "60000", "60001")
            record.lighter_fill_price = None

            await runtime.place_lighter_order(record)

            self.assertEqual(record.hedge_status, "error")
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime.emergency_records, ["rejected-open"])

        asyncio.run(run_case())

    def test_pending_intent_can_be_cleared_by_exact_startup_reconciliation(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.mark_var_intent_sent("open", "BUY", Decimal("300"))
            reconcile_reason = "Recovered an unresolved Var order; account reconciliation required"
            runtime.pause_automation(reconcile_reason)
            runtime._reconcile_pause_reason = reconcile_reason

            async def get_var_position(_asset: str) -> Decimal:
                return Decimal("0")

            async def get_lighter_snapshot() -> tuple[Decimal, int]:
                return Decimal("0"), 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime)

            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertTrue(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_v2_manual_pause_survives_startup_reconciliation(self) -> None:
        async def run_case() -> None:
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.pause_automation("manual safety pause")
            await source.persist_runtime_state()
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000

            async def get_var_position(_asset: str) -> Decimal:
                return Decimal("0")

            async def get_lighter_snapshot():
                return Decimal("0"), 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime)

            self.assertTrue(await runtime.load_runtime_state("BTC"))
            self.assertTrue(runtime.automation_paused)
            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime.automation_pause_reason, "manual safety pause")

        asyncio.run(run_case())

    def test_restored_lighter_hedge_pause_clears_only_after_exact_flat_reconciliation(
        self,
    ) -> None:
        async def run_case() -> None:
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.pause_automation(
                "Lighter hedge failed: Lighter IOC canceled-too-much-slippage"
            )
            await source.persist_runtime_state()
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000
            positions = {"var": Decimal("0"), "lighter": Decimal("-0.00309")}

            async def get_var_position(_asset: str) -> Decimal:
                return positions["var"]

            async def get_lighter_snapshot():
                return positions["lighter"], 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime)

            self.assertTrue(await runtime.load_runtime_state("BTC"))
            self.assertTrue(runtime.automation_paused)
            self.assertFalse(await runtime.reconcile_accounts(allow_resume=True))
            self.assertTrue(runtime.automation_paused)

            positions["lighter"] = Decimal("0")
            mark_fresh_variational_portfolio(runtime)
            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(
                runtime.last_auto_var_order_status,
                "reconciled; cooldown 30s",
            )
            self.assertEqual(runtime.round_cooldown_remaining_seconds(), 30)

        asyncio.run(run_case())

    def test_v2_reconciliation_pause_resumes_after_clean_reconciliation(self) -> None:
        async def run_case() -> None:
            old_reason = (
                "Var fill matched the pending side/market but not its exact order metadata; "
                "hedging the confirmed fill and requiring reconciliation"
            )
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.pause_automation(old_reason)
            await source.persist_runtime_state()
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000

            async def get_var_position(_asset: str) -> Decimal:
                return Decimal("0")

            async def get_lighter_snapshot():
                return Decimal("0"), 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime)

            self.assertTrue(await runtime.load_runtime_state("BTC"))
            self.assertEqual(runtime._reconcile_pause_reason, old_reason)
            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_restored_unconfirmed_var_commit_pause_requires_exact_clean_reconciliation(
        self,
    ) -> None:
        async def run_case() -> None:
            reason = (
                "Var commit was accepted but its position/fill could not be confirmed; "
                "manual account reconciliation is required"
            )
            source = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            source.variational_ticker = "BTC"
            source.pause_for_reconciliation(reason)
            await source.persist_runtime_state()

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000
            positions = {"var": Decimal("0.003091"), "lighter": Decimal("0")}

            async def get_var_position(_asset: str) -> Decimal:
                return positions["var"]

            async def get_lighter_snapshot():
                return positions["lighter"], 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime, qty=positions["var"])

            self.assertTrue(await runtime.load_runtime_state("BTC"))
            self.assertEqual(runtime._reconcile_pause_reason, reason)
            self.assertFalse(await runtime.reconcile_accounts(allow_resume=True))
            self.assertTrue(runtime.automation_paused)

            positions.update(var=Decimal("0"), lighter=Decimal("0"))
            mark_fresh_variational_portfolio(runtime)
            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(runtime.automation_pause_reason, "-")

        asyncio.run(run_case())

    def test_pre_v2_pending_intent_is_rejected(self) -> None:
        async def run_case() -> None:
            main_module.RUNTIME_STATE_FILE.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "asset": "BTC",
                        "ticker": "BTC",
                        "records": [],
                        "pending_var_intent": {
                            "phase": "open",
                            "side": "BUY",
                            "amount": "200",
                            "market": "BTC",
                            "request_id": "pending-manual-pause",
                        },
                        "automation_paused": True,
                        "automation_pause_reason": "manual safety pause",
                    }
                ),
                encoding="utf-8",
            )
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1_000_000

            async def get_var_position(_asset: str) -> Decimal:
                return Decimal("0")

            async def get_lighter_snapshot():
                return Decimal("0"), 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime)

            with self.assertRaisesRegex(RuntimeError, "Pre-v2 runtime state"):
                await runtime.load_runtime_state("BTC")

        asyncio.run(run_case())

    def test_transient_account_settlement_mismatch_does_not_permanently_pause(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = Decimal("100000")
            positions = {"var": Decimal("0.00316"), "lighter": Decimal("-0.00316")}

            async def get_var_position(_asset: str) -> Decimal:
                return positions["var"]

            async def get_lighter_snapshot() -> tuple[Decimal, int]:
                return positions["lighter"], 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime, qty=positions["var"])

            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)

            positions.update(var=Decimal("0"), lighter=Decimal("0"))
            mark_fresh_variational_portfolio(runtime)
            self.assertTrue(await runtime.reconcile_accounts())
            self.assertTrue(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_reconciliation_pause_auto_clears_only_after_exact_recovery(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = Decimal("100000")
            positions = {"var": Decimal("0.00316"), "lighter": Decimal("-0.00316")}

            async def get_var_position(_asset: str) -> Decimal:
                return positions["var"]

            async def get_lighter_snapshot() -> tuple[Decimal, int]:
                return positions["lighter"], 0

            async def no_persist() -> None:
                return None

            runtime.get_variational_position = get_var_position
            runtime.get_lighter_account_snapshot = get_lighter_snapshot
            runtime.persist_runtime_state = no_persist
            mark_fresh_variational_portfolio(runtime, qty=positions["var"])

            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(runtime.automation_paused)
            assert runtime._reconcile_mismatch_first_monotonic is not None
            runtime._reconcile_mismatch_first_monotonic -= 5.1
            mark_fresh_variational_portfolio(runtime, qty=positions["var"])
            self.assertFalse(await runtime.reconcile_accounts())
            self.assertTrue(runtime.automation_paused)

            positions.update(var=Decimal("0"), lighter=Decimal("0"))
            mark_fresh_variational_portfolio(runtime)
            self.assertTrue(await runtime.reconcile_accounts())
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(
                runtime.last_auto_var_order_status,
                "reconciled; cooldown 30s",
            )
            self.assertEqual(runtime.round_cooldown_remaining_seconds(), 30)

            runtime.pause_automation("manual safety pause")
            self.assertTrue(await runtime.reconcile_accounts())
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime.automation_pause_reason, "manual safety pause")

            positions.update(var=Decimal("0.00316"), lighter=Decimal("-0.00316"))
            mark_fresh_variational_portfolio(runtime, qty=positions["var"])
            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(await runtime.reconcile_accounts())
            positions.update(var=Decimal("0"), lighter=Decimal("0"))
            mark_fresh_variational_portfolio(runtime)
            self.assertTrue(await runtime.reconcile_accounts())
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime.automation_pause_reason, "manual safety pause")

        asyncio.run(run_case())

    def test_reconcile_api_errors_do_not_take_over_manual_pause(self) -> None:
        async def run_case() -> None:
            class ErrorRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls = 0

                async def reconcile_accounts(self, *, allow_resume: bool = False) -> bool:
                    self.calls += 1
                    if self.calls <= 2:
                        raise RuntimeError("temporary account API failure")
                    self.stop_flag = True
                    return False

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = ErrorRuntime()
            runtime.strategy_config.reconcile_interval_seconds = 0.001
            runtime.pause_automation("manual safety pause")

            await runtime.reconcile_loop()

            self.assertTrue(runtime.automation_paused)
            self.assertEqual(runtime.automation_pause_reason, "manual safety pause")
            self.assertIsNone(runtime._reconcile_pause_reason)

        asyncio.run(run_case())

    def test_new_var_order_is_blocked_while_lighter_transition_is_active(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.automation_ready = True
            release = asyncio.Event()
            task = asyncio.create_task(release.wait())
            runtime.hedge_tasks.add(task)
            try:
                self.assertFalse(runtime.automation_can_submit_var_order("last_auto_var_order_status"))
                self.assertEqual(runtime.last_auto_var_order_status, "waiting Lighter hedge")
            finally:
                release.set()
                await task
            self.assertTrue(runtime.automation_can_submit_var_order("last_auto_var_order_status"))
            self.assertEqual(runtime.last_auto_var_order_status, "-")

        asyncio.run(run_case())

    def test_first_release_rejects_non_btc_asset_activation(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.ticker = "BTC"
            runtime.automation_ready = True

            with self.assertRaisesRegex(RuntimeError, "supports BTC only"):
                await runtime.activate_asset("XAU", "test")
            self.assertFalse(runtime._asset_switch_in_progress)
            self.assertEqual(runtime.variational_ticker, "BTC")

        asyncio.run(run_case())

    def test_auto_open_rechecks_asset_switch_gate_after_signal_calculation(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                async def extension_connected(self) -> bool:
                    return True

            class GateRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.requests = 0

                async def _auto_var_signal_for_current_open(self, _current_open):
                    self._selected_open_candidate = make_open_candidate(self)
                    self._asset_switch_in_progress = True
                    return "BUY", Decimal("0.20")

                def live_open_block_reason(self):
                    return None

                async def request_guarded_var_order(self, **kwargs):
                    self.requests += 1
                    return {"ok": False, "error": "must not run"}

            runtime = GateRuntime()
            runtime.runtime.command_broker = FakeBroker()
            runtime.variational_ticker = "BTC"
            runtime.automation_ready = True
            await runtime._evaluate_auto_open_once(None)

            self.assertEqual(runtime.requests, 0)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_auto_close_rechecks_asset_switch_gate_after_signal_calculation(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                async def extension_connected(self) -> bool:
                    return True

            class GateRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.requests = 0

                async def _auto_var_close_signal_for_current_open(self, _current_open):
                    self._asset_switch_in_progress = True
                    return (
                        "SELL",
                        Decimal("-0.01"),
                        Decimal("200"),
                        Decimal("2"),
                        Decimal("-0.10"),
                    )

                async def request_guarded_var_order(self, **kwargs):
                    self.requests += 1
                    return {"ok": False, "error": "must not run"}

            runtime = GateRuntime()
            runtime.runtime.command_broker = FakeBroker()
            runtime.variational_ticker = "BTC"
            runtime.automation_ready = True
            current_open = OrderLifecycle(
                trade_key="open",
                trade_id="open",
                side="buy",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
            )
            await runtime._evaluate_auto_close_once(current_open)

            self.assertEqual(runtime.requests, 0)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_pending_var_intent_timeout_pauses_instead_of_retrying(self) -> None:
        async def run_case() -> None:
            os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
            os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
            os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

            runtime.mark_var_intent_sent("open", "BUY", Decimal("300"))
            runtime.pending_var_intent.sent_monotonic -= 30

            self.assertTrue(runtime.expire_pending_var_intent())
            self.assertTrue(runtime.automation_paused)
            self.assertIn("no fill event", runtime.automation_pause_reason)

        asyncio.run(run_case())

    def test_receiver_channel_never_changes_strategy_config(self) -> None:
        async def run_case() -> None:
            received: list[dict] = []

            async def handle_config(payload: dict) -> dict:
                received.append(payload)
                return payload

            sink = EventSink(output_dir=None, quiet=True)
            sink.config_handler = handle_config
            await sink.handle(
                "ws",
                json.dumps(
                    {
                        "type": "CONFIG_UPDATE",
                        "strategyConfig": {
                            "signalNotionalUsd": "200",
                            "closeMinProfitUsd": "-0.12",
                        },
                    }
                ),
            )

            self.assertEqual(received, [])

        asyncio.run(run_case())

    def test_receiver_websocket_bridge_does_not_count_as_order_command(self) -> None:
        async def run_case() -> None:
            class FakeWebSocket:
                def __init__(self) -> None:
                    self.sent: list[dict] = []

                async def send(self, raw: str) -> None:
                    self.sent.append(json.loads(raw))

            websocket = FakeWebSocket()
            broker = CommandBroker(quiet=True)

            await broker.attach_extension_transport(websocket)
            self.assertFalse(await broker.extension_connected())

            result = await broker.request_place_order(
                side="BUY",
                amount="200",
                timeout_ms=1000,
                fetch_stage="quote",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "No extension command client connected.")

        asyncio.run(run_case())

    def test_command_disconnect_does_not_fall_back_to_receiver_bridge_for_orders(self) -> None:
        async def run_case() -> None:
            class FakeWebSocket:
                def __init__(self) -> None:
                    self.sent: list[dict] = []

                async def send(self, raw: str) -> None:
                    self.sent.append(json.loads(raw))

            bridge_ws = FakeWebSocket()
            command_ws = FakeWebSocket()
            broker = CommandBroker(quiet=True)

            await broker.attach_extension_transport(bridge_ws)
            await broker.handle_raw_message(
                command_ws,
                json.dumps(
                    {
                        "type": "REGISTER",
                        "role": "extension",
                        "protocolVersion": COMMAND_PROTOCOL_VERSION,
                        "build": COMMAND_EXTENSION_BUILD,
                    }
                ),
            )
            await broker.on_disconnect(command_ws)
            self.assertFalse(await broker.extension_connected())

            result = await broker.request_place_order(
                side="SELL",
                amount="200",
                timeout_ms=1000,
                fetch_stage="quote",
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "No extension command client connected.")

        asyncio.run(run_case())

    def test_command_order_payload_is_fetch_only_and_omits_strategy_mode_fields(self) -> None:
        async def run_case() -> None:
            class FakeWebSocket:
                def __init__(self) -> None:
                    self.sent: list[dict] = []

                async def send(self, raw: str) -> None:
                    self.sent.append(json.loads(raw))

            websocket = FakeWebSocket()
            broker = CommandBroker(quiet=True)
            await broker.handle_raw_message(
                websocket,
                json.dumps(
                    {
                        "type": "REGISTER",
                        "role": "extension",
                        "protocolVersion": COMMAND_PROTOCOL_VERSION,
                        "build": COMMAND_EXTENSION_BUILD,
                    }
                ),
            )

            task = asyncio.create_task(
                broker.request_place_order(
                    side="BUY",
                    amount="200",
                    base_qty="0.003186",
                    timeout_ms=1000,
                    phase="open",
                    trace_id="trace-command-123",
                    fetch_stage="commit",
                    firm_quote={"quoteId": "quote-123", "firmPrice": "63000", "firmQty": "0.003174"},
                    guard={"required": True, "minPnlUsd": "0.18"},
                )
            )
            await asyncio.sleep(0)
            place_order = websocket.sent[-1]
            self.assertEqual(place_order["type"], "PLACE_ORDER")
            self.assertEqual(place_order["phase"], "open")
            self.assertEqual(place_order["traceId"], "trace-command-123")
            self.assertEqual(place_order["fetchStage"], "commit")
            self.assertEqual(place_order["firmQuote"]["quoteId"], "quote-123")
            self.assertEqual(place_order["baseQty"], "0.003186")
            self.assertNotIn("executionMode", place_order)
            self.assertNotIn("fillAmount", place_order)
            self.assertEqual(
                place_order["guard"],
                {"required": True, "minPnlUsd": "0.18"},
            )

            await broker.handle_raw_message(
                websocket,
                json.dumps(
                    {
                        "type": "ORDER_RESULT",
                        "requestId": place_order["requestId"],
                        "ok": True,
                    }
                ),
            )
            result = await task
            self.assertTrue(result["ok"])

        asyncio.run(run_case())

    def test_lighter_vwap_consumes_enough_depth_for_exact_quantity(self) -> None:
        book = {
            "bids": {
                Decimal("100.0"): Decimal("1.0"),
                Decimal("99.0"): Decimal("2.0"),
            },
            "asks": {
                Decimal("101.0"): Decimal("0.5"),
                Decimal("102.0"): Decimal("2.0"),
            },
        }

        self.assertEqual(
            calculate_lighter_vwap(book, "SELL", Decimal("2")),
            Decimal("99.5"),
        )
        self.assertEqual(
            calculate_lighter_vwap(book, "BUY", Decimal("1.5")),
            Decimal("101.6666666666666666666666667"),
        )
        self.assertIsNone(calculate_lighter_vwap(book, "BUY", Decimal("3")))

    def test_firm_quote_guard_adds_frozen_execution_reserve(self) -> None:
        decision = evaluate_firm_quote_guard(
            var_side="BUY",
            firm_price=Decimal("100"),
            firm_qty=Decimal("2"),
            lighter_vwap=Decimal("100.1"),
            minimum_pnl=Decimal("0.10"),
            execution_reserve=Decimal("0.03"),
        )

        self.assertIsInstance(decision, FirmQuoteDecision)
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.expected_pnl, Decimal("0.2"))
        self.assertEqual(decision.required_pnl, Decimal("0.13"))

        rejected = evaluate_firm_quote_guard(
            var_side="SELL",
            firm_price=Decimal("100"),
            firm_qty=Decimal("2"),
            lighter_vwap=Decimal("100.04"),
            minimum_pnl=Decimal("-0.10"),
            execution_reserve=Decimal("0.03"),
        )
        self.assertFalse(rejected.allowed)
        self.assertEqual(rejected.expected_pnl, Decimal("-0.08"))
        self.assertEqual(rejected.required_pnl, Decimal("-0.07"))

    def test_guarded_var_fetch_rechecks_latest_lighter_depth_before_commit(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    if kwargs.get("fetch_stage") == "quote":
                        return {
                            "ok": True,
                            "requestId": "quote-request",
                            "detail": {
                                "stage": "quote",
                                "quote": {
                                    "quoteId": "firm-1",
                                    "firmPrice": "100.1",
                                    "firmQty": "2",
                                },
                            },
                        }
                    return {
                        "ok": True,
                        "requestId": "commit-request",
                        "detail": {"stage": "commit", "status": 200},
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = Decimal("1000000")
            broker = FakeBroker()
            runtime.runtime.command_broker = broker
            runtime.variational_ticker = "BTC"
            open_candidate = make_open_candidate(runtime)
            # Firm Guard must use the in-flight candidate's frozen epoch, not
            # a mutable/current runtime setting.
            runtime.strategy_config.provisional_reserve_bps_per_leg = Decimal("5")
            runtime.lighter_order_book = {
                "bids": {Decimal("100.2033"): Decimal("10")},
                "asks": {Decimal("100.30"): Decimal("10")},
            }
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_book_received_monotonic = time.monotonic()

            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=open_candidate,
            )

            self.assertTrue(result["ok"])
            self.assertEqual([call["fetch_stage"] for call in broker.calls], ["quote", "commit"])
            firm_quote = broker.calls[1]["firm_quote"]
            self.assertEqual(firm_quote["quoteId"], "firm-1")
            self.assertEqual(firm_quote["guardPnl"], "0.2066")
            self.assertEqual(firm_quote["executionReserveUsd"], "0.01001")
            self.assertEqual(
                Decimal(firm_quote["guardMinPnl"]),
                open_candidate.threshold * Decimal("200.2")
                + Decimal("0.01001"),
            )
            self.assertEqual(
                firm_quote["strategyTag"],
                main_module.ADAPTIVE_MODEL_VERSION,
            )
            self.assertEqual(result["detail"]["quote"]["quoteId"], "firm-1")

        asyncio.run(run_case())

    def test_guarded_var_fetch_does_not_commit_when_fresh_depth_is_too_expensive(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return {
                        "ok": True,
                        "requestId": "quote-request",
                        "detail": {
                            "quote": {
                                "quoteId": "firm-2",
                                "firmPrice": "100",
                                "firmQty": "2",
                            }
                        },
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            broker = FakeBroker()
            runtime.runtime.command_broker = broker
            runtime.variational_ticker = "BTC"
            open_candidate = make_open_candidate(runtime)
            runtime.lighter_order_book = {
                "bids": {Decimal("100.04"): Decimal("2")},
                "asks": {Decimal("100.20"): Decimal("2")},
            }
            runtime.lighter_order_book_ready = True
            runtime.lighter_book_received_monotonic = time.monotonic()

            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=open_candidate,
            )

            self.assertFalse(result["ok"])
            self.assertEqual(len(broker.calls), 1)
            self.assertIn("fresh firm quote", result["error"])

        asyncio.run(run_case())

    def test_close_firm_notional_above_open_cap_still_uses_frozen_exit_rules(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    if kwargs.get("fetch_stage") == "quote":
                        return {
                            "ok": True,
                            "requestId": "close-quote-request",
                            "detail": {
                                "quote": {
                                    "quoteId": "close-firm-1",
                                    "firmPrice": "105.10",
                                    "firmQty": "2",
                                }
                            },
                        }
                    return {
                        "ok": True,
                        "requestId": "close-commit-request",
                        "detail": {"stage": "commit", "status": 200},
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = Decimal("1000000")
            broker = FakeBroker()
            runtime.runtime.command_broker = broker
            runtime.variational_ticker = "BTC"
            frozen_open = make_open_candidate(runtime)
            open_record = make_record("adaptive-open-cap", "buy", "2", "100", "100.2")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen_open)
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            runtime.records[open_record.trade_key] = open_record
            runtime.record_order.append(open_record.trade_key)

            # The in-flight preliminary candidate may be older than the quote
            # age limit; Firm Quote and fresh Lighter depth are the authoritative
            # close recheck and must not be invalidated by browser latency.
            captured_at_ms = (
                time.time_ns() // 1_000_000
                - runtime.strategy_config.max_quote_age_ms
                - 1
            )
            close_candidate = CloseCandidate(
                close_direction=Side.SELL,
                frame_captured_at_ms=captured_at_ms,
                frozen_epoch_id=frozen_open.epoch.epoch_id,
                held_seconds=1,
                actual_close_rate=Decimal("0"),
                regression_target_rate=Decimal("-0.001"),
                expected_close_pnl_usd=Decimal("0"),
                close_reserve_usd=Decimal("0.02"),
                round_lower_bound_usd=Decimal("0.38"),
                required_floor_usd=Decimal("0"),
                regression_passed=True,
                max_hold_alert=False,
            )
            # A current config change must not alter the position's frozen exit
            # reserve, and the 200U cap only authorizes new exposure.
            runtime.strategy_config.provisional_reserve_bps_per_leg = Decimal("5")

            async def fresh_vwap(*, var_side: str, qty: Decimal):
                self.assertEqual(var_side, "SELL")
                self.assertEqual(qty, Decimal("2"))
                return Decimal("105.00"), 0

            runtime.get_fresh_lighter_vwap = fresh_vwap
            result = await runtime.request_guarded_var_order(
                phase="close",
                side="SELL",
                amount=Decimal("200"),
                base_qty=Decimal("2"),
                close_candidate=close_candidate,
            )

            self.assertTrue(result["ok"])
            self.assertEqual([call["fetch_stage"] for call in broker.calls], ["quote", "commit"])
            guarded = broker.calls[1]["firm_quote"]
            self.assertEqual(guarded["adaptiveStrategy"]["firmNotionalUsd"], "210.20")
            self.assertTrue(guarded["adaptiveStrategy"]["regressionPassed"])
            self.assertEqual(
                guarded["adaptiveStrategy"]["regressionTargetRate"],
                "-0.001",
            )
            self.assertGreater(
                Decimal(guarded["adaptiveStrategy"]["firmCloseRate"]),
                Decimal("-0.001"),
            )
            self.assertEqual(guarded["executionReserveUsd"], "0.010510")

        asyncio.run(run_case())

    def test_v2_close_firm_guard_keeps_regression_as_diagnostic_only(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    return {
                        "ok": True,
                        "requestId": "close-quote-request",
                        "detail": {
                            "quote": {
                                "quoteId": "close-firm-regression",
                                "firmPrice": "105.10",
                                "firmQty": "2",
                            }
                        },
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = Decimal("1000000")
            broker = FakeBroker()
            runtime.runtime.command_broker = broker
            runtime.variational_ticker = "BTC"
            frozen_open = make_open_candidate(runtime)
            open_record = make_record("adaptive-open-regression", "buy", "2", "100", "100.2")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen_open)
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            runtime.records[open_record.trade_key] = open_record
            runtime.record_order.append(open_record.trade_key)
            close_candidate = CloseCandidate(
                close_direction=Side.SELL,
                frame_captured_at_ms=time.time_ns() // 1_000_000,
                frozen_epoch_id=frozen_open.epoch.epoch_id,
                held_seconds=1,
                actual_close_rate=Decimal("0.02"),
                regression_target_rate=Decimal("0.01"),
                expected_close_pnl_usd=Decimal("4"),
                close_reserve_usd=Decimal("0.02"),
                round_lower_bound_usd=Decimal("4"),
                required_floor_usd=Decimal("0"),
                regression_passed=False,
                max_hold_alert=False,
            )

            async def fresh_vwap(*, var_side: str, qty: Decimal):
                return Decimal("105.00"), 0

            runtime.get_fresh_lighter_vwap = fresh_vwap
            result = await runtime.request_guarded_var_order(
                phase="close",
                side="SELL",
                amount=Decimal("200"),
                base_qty=Decimal("2"),
                close_candidate=close_candidate,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(len(broker.calls), 2)
            guarded = broker.calls[1]["firm_quote"]
            self.assertFalse(guarded["adaptiveStrategy"]["regressionPassed"])
            self.assertFalse(guarded["adaptiveStrategy"]["regressionRequired"])

        asyncio.run(run_case())

    def test_zero_wear_stability_firm_guard_requires_fresh_gross_pnl_non_negative(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    if kwargs.get("fetch_stage") == "quote":
                        return {
                            "ok": True,
                            "requestId": "stable-close-quote",
                            "detail": {
                                "quote": {
                                    "quoteId": "stable-close-firm",
                                    "firmPrice": "100.1975",
                                    "firmQty": "2",
                                }
                            },
                        }
                    return {
                        "ok": True,
                        "requestId": "stable-close-commit",
                        "detail": {"stage": "commit", "status": 200},
                    }

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = Decimal("1000000")
            runtime.runtime.command_broker = FakeBroker()
            runtime.variational_ticker = "BTC"
            frozen_open = make_open_candidate(runtime)
            # Gross open PnL is +0.010U.  The fresh close quote is -0.005U:
            # gross round remains +0.005U, but is below the half reserve.
            open_record = make_record("stable-open", "buy", "2", "100", "100.005")
            open_record.strategy_tag = main_module.ADAPTIVE_MODEL_VERSION
            open_record.strategy_phase = "open"
            open_record.adaptive_strategy_context = open_candidate_to_payload(frozen_open)
            open_record.var_fill_ts_iso = datetime.now(timezone.utc).isoformat()
            open_record.open_notional_usd = Decimal("200")
            open_record.hedge_status = "filled"
            open_record.lighter_filled_qty = Decimal("2")
            runtime.records[open_record.trade_key] = open_record
            runtime.record_order.append(open_record.trade_key)
            close_candidate = CloseCandidate(
                close_direction=Side.SELL,
                frame_captured_at_ms=time.time_ns() // 1_000_000,
                frozen_epoch_id=frozen_open.epoch.epoch_id,
                held_seconds=10,
                actual_close_rate=Decimal("-0.000025"),
                regression_target_rate=Decimal("-1"),
                expected_close_pnl_usd=Decimal("-0.005"),
                close_reserve_usd=Decimal("0.01"),
                round_lower_bound_usd=Decimal("-0.005"),
                required_floor_usd=Decimal("0"),
                regression_passed=True,
                max_hold_alert=False,
                zero_wear_stability_passed=True,
                zero_wear_continuous_ms=2_000,
                zero_wear_accumulated_ms=2_000,
            )

            async def fresh_vwap(*, var_side: str, qty: Decimal):
                self.assertEqual((var_side, qty), ("SELL", Decimal("2")))
                return Decimal("100.2"), 0

            runtime.get_fresh_lighter_vwap = fresh_vwap
            result = await runtime.request_guarded_var_order(
                phase="close",
                side="SELL",
                amount=Decimal("200"),
                base_qty=Decimal("2"),
                close_candidate=close_candidate,
            )

            self.assertTrue(result["ok"])
            guarded = runtime.runtime.command_broker.calls[1]["firm_quote"]
            self.assertEqual(guarded["guardPnl"], "-0.0050")
            self.assertEqual(guarded["guardMinPnl"], "-0.01000000")
            self.assertTrue(
                guarded["adaptiveStrategy"]["zeroWearStabilityPassed"]
            )

        asyncio.run(run_case())

    def test_http_commit_provisional_hedge_merges_fill_without_duplicate(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            result = {
                "ok": True,
                "requestId": "commit-1",
                "timestamp": "2026-07-10T00:00:00+00:00",
                "detail": {
                    "status": 200,
                    "quote": {
                        "quoteId": "firm-1",
                        "firmPrice": "100",
                        "firmQty": "2",
                        "guardPnl": "0.20",
                    },
                },
            }

            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result=result,
            )
            self.assertIsNotNone(provisional)
            self.assertEqual(len(runtime.scheduled), 1)
            self.assertEqual(runtime.pending_var_intent.provisional_trade_key, provisional.trade_key)

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "actual-fill-1",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100.02",
                    "timestamp": "2026-07-10T00:00:00.200000+00:00",
                }
            )

            self.assertEqual(len(runtime.records), 1)
            record = next(iter(runtime.records.values()))
            self.assertEqual(record.trade_id, "actual-fill-1")
            self.assertEqual(record.var_fill_price, Decimal("100.02"))
            self.assertEqual(record.var_fill_source, "event")
            self.assertEqual(len(runtime.scheduled), 1)
            self.assertIsNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_complete_automatic_open_and_close_round_uses_two_stage_var_and_two_hedges(self) -> None:
        async def run_case() -> None:
            class RoundBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def extension_connected(self) -> bool:
                    return True

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    phase = kwargs["phase"]
                    stage = kwargs["fetch_stage"]
                    if stage == "quote":
                        return {
                            "ok": True,
                            "requestId": f"{phase}-quote-request",
                            "detail": {
                                "stage": "quote",
                                "quote": {
                                    "quoteId": f"{phase}-firm",
                                    "firmPrice": "100" if phase == "open" else "100.2",
                                    "firmQty": "2",
                                },
                            },
                        }
                    return {
                        "ok": True,
                        "requestId": f"{phase}-commit-request",
                        "detail": {"stage": "commit", "status": 200},
                    }

            class InstantHedgeRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled_hedges: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    if record.trade_key in self.scheduled_hedges:
                        return False
                    self.scheduled_hedges.append(record.trade_key)
                    record.lighter_side = "SELL" if record.side == "buy" else "BUY"
                    record.lighter_fill_price = (
                        Decimal("100.2") if record.lighter_side == "SELL" else Decimal("100")
                    )
                    record.lighter_filled_qty = record.qty
                    record.lighter_filled_quote = record.qty * record.lighter_fill_price
                    record.lighter_fill_ts_iso = datetime.now(timezone.utc).isoformat()
                    record.lighter_client_order_id = record.lighter_reserved_client_order_id
                    if record.lighter_client_order_id is not None:
                        record.lighter_client_order_ids = [record.lighter_client_order_id]
                    record.lighter_outcome_final = True
                    record.hedge_status = "filled"
                    record.execution_state = "HEDGED"
                    return True

            runtime = InstantHedgeRuntime()
            runtime.runtime.command_broker = RoundBroker()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.base_amount_multiplier = Decimal("1")
            runtime.strategy_config.execution_mode = "live"
            runtime._canary_session_state = main_module.CANARY_SESSION_ARMED
            runtime.persist_runtime_state = AsyncMock()
            runtime.append_order_log = AsyncMock()
            runtime.automation_can_submit_var_order = lambda *_args, **_kwargs: True
            runtime.live_open_block_reason = lambda: None
            runtime.lighter_order_entry_is_ready = lambda: True

            open_candidate = make_open_candidate(runtime)
            runtime.lighter_order_book = {
                "bids": {Decimal("100.103"): Decimal("10")},
                "asks": {Decimal("100"): Decimal("10")},
            }
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_book_received_monotonic = time.monotonic()

            async def open_signal(_current_open):
                runtime._selected_open_candidate = open_candidate
                return "BUY", Decimal("0.4")

            runtime._auto_var_signal_for_current_open = open_signal
            await runtime._evaluate_auto_open_once(None)
            self.assertEqual(len(runtime.records), 1)
            open_record = next(iter(runtime.records.values()))
            self.assertEqual(open_record.hedge_status, "filled")

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "full-round-open-fill",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            current_open, rounds = build_trade_rounds(
                [runtime.records[key] for key in runtime.record_order]
            )
            self.assertIsNotNone(current_open)
            self.assertEqual(rounds, [])
            assert current_open is not None

            close_candidate = CloseCandidate(
                close_direction=Side.SELL,
                frame_captured_at_ms=time.time_ns() // 1_000_000,
                frozen_epoch_id=open_candidate.epoch.epoch_id,
                held_seconds=1,
                actual_close_rate=Decimal("0.002"),
                regression_target_rate=Decimal("-1"),
                expected_close_pnl_usd=Decimal("0.4"),
                close_reserve_usd=Decimal("0.01"),
                round_lower_bound_usd=Decimal("0.78"),
                required_floor_usd=Decimal("0"),
                regression_passed=True,
                max_hold_alert=False,
            )

            async def close_signal(record):
                self.assertIs(record, current_open)
                return (
                    "SELL",
                    Decimal("0.78"),
                    Decimal("200"),
                    Decimal("2"),
                    close_candidate,
                )

            runtime._auto_var_close_signal_for_current_open = close_signal
            runtime._last_auto_var_order_at = 0
            await runtime._evaluate_auto_close_once(current_open)
            self.assertEqual(len(runtime.records), 2)

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "full-round-close-fill",
                    "side": "sell",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100.2",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

            current_open, rounds = build_trade_rounds(
                [runtime.records[key] for key in runtime.record_order]
            )
            self.assertIsNone(current_open)
            self.assertEqual(len(rounds), 1)
            self.assertGreater(rounds[0].round_pnl or Decimal("-1"), Decimal("0"))
            self.assertEqual(len(runtime.scheduled_hedges), 2)
            self.assertEqual(
                [(call["phase"], call["fetch_stage"]) for call in runtime.runtime.command_broker.calls],
                [
                    ("open", "quote"),
                    ("open", "commit"),
                    ("close", "quote"),
                    ("close", "commit"),
                ],
            )
            self.assertGreater(runtime._last_round_closed_at, 0)

        asyncio.run(run_case())

    def test_prepared_intent_allows_commit_to_schedule_hedge_without_post_commit_persist(self) -> None:
        async def run_case() -> None:
            events: list[str] = []

            class WriteAheadRuntime(VariationalToLighterRuntime):
                async def persist_runtime_state(self) -> None:
                    events.append("persist")

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    events.append("schedule")

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    events.append("log")

            runtime = WriteAheadRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.variational_ticker = "BTC"
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            runtime.pending_var_intent.state = "VAR_COMMITTING"
            runtime.pending_var_intent.trace_id = "trace-write-ahead"
            runtime.pending_var_intent.firm_quote_id = "firm-write-ahead"
            runtime.pending_var_intent.lighter_client_order_index = 654321

            record = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "write-ahead-commit",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-write-ahead",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )

            self.assertEqual(events[:2], ["schedule", "log"])
            self.assertNotIn("persist", events)
            self.assertIsNotNone(record)
            self.assertEqual(record.execution_state, "VAR_COMMITTED")
            self.assertEqual(record.lighter_reserved_client_order_id, 654321)

        asyncio.run(run_case())

    def test_var_fill_before_commit_result_waits_and_schedules_exactly_once(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)
                    return True

                async def persist_runtime_state(self) -> None:
                    return None

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            intent = await runtime.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="trace-event-first",
                firm_quote={
                    "quoteId": "firm-event-first",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "guardPnl": "0.2",
                    "guardMinPnl": "0.1",
                },
            )
            self.assertIsNotNone(intent)
            self.assertTrue(
                await runtime.mark_pending_var_intent_committing(
                    phase="open", side="BUY", trace_id="trace-event-first"
                )
            )

            event_time = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "event-first-trade",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "timestamp": event_time,
                    "captured_at": event_time,
                    "source_rfq": "rfq-event-first",
                    "source_quote": "source-quote-event-first",
                }
            )

            self.assertEqual(runtime.scheduled, [])
            self.assertIs(runtime.pending_var_intent, intent)
            record = next(iter(runtime.records.values()))
            self.assertEqual(record.hedge_status, "waiting_commit")
            self.assertEqual(record.var_source_rfq, "rfq-event-first")

            committed = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-event-first",
                    "trace_id": "trace-event-first",
                    "detail": {
                        "responsePreview": json.dumps({"rfq_id": "rfq-event-first"}),
                        "quote": {
                            "quoteId": "firm-event-first",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        },
                    },
                },
            )

            self.assertIs(committed, record)
            self.assertEqual(runtime.scheduled, [record])
            self.assertIsNone(runtime.pending_var_intent)
            self.assertEqual(record.var_event_origin, "AUTO_INTENT")

        asyncio.run(run_case())

    def test_ambiguous_commit_releases_preobserved_fill_with_frozen_lighter_id(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)
                    return True

                async def persist_runtime_state(self) -> None:
                    return None

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            expected = runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            intent = await runtime.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="trace-ambiguous-release",
                firm_quote={
                    "quoteId": "firm-ambiguous-release",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "guardPnl": "0.2",
                    "guardMinPnl": "0.1",
                },
                expected_intent=expected,
            )
            self.assertIs(intent, expected)
            self.assertTrue(
                await runtime.mark_pending_var_intent_committing(
                    phase="open",
                    side="BUY",
                    trace_id="trace-ambiguous-release",
                    expected_intent=intent,
                )
            )
            reserved_id = intent.lighter_client_order_index
            now = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "ambiguous-release-trade",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "timestamp": now,
                    "captured_at": now,
                }
            )

            record = next(iter(runtime.records.values()))
            self.assertEqual(record.hedge_status, "waiting_commit")
            self.assertEqual(runtime.scheduled, [])
            self.assertTrue(
                await runtime.mark_pending_var_intent_commit_ambiguous(
                    phase="open",
                    side="BUY",
                    trace_id="trace-ambiguous-release",
                    expected_intent=intent,
                )
            )

            self.assertIsNone(runtime.pending_var_intent)
            self.assertEqual(runtime.scheduled, [record])
            self.assertEqual(record.hedge_status, "queued")
            self.assertEqual(record.lighter_reserved_client_order_id, reserved_id)
            self.assertEqual(record.var_fill_source, "event")

        asyncio.run(run_case())

    def test_quoting_intent_is_cancelled_before_manual_fill_is_hedged(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record)
                    return True

                async def persist_runtime_state(self) -> None:
                    return None

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            intent = runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            now = datetime.now(timezone.utc).isoformat()
            event = {
                "trade_id": "same-size-during-quote",
                "side": "buy",
                "qty": "2",
                "asset": "BTC",
                "status": "filled",
                "price": "100",
                "timestamp": now,
                "captured_at": now,
            }

            self.assertFalse(runtime.var_event_matches_intent(intent, event))
            await runtime.process_variational_trade_event(event)

            self.assertIsNone(runtime.pending_var_intent)
            manual = runtime.records["id:same-size-during-quote"]
            self.assertEqual(runtime.scheduled, [manual])
            self.assertEqual(manual.strategy_tag, "manual")
            self.assertEqual(manual.var_event_origin, "MANUAL_LIVE")
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_commit_rfq_mismatch_never_schedules_preobserved_fill(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record)
                    return True

                async def persist_runtime_state(self) -> None:
                    return None

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            await runtime.prepare_pending_var_intent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                trace_id="trace-rfq-mismatch",
                firm_quote={
                    "quoteId": "firm-rfq-mismatch",
                    "firmPrice": "100",
                    "firmQty": "2",
                    "guardPnl": "0.2",
                    "guardMinPnl": "0.1",
                },
            )
            await runtime.mark_pending_var_intent_committing(
                phase="open", side="BUY", trace_id="trace-rfq-mismatch"
            )
            now = datetime.now(timezone.utc).isoformat()
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "wrong-rfq-trade",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                    "timestamp": now,
                    "captured_at": now,
                    "source_rfq": "rfq-wrong",
                }
            )

            committed = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-rfq-mismatch",
                    "trace_id": "trace-rfq-mismatch",
                    "detail": {
                        "responsePreview": json.dumps({"rfq_id": "rfq-correct"}),
                        "quote": {
                            "quoteId": "firm-rfq-mismatch",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        },
                    },
                },
            )

            self.assertIsNone(committed)
            self.assertEqual(runtime.scheduled, [])
            self.assertTrue(runtime.automation_paused)
            record = next(iter(runtime.records.values()))
            self.assertEqual(record.execution_state, "RECOVERY_REQUIRED")

        asyncio.run(run_case())

    def test_fill_event_winning_race_prevents_late_http_result_duplicate(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            intent = runtime.mark_var_intent_sent("open", "SELL", Decimal("200"))
            intent.state = main_module.VAR_INTENT_COMMITTING
            intent.firm_quote_id = "firm-late"
            intent.firm_price = Decimal("100")
            intent.firm_qty = Decimal("2")
            intent.firm_guard_pnl = Decimal("0.1")
            await runtime.process_variational_trade_event(
                {
                    "trade_id": "actual-first",
                    "side": "sell",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                }
            )

            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="SELL",
                result={
                    "ok": True,
                    "requestId": "late-result",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-late",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.1",
                        }
                    },
                },
            )

            self.assertIsNotNone(provisional)
            self.assertEqual(len(runtime.records), 1)
            self.assertEqual(len(runtime.scheduled), 1)
            self.assertEqual(provisional.trade_id, "actual-first")
            self.assertEqual(provisional.strategy_phase, "open")
            self.assertEqual(provisional.firm_quote_id, "firm-late")
            self.assertEqual(provisional.firm_guard_pnl, Decimal("0.1"))

        asyncio.run(run_case())

    def test_event_first_merge_rereads_intent_after_record_lock_wait(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    self.scheduled.append(record)

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            await runtime._record_lock.acquire()
            try:
                task = asyncio.create_task(
                    runtime.start_committed_var_hedge(
                        phase="open",
                        side="BUY",
                        result={
                            "ok": True,
                            "requestId": "late-http",
                            "detail": {
                                "quote": {
                                    "quoteId": "firm-race",
                                    "firmPrice": "100",
                                    "firmQty": "2",
                                    "guardPnl": "0.2",
                                }
                            },
                        },
                    )
                )
                await asyncio.sleep(0)
                event_record = make_record("id:actual", "buy", "2", "100.01", "100.1")
                event_record.lighter_fill_price = None
                event_record.strategy_phase = "open"
                runtime.records[event_record.trade_key] = event_record
                runtime.record_order.append(event_record.trade_key)
                runtime.pending_var_intent = None
            finally:
                runtime._record_lock.release()

            merged = await task

            self.assertIs(merged, event_record)
            self.assertEqual(list(runtime.records), ["id:actual"])
            self.assertEqual(runtime.scheduled, [])
            self.assertEqual(event_record.firm_quote_id, "firm-race")

        asyncio.run(run_case())

    def test_http_first_lock_queue_does_not_leave_event_with_stale_intent(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))

            await runtime._record_lock.acquire()
            try:
                http_task = asyncio.create_task(
                    runtime.start_committed_var_hedge(
                        phase="open",
                        side="BUY",
                        result={
                            "ok": True,
                            "requestId": "http-first",
                            "detail": {
                                "quote": {
                                    "quoteId": "firm-http-first",
                                    "firmPrice": "100",
                                    "firmQty": "2",
                                    "guardPnl": "0.2",
                                }
                            },
                        },
                    )
                )
                await asyncio.sleep(0)
                event_task = asyncio.create_task(
                    runtime.process_variational_trade_event(
                        {
                            "trade_id": "actual-after-http-queued",
                            "side": "buy",
                            "qty": "2",
                            "asset": "BTC",
                            "status": "filled",
                            "price": "100.01",
                        }
                    )
                )
                await asyncio.sleep(0)
            finally:
                runtime._record_lock.release()

            await asyncio.gather(http_task, event_task)

            self.assertEqual(len(runtime.records), 1)
            self.assertEqual(len(runtime.scheduled), 1)
            record = next(iter(runtime.records.values()))
            self.assertEqual(record.trade_id, "actual-after-http-queued")
            self.assertEqual(record.var_fill_source, "event")

        asyncio.run(run_case())

    def test_http_commit_does_not_wait_for_a_post_commit_state_write(self) -> None:
        async def run_case() -> None:
            class PersistTrackingRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []
                    self.persist_calls = 0

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record.trade_key)

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    self.persist_calls += 1

            runtime = PersistTrackingRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))

            record = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "persist-race",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-persist-race",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )

            self.assertIsNotNone(record)
            self.assertEqual(runtime.persist_calls, 0)
            self.assertEqual(runtime.scheduled, ["commit:persist-race"])

        asyncio.run(run_case())

    def test_commit_result_to_lighter_queue_p95_is_under_ten_milliseconds(self) -> None:
        async def run_case() -> None:
            class BenchmarkRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.trace_writer = None
                    self.scheduled_at_ns = 0

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    record.hedge_status = "queued"
                    self.scheduled_at_ns = time.perf_counter_ns()
                    return True

                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

                async def reconcile_lighter_client_order(self, *_args, **_kwargs):
                    raise AssertionError("normal confirmed Commit must not query recovery")

            runtime = BenchmarkRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            samples: list[int] = []
            for index in range(10_000):
                runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
                started_ns = time.perf_counter_ns()
                record = await runtime.start_committed_var_hedge(
                    phase="open",
                    side="BUY",
                    result={
                        "ok": True,
                        "requestId": f"latency-{index}",
                        "detail": {
                            "quote": {
                                "quoteId": f"firm-latency-{index}",
                                "firmPrice": "100",
                                "firmQty": "2",
                                "guardPnl": "0.2",
                            }
                        },
                    },
                )
                self.assertIsNotNone(record)
                self.assertGreaterEqual(runtime.scheduled_at_ns, started_ns)
                samples.append(runtime.scheduled_at_ns - started_ns)

            p95_ns = sorted(samples)[9_499]
            self.assertLess(
                p95_ns,
                10_000_000,
                f"Commit result to Lighter queue p95={p95_ns}ns",
            )

        asyncio.run(run_case())

    def test_unconfirmed_var_commit_rolls_back_filled_lighter_exposure(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-unconfirmed",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-unconfirmed",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )
            provisional.lighter_filled_qty = Decimal("2")
            provisional.lighter_fill_price = Decimal("100.1")
            provisional.hedge_status = "filled"

            handled = await runtime.rollback_unconfirmed_var_commit()

            self.assertTrue(handled)
            self.assertIsNone(runtime.pending_var_intent)
            self.assertIsNone(provisional.var_fill_price)
            self.assertEqual(provisional.var_fill_source, "unconfirmed_commit")
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(len(runtime.scheduled), 2)
            rollback = runtime.scheduled[-1]
            self.assertEqual(rollback.side, "sell")
            self.assertEqual(rollback.qty, Decimal("2"))
            self.assertTrue(rollback.lighter_reduce_only)
            self.assertEqual(rollback.var_fill_source, "lighter_rollback")

        asyncio.run(run_case())

    def test_late_lighter_fill_after_unconfirmed_commit_is_also_rolled_back(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.base_amount_multiplier = 1
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-late-fill",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-late-fill",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )
            provisional.lighter_client_order_id = 123
            provisional.lighter_client_order_ids = [123]
            runtime.lighter_client_order_to_trade_key[123] = provisional.trade_key

            await runtime.rollback_unconfirmed_var_commit()
            self.assertEqual(len(runtime.scheduled), 1)

            await runtime.handle_lighter_fill_update(
                {
                    "client_order_id": "123",
                    "status": "filled",
                    "filled_base_amount": "2",
                    "filled_quote_amount": "200.2",
                }
            )

            self.assertEqual(len(runtime.scheduled), 2)
            rollback = runtime.scheduled[-1]
            self.assertEqual(rollback.var_fill_source, "lighter_rollback")
            self.assertEqual(rollback.qty, Decimal("2"))

        asyncio.run(run_case())

    def test_confirmed_var_open_is_flattened_when_early_lighter_hedge_failed(self) -> None:
        async def run_case() -> None:
            class FailedHedgeRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.emergency: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "error"
                    record.hedge_error = "simulated early rejection"

                async def emergency_flatten_var(self, record: OrderLifecycle) -> None:
                    self.emergency.append(record.trade_key)

            runtime = FailedHedgeRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-hedge-failed",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-hedge-failed",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "confirmed-after-hedge-failed",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                }
            )

            self.assertEqual(runtime.emergency, [provisional.trade_key])

        asyncio.run(run_case())

    def test_unconfirmed_close_restores_lighter_hedge_without_reduce_only(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.mark_var_intent_sent("close", "SELL", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="close",
                side="SELL",
                result={
                    "ok": True,
                    "requestId": "commit-close-unconfirmed",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-close-unconfirmed",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )
            provisional.lighter_filled_qty = Decimal("2")
            provisional.hedge_status = "filled"

            await runtime.rollback_unconfirmed_var_commit()

            restore = runtime.scheduled[-1]
            self.assertEqual(restore.side, "buy")
            self.assertFalse(restore.lighter_reduce_only)

        asyncio.run(run_case())

    def test_late_var_fill_after_rollback_creates_fresh_hedge_even_with_same_trade_id(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-same-id",
                    "detail": {
                        "orderId": "same-trade-id",
                        "quote": {
                            "quoteId": "firm-same-id",
                            "firmPrice": "100",
                            "firmQty": "2",
                            "guardPnl": "0.2",
                        },
                    },
                },
            )
            provisional.lighter_filled_qty = Decimal("2")
            provisional.hedge_status = "filled"
            await runtime.rollback_unconfirmed_var_commit()

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "same-trade-id",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100.01",
                }
            )

            self.assertEqual(len(runtime.scheduled), 3)
            confirmed = [
                record for record in runtime.records.values()
                if record.var_fill_source == "event"
            ]
            self.assertEqual(len(confirmed), 1)
            self.assertNotEqual(confirmed[0].trade_key, provisional.trade_key)

        asyncio.run(run_case())

    def test_execution_loss_sample_waits_for_full_lighter_fill(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        record = make_record("sample", "buy", "2", "100", "100.1")
        record.var_fill_source = "event"
        record.strategy_phase = "open"
        record.firm_guard_pnl = Decimal("0.25")
        record.lighter_filled_qty = Decimal("1")
        record.hedge_status = "partial"

        runtime._capture_execution_loss_locked(record)
        self.assertFalse(record.execution_loss_recorded)

        record.lighter_filled_qty = Decimal("2")
        record.hedge_status = "filled"
        runtime._capture_execution_loss_locked(record)
        self.assertTrue(record.execution_loss_recorded)

    def test_actual_var_qty_larger_than_firm_qty_tops_up_completed_hedge(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.base_amount_multiplier = 1_000_000
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-topup",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-topup",
                            "firmPrice": "100",
                            "firmQty": "1.9",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )
            provisional.lighter_filled_qty = Decimal("1.9")
            provisional.lighter_fill_price = Decimal("100.1")
            provisional.hedge_status = "filled"

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "actual-topup",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                }
            )

            self.assertEqual(len(runtime.scheduled), 2)
            self.assertIs(runtime.scheduled[-1], provisional)
            self.assertEqual(provisional.qty, Decimal("2"))

        asyncio.run(run_case())

    def test_actual_var_qty_smaller_than_firm_qty_reduces_excess_hedge(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[OrderLifecycle] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> None:
                    record.hedge_status = "queued"
                    self.scheduled.append(record)

            runtime = CaptureRuntime()
            runtime.variational_ticker = "BTC"
            runtime.accepted_assets = {"BTC"}
            runtime.base_amount_multiplier = 1_000_000
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            provisional = await runtime.start_committed_var_hedge(
                phase="open",
                side="BUY",
                result={
                    "ok": True,
                    "requestId": "commit-excess",
                    "detail": {
                        "quote": {
                            "quoteId": "firm-excess",
                            "firmPrice": "100",
                            "firmQty": "2.1",
                            "guardPnl": "0.2",
                        }
                    },
                },
            )
            provisional.lighter_filled_qty = Decimal("2.1")
            provisional.lighter_fill_price = Decimal("100.1")
            provisional.hedge_status = "filled"

            await runtime.process_variational_trade_event(
                {
                    "trade_id": "actual-excess",
                    "side": "buy",
                    "qty": "2",
                    "asset": "BTC",
                    "status": "filled",
                    "price": "100",
                }
            )

            self.assertEqual(len(runtime.scheduled), 2)
            correction = runtime.scheduled[-1]
            self.assertIsNot(correction, provisional)
            self.assertEqual(correction.qty, Decimal("0.1"))
            self.assertEqual(correction.side, "sell")
            self.assertTrue(correction.lighter_reduce_only)

        asyncio.run(run_case())

    def test_var_close_qty_is_rounded_to_min_tick(self) -> None:
        self.assertEqual(normalize_var_base_qty(Decimal("0.0031869")), Decimal("0.003186"))
        self.assertIsNone(normalize_var_base_qty(Decimal("0.0000009")))

    def test_runtime_recovery_keeps_only_unfinished_exposure(self) -> None:
        recovery_filter = getattr(main_module, "runtime_recovery_records", None)
        self.assertIsNotNone(recovery_filter, "runtime recovery filter must be implemented")

        completed_open = make_record("old-open", "buy", "2", "100", "100.1")
        completed_close = make_record("old-close", "sell", "2", "100.2", "100.3")
        active_open = make_record("active-open", "buy", "2", "101", "101.1")
        for record in (completed_open, completed_close, active_open):
            record.lighter_filled_qty = record.qty
            record.hedge_status = "filled"

        retained = recovery_filter([completed_open, completed_close, active_open])

        self.assertEqual([record.trade_key for record in retained], ["active-open"])

    def test_runtime_recovery_keeps_round_with_unconfirmed_var_close(self) -> None:
        recovery_filter = getattr(main_module, "runtime_recovery_records")
        confirmed_open = make_record("confirmed-open", "buy", "2", "100", "100.1")
        provisional_close = make_record("provisional-close", "sell", "2", "100.2", "100.3")
        confirmed_open.var_fill_source = "event"
        provisional_close.var_fill_source = "http_commit"
        provisional_close.last_variational_status = "accepted"
        for record in (confirmed_open, provisional_close):
            record.lighter_filled_qty = record.qty
            record.hedge_status = "filled"

        retained = recovery_filter([confirmed_open, provisional_close])

        self.assertEqual(
            [record.trade_key for record in retained],
            ["confirmed-open", "provisional-close"],
        )

    def test_runtime_has_no_persistent_dashboard_csv_export(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        self.assertFalse(hasattr(runtime, "trade_records_csv_file"))
        self.assertFalse(hasattr(runtime, "export_trade_records_csv"))

    def test_new_runtime_preserves_visible_logs_and_recovery_state(self) -> None:
        metrics_path = main_module.OUTPUT_DIR / "order_metrics.jsonl"
        main_module.APP_LOG_FILE.write_text("old runtime\n", encoding="utf-8")
        metrics_path.write_text("old metrics\n", encoding="utf-8")
        main_module.RUNTIME_STATE_FILE.write_text('{"records": []}', encoding="utf-8")

        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))

        self.assertIn("old runtime", main_module.APP_LOG_FILE.read_text(encoding="utf-8"))
        self.assertEqual(metrics_path.read_text(encoding="utf-8"), "old metrics\n")
        self.assertTrue(main_module.RUNTIME_STATE_FILE.exists())
        for handler in runtime.logger.handlers:
            handler.close()


if __name__ == "__main__":
    unittest.main()
