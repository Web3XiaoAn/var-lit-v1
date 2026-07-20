import asyncio
import os
import time
import unittest
from argparse import Namespace
from decimal import Decimal
from unittest.mock import patch

import main as main_module
from main import (
    OrderLifecycle,
    VariationalToLighterRuntime,
    calculate_lighter_execution,
)


os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


def record() -> OrderLifecycle:
    return OrderLifecycle(
        trade_key="depth",
        trade_id="depth",
        side="buy",
        qty=Decimal("2"),
        asset="BTC",
        auto_hedge_enabled=True,
        last_variational_status="filled",
        firm_price=Decimal("98"),
        firm_required_pnl=Decimal("1"),
        lighter_client_order_id=77,
    )


class DepthRevalidationTests(unittest.TestCase):
    def test_execution_reports_vwap_and_marginal_price_from_complete_depth(self):
        result = calculate_lighter_execution(
            {"bids": {Decimal("100"): Decimal("1"), Decimal("99"): Decimal("2")}, "asks": {}},
            "SELL",
            Decimal("2"),
        )
        self.assertEqual(result, (Decimal("99.5"), Decimal("99")))

    def test_snapshot_uses_latest_nonce_and_existing_ioc_boundary(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("bids", [["100", "2"]])
            runtime.update_lighter_order_book("asks", [["101", "2"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1234
            runtime.lighter_book_received_monotonic = time.monotonic()
            with patch.object(main_module, "evaluate_firm_quote_guard") as guard:
                guard.side_effect = AssertionError("post-Commit snapshot must not rerun Firm Guard")
                snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                    record=record(),
                    lighter_side="SELL",
                    base_amount=2_000,
                    market_generation=runtime.market_generation,
                    market_index=runtime.lighter_market_index,
                )
            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.order_book_nonce, 1234)
            self.assertEqual(snapshot.price_i, 9_998)
            self.assertEqual(snapshot.marginal_price_i, 10_000)
            self.assertEqual(snapshot.economic_limit_price_i, 9_850)
            self.assertTrue(runtime.lighter_execution_tick_cache)

            runtime.update_lighter_order_book("bids", [["100", "1"], ["98", "1"]])
            rejected, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=record(),
                lighter_side="SELL",
                base_amount=2_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )
            self.assertIsNone(rejected)
            self.assertIn("marginal", error or "")

        asyncio.run(run_case())

    def test_frozen_firm_economics_cap_post_commit_ioc(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("asks", [["101", "2"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1235
            runtime.lighter_book_received_monotonic = time.monotonic()
            open_record = OrderLifecycle(
                trade_key="open-economic-headroom",
                trade_id="open-economic-headroom",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                firm_required_pnl=Decimal("-1.01"),
                lighter_client_order_id=78,
                lighter_client_order_ids=[78],
                lighter_reduce_only=False,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=open_record,
                lighter_side="BUY",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.economic_limit_price_i, 10_101)
            self.assertEqual(snapshot.price_i, 10_101)

        asyncio.run(run_case())

    def test_normal_strategy_close_uses_fresh_depth_and_economic_limit(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("asks", [["101", "2"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1236
            runtime.lighter_book_received_monotonic = time.monotonic()
            normal_close = OrderLifecycle(
                trade_key="normal-close",
                trade_id="normal-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                firm_required_pnl=Decimal("-1.01"),
                strategy_phase="close",
                strategy_tag=main_module.ADAPTIVE_MODEL_VERSION,
                lighter_client_order_id=79,
                lighter_client_order_ids=[79],
                lighter_reduce_only=True,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=normal_close,
                lighter_side="BUY",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.economic_limit_price_i, 10_101)
            self.assertEqual(snapshot.price_i, 10_101)

        asyncio.run(run_case())

    def test_normal_strategy_close_never_falls_back_without_economic_limit(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("asks", [["101", "2"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1237
            runtime.lighter_book_received_monotonic = time.monotonic()
            incomplete_close = OrderLifecycle(
                trade_key="incomplete-normal-close",
                trade_id="incomplete-normal-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                strategy_phase="close",
                strategy_tag=main_module.ADAPTIVE_MODEL_VERSION,
                lighter_reduce_only=True,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=incomplete_close,
                lighter_side="BUY",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(snapshot)
            self.assertIn("economic limit", error or "")

        asyncio.run(run_case())

    def test_normal_strategy_close_recovers_after_protected_retries_without_pause(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = 1_000
            normal_close = OrderLifecycle(
                trade_key="retry-normal-close",
                trade_id="retry-normal-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                firm_required_pnl=Decimal("-1"),
                strategy_phase="close",
                strategy_tag=main_module.ADAPTIVE_MODEL_VERSION,
                lighter_reduce_only=True,
            )
            attempts = 0
            submitted_prices = []

            async def snapshot(**kwargs):
                nonlocal attempts
                if kwargs.get("force_reduce_only_recovery"):
                    return (
                        main_module.LighterHedgeDispatchSnapshot(
                            market_generation=runtime.market_generation,
                            market_index=runtime.lighter_market_index,
                            base_amount=1_000,
                            price_i=20_000,
                            marginal_price_i=10_100,
                            economic_limit_price_i=None,
                            order_book_nonce=1,
                            quote_age_ms=0,
                        ),
                        None,
                    )
                attempts += 1
                return None, "protected IOC price unavailable"

            async def persist_noop():
                return None

            async def submit(**kwargs):
                submitted_prices.append(kwargs["price"])
                return type("Receipt", (), {"code": 0, "tx_hash": "recovery"})(), None

            runtime.capture_lighter_hedge_dispatch_snapshot = snapshot
            runtime.persist_runtime_state = persist_noop
            runtime.submit_lighter_create_order = submit

            await runtime._run_lighter_order_task(normal_close)

            self.assertEqual(
                attempts,
                runtime.strategy_config.lighter_hedge_max_attempts,
            )
            self.assertEqual(submitted_prices, [20_000])
            self.assertEqual(normal_close.hedge_status, "submitted")
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_exhausted_close_limit_sweeps_without_another_protected_retry(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = 1_000
            normal_close = OrderLifecycle(
                trade_key="exhausted-normal-close",
                trade_id="exhausted-normal-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                firm_required_pnl=Decimal("-1"),
                strategy_phase="close",
                strategy_tag=main_module.ADAPTIVE_MODEL_VERSION,
                lighter_reduce_only=True,
                lighter_client_order_ids=[11],
            )
            protected_attempts = 0
            submitted_prices = []

            async def snapshot(**kwargs):
                nonlocal protected_attempts
                if kwargs.get("force_reduce_only_recovery"):
                    return (
                        main_module.LighterHedgeDispatchSnapshot(
                            market_generation=runtime.market_generation,
                            market_index=runtime.lighter_market_index,
                            base_amount=1_000,
                            price_i=1,
                            marginal_price_i=9_900,
                            economic_limit_price_i=None,
                            order_book_nonce=2,
                            quote_age_ms=0,
                        ),
                        None,
                    )
                protected_attempts += 1
                return None, main_module.LIGHTER_IOC_LIMIT_EXHAUSTED

            async def persist_noop():
                return None

            async def submit(**kwargs):
                submitted_prices.append(kwargs["price"])
                return type("Receipt", (), {"code": 0, "tx_hash": "recovery"})(), None

            runtime.capture_lighter_hedge_dispatch_snapshot = snapshot
            runtime.persist_runtime_state = persist_noop
            runtime.submit_lighter_create_order = submit

            await runtime._run_lighter_order_task(normal_close)

            self.assertEqual(protected_attempts, 1)
            self.assertEqual(submitted_prices, [1])
            self.assertEqual(normal_close.hedge_status, "submitted")
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_reduce_only_first_send_uses_mandatory_market_sweep(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("asks", [["101", "2"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1236
            runtime.lighter_book_received_monotonic = time.monotonic()
            protective_close = OrderLifecycle(
                trade_key="protective-close",
                trade_id="protective-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                firm_required_pnl=Decimal("-1.01"),
                lighter_client_order_id=80,
                lighter_client_order_ids=[80],
                lighter_reduce_only=True,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=protective_close,
                lighter_side="BUY",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.price_i, 20_200)

        asyncio.run(run_case())

    def test_reduce_only_close_is_not_blocked_by_stale_or_insufficient_depth(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("bids", [["100", "0.1"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_sequence_gap = True
            runtime.lighter_order_book_nonce = 321
            runtime.lighter_book_received_monotonic = time.monotonic() - 10
            protective_close = OrderLifecycle(
                trade_key="mandatory-close",
                trade_id="mandatory-close",
                side="buy",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                firm_price=Decimal("100"),
                lighter_reduce_only=True,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=protective_close,
                lighter_side="SELL",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.price_i, 1)

        asyncio.run(run_case())

    def test_reduce_only_close_can_fall_back_to_committed_var_fill_without_book(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            protective_close = OrderLifecycle(
                trade_key="bookless-close",
                trade_id="bookless-close",
                side="sell",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                var_fill_price=Decimal("100"),
                lighter_reduce_only=True,
            )

            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=protective_close,
                lighter_side="BUY",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )

            self.assertIsNone(error)
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.price_i, 20_000)
            self.assertEqual(snapshot.quote_age_ms, -1)

        asyncio.run(run_case())

    def test_stale_or_insufficient_depth_is_rejected_before_submission(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("bids", [["100", "1"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 88
            runtime.lighter_book_received_monotonic = time.monotonic()
            missing, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=record(),
                lighter_side="SELL",
                base_amount=2_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )
            self.assertIsNone(missing)
            self.assertIn("full depth", error or "")
            runtime.lighter_book_received_monotonic = time.monotonic() - 10
            stale, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=record(),
                lighter_side="SELL",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )
            self.assertIsNone(stale)
            self.assertIn("stale", error or "")

        asyncio.run(run_case())
