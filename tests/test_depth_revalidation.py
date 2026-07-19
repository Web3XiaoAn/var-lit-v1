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

    def test_frozen_firm_economics_do_not_narrow_post_commit_ioc(self):
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
            self.assertEqual(snapshot.price_i, 10_103)

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
