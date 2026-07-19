import asyncio
import os
import time
import unittest
from argparse import Namespace
from decimal import Decimal

from main import (
    OrderLifecycle,
    VariationalToLighterRuntime,
    calculate_lighter_execution_ticks,
)


os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


class FixedPointOrderBookTests(unittest.TestCase):
    def test_integer_depth_returns_exact_vwap_and_marginal_price(self):
        execution = calculate_lighter_execution_ticks(
            {"bids": {10_010: 1_000, 10_000: 2_000}, "asks": {}},
            "SELL",
            1_500,
            price_multiplier=100,
            base_amount_multiplier=1_000,
        )
        self.assertEqual(execution, (Decimal("100.0666666666666666666666667"), Decimal("100")))

    def test_book_updates_cache_bbo_and_invalidate_target_vwap(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_book_received_monotonic = time.monotonic()
            async with runtime.lighter_order_book_lock:
                runtime.update_lighter_order_book("bids", [["100.10", "2"]])
                runtime.update_lighter_order_book("asks", [["100.20", "2"]])
                runtime.refresh_lighter_best_prices_locked()
            self.assertEqual(await runtime.get_lighter_best_bid_ask(), (Decimal("100.1"), Decimal("100.2")))
            first, marginal, _nonce, _age = await runtime.get_lighter_execution_snapshot(
                lighter_side="SELL", qty=Decimal("1")
            )
            self.assertEqual((first, marginal), (Decimal("100.1"), Decimal("100.1")))
            self.assertTrue(runtime.lighter_vwap_cache)

            async with runtime.lighter_order_book_lock:
                runtime.update_lighter_order_book("bids", [["100.30", "2"]])
                runtime.refresh_lighter_best_prices_locked()
            second, marginal, _nonce, _age = await runtime.get_lighter_execution_snapshot(
                lighter_side="SELL", qty=Decimal("1")
            )
            self.assertEqual((second, marginal), (Decimal("100.3"), Decimal("100.3")))

        asyncio.run(run_case())

    def test_firm_and_hedge_snapshots_reject_sequence_gap(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("bids", [["100.10", "3"]])
            runtime.update_lighter_order_book("asks", [["100.20", "3"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 7
            runtime.lighter_order_book_sequence_gap = True
            runtime.lighter_book_received_monotonic = time.monotonic()

            vwap, _marginal, nonce, age = await runtime.get_lighter_execution_snapshot(
                lighter_side="SELL",
                qty=Decimal("1"),
            )
            self.assertIsNone(vwap)
            self.assertEqual(nonce, 7)
            self.assertIsNone(age)

            record = OrderLifecycle(
                trade_key="sequence-gap",
                trade_id="sequence-gap",
                side="buy",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
            )
            snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                record=record,
                lighter_side="SELL",
                base_amount=1_000,
                market_generation=runtime.market_generation,
                market_index=runtime.lighter_market_index,
            )
            self.assertIsNone(snapshot)
            self.assertIn("sequence", error or "")

        asyncio.run(run_case())

    def test_book_rejects_levels_outside_exchange_tick_precision(self):
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.price_multiplier = 100
        runtime.base_amount_multiplier = 1_000
        with self.assertRaises(ValueError):
            runtime.update_lighter_order_book("bids", [["100.001", "1"]])
        with self.assertRaises(ValueError):
            runtime.update_lighter_order_book("bids", [["100.00", "1.0001"]])

    def test_pre_send_fixed_point_snapshot_p95_is_under_one_millisecond(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.trace_writer = None
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.update_lighter_order_book("bids", [["100.10", "3"]])
            runtime.update_lighter_order_book("asks", [["100.20", "3"]])
            runtime.lighter_order_book_ready = True
            runtime.lighter_order_book_nonce = 8
            runtime.lighter_book_received_monotonic = time.monotonic()
            record = OrderLifecycle(
                trade_key="benchmark",
                trade_id="benchmark",
                side="buy",
                qty=Decimal("1"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
            )
            samples: list[int] = []
            for _ in range(10_000):
                started = time.perf_counter_ns()
                snapshot, error = await runtime.capture_lighter_hedge_dispatch_snapshot(
                    record=record,
                    lighter_side="SELL",
                    base_amount=1_000,
                    market_generation=runtime.market_generation,
                    market_index=runtime.lighter_market_index,
                )
                samples.append(time.perf_counter_ns() - started)
                self.assertIsNotNone(snapshot)
                self.assertIsNone(error)
            p95 = sorted(samples)[9_499]
            self.assertLess(p95, 1_000_000, f"fixed-point snapshot p95={p95}ns")

        asyncio.run(run_case())

    def test_cached_event_driven_market_frame_p95_is_under_one_millisecond(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.trace_writer = None
            runtime.variational_ticker = "BTC"
            runtime.market_generation = 1
            runtime.price_multiplier = 100
            runtime.base_amount_multiplier = 1_000
            runtime.strategy_config.max_quote_age_ms = 10_000
            received = time.monotonic()
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.quotes["BTC"] = {
                    "asset": "BTC",
                    "bid": "100.00",
                    "ask": "100.10",
                    "received_monotonic": received,
                }
            async with runtime.lighter_order_book_lock:
                runtime.update_lighter_order_book("bids", [["100.20", "10"]])
                runtime.update_lighter_order_book("asks", [["100.30", "10"]])
                runtime.refresh_lighter_best_prices_locked()
                runtime.lighter_order_book_ready = True
                runtime.lighter_order_book_nonce = 9
                runtime.lighter_book_received_monotonic = received

            self.assertTrue(await runtime.refresh_adaptive_market_frame_for_decision())
            samples: list[int] = []
            for _ in range(5_000):
                started = time.perf_counter_ns()
                refreshed = await runtime.refresh_adaptive_market_frame_for_decision()
                samples.append(time.perf_counter_ns() - started)
                self.assertTrue(refreshed)
            p95 = sorted(samples)[4_749]
            self.assertLess(p95, 1_000_000, f"market-frame adapter p95={p95}ns")

        asyncio.run(run_case())
