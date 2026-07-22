from __future__ import annotations

import asyncio
import json
import os
import unittest
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

from main import (
    AccountReconcileOutcome,
    OrderLifecycle,
    VariationalToLighterRuntime,
)
from variational.listener import VariationalMonitor


def portfolio_payload(
    *,
    qty: str | None,
    published_at: datetime,
    updated_at: datetime | None = None,
) -> dict:
    positions = []
    if qty is not None:
        positions.append(
            {
                "position_info": {
                    "instrument": {"underlying": "BTC"},
                    "qty": qty,
                    "avg_entry_price": "100",
                    "updated_at": (updated_at or published_at).isoformat(),
                },
                "value": "1",
                "upnl": "0",
                "rpnl": "0",
            }
        )
    return {
        "positions": positions,
        "published_at": published_at.isoformat(),
        "pool_portfolio_result": {"balance": "100", "upnl": "0"},
    }


def filled_record(key: str, side: str, timestamp: datetime) -> OrderLifecycle:
    return OrderLifecycle(
        trade_key=key,
        trade_id=key,
        side=side,
        qty=Decimal("0.00798"),
        asset="BTC",
        auto_hedge_enabled=True,
        last_variational_status="filled",
        var_fill_price=Decimal("100"),
        var_fill_ts_iso=timestamp.isoformat(),
        lighter_fill_price=Decimal("100"),
        lighter_filled_qty=Decimal("0.00798"),
        lighter_fill_ts_iso=timestamp.isoformat(),
        hedge_status="filled",
    )


class ReconcileRuntime(VariationalToLighterRuntime):
    def __init__(self) -> None:
        super().__init__(Namespace(auto_hedge=True, lang="zh"))
        self.variational_ticker = "BTC"
        self.base_amount_multiplier = 1_000_000
        self.persist_count = 0
        self.lighter_position = Decimal("0")
        self.active_orders = 0
        self.lighter_error: Exception | None = None
        self.lighter_queries = 0

    async def get_lighter_account_snapshot(self) -> tuple[Decimal, int]:
        self.lighter_queries += 1
        if self.lighter_error is not None:
            raise self.lighter_error
        return self.lighter_position, self.active_orders

    async def persist_runtime_state(self) -> None:
        self.persist_count += 1


class ReconciliationFreshnessTests(unittest.TestCase):
    def test_portfolio_websocket_metadata_tracks_stream_and_content(self) -> None:
        async def run_case() -> None:
            monitor = VariationalMonitor()
            now = datetime.now(timezone.utc)
            payload = portfolio_payload(qty="0.01", published_at=now)
            await monitor.process_ws_event(
                {
                    "kind": "ws_frame",
                    "direction": "received",
                    "requestId": "cdp-portfolio-stream",
                    "url": "wss://omni.variational.io/portfolio",
                    "timestamp": now.isoformat(),
                    "opcode": 1,
                    "payloadData": json.dumps(payload),
                }
            )

            state = await monitor.get_trading_state()
            self.assertEqual(
                state["portfolio_request_id"],
                "cdp-portfolio-stream",
            )
            self.assertEqual(state["portfolio_published_at"], now.isoformat())
            self.assertTrue(state["portfolio_fingerprint"])
            self.assertEqual(state["portfolio_content_revision"], 1)

        asyncio.run(run_case())

    def test_reconcile_degradation_blocks_open_but_allows_existing_close(self) -> None:
        runtime = ReconcileRuntime()
        runtime.automation_ready = False
        runtime.automation_paused = False

        self.assertFalse(
            runtime.automation_can_submit_var_order("last_auto_var_order_status")
        )
        self.assertTrue(
            runtime.automation_can_submit_var_order(
                "last_auto_var_close_status",
                allow_reconcile_degraded=True,
            )
        )

        runtime.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MISMATCH
        self.assertFalse(
            runtime.automation_can_submit_var_order(
                "last_auto_var_close_status",
                allow_reconcile_degraded=True,
            )
        )
        self.assertIn("fresh account mismatch", runtime.last_auto_var_close_status)

        runtime.last_reconcile_outcome = AccountReconcileOutcome.UNKNOWN
        runtime.pause_automation("non-reconciliation execution failure")
        self.assertFalse(
            runtime.automation_can_submit_var_order(
                "last_auto_var_close_status",
                allow_reconcile_degraded=True,
            )
        )

    def test_stale_position_after_confirmed_close_never_pauses(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            opened = filled_record("open", "buy", now - timedelta(minutes=31))
            closed = filled_record("close", "sell", now)
            runtime.records = {"open": opened, "close": closed}
            runtime.record_order.extend(("open", "close"))

            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(
                    qty="0.00798",
                    published_at=now + timedelta(seconds=1),
                    updated_at=now - timedelta(seconds=10),
                ),
                request_id="portfolio-stream-1",
                captured_at=(now + timedelta(seconds=1)).isoformat(),
            )

            self.assertFalse(await runtime.reconcile_accounts())
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.STALE,
            )
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(runtime.lighter_queries, 0)

            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(
                    qty=None,
                    published_at=now + timedelta(seconds=2),
                ),
                request_id="portfolio-stream-1",
                captured_at=(now + timedelta(seconds=2)).isoformat(),
            )
            self.assertTrue(await runtime.reconcile_accounts())
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.FRESH_MATCH,
            )
            self.assertTrue(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_unchanged_flat_portfolio_does_not_expire_by_wall_clock(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(qty=None, published_at=now),
                request_id="portfolio-stream-flat",
                captured_at=now.isoformat(),
            )
            assert runtime.runtime.monitor._portfolio_received_monotonic is not None
            runtime.runtime.monitor._portfolio_received_monotonic -= 60

            self.assertTrue(await runtime.reconcile_accounts())
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.FRESH_MATCH,
            )
            self.assertTrue(runtime.automation_ready)
            self.assertEqual(runtime.lighter_queries, 1)

        asyncio.run(run_case())

    def test_lighter_tls_failure_is_unknown_and_deduplicated(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(qty=None, published_at=now),
                request_id="portfolio-stream-tls",
                captured_at=now.isoformat(),
            )
            runtime.lighter_error = RuntimeError("simulated TLS handshake timeout")

            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(await runtime.reconcile_accounts())
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.UNKNOWN,
            )
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(runtime.persist_count, 1)

        asyncio.run(run_case())

    def test_only_distinct_fresh_snapshots_over_five_seconds_pause(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            first = portfolio_payload(qty="0.01", published_at=now)
            runtime.runtime.monitor._update_portfolio(
                first,
                request_id="portfolio-stream-mismatch",
                captured_at=now.isoformat(),
            )

            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(runtime.automation_paused)
            assert runtime._reconcile_mismatch_first_monotonic is not None
            runtime._reconcile_mismatch_first_monotonic -= 5.1

            runtime.runtime.monitor._update_portfolio(
                first,
                request_id="portfolio-stream-mismatch",
                captured_at=(now + timedelta(seconds=6)).isoformat(),
            )
            self.assertFalse(await runtime.reconcile_accounts())
            self.assertFalse(runtime.automation_paused)

            second = portfolio_payload(
                qty="0.01",
                published_at=now + timedelta(seconds=6),
                updated_at=now,
            )
            runtime.runtime.monitor._update_portfolio(
                second,
                request_id="portfolio-stream-mismatch",
                captured_at=(now + timedelta(seconds=6)).isoformat(),
            )
            self.assertFalse(
                await runtime.reconcile_accounts(after_page_refresh=True)
            )
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.FRESH_MISMATCH,
            )
            self.assertTrue(runtime.automation_paused)

        asyncio.run(run_case())

    def test_quantized_manual_hedge_clears_restored_reconciliation_pause(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            runtime.base_amount_multiplier = 100_000
            now = datetime.now(timezone.utc)
            opened = filled_record("manual-open", "sell", now)
            opened.qty = Decimal("0.003091")
            opened.strategy_tag = "manual"
            opened.strategy_phase = "open"
            runtime.records = {opened.trade_key: opened}
            runtime.record_order.append(opened.trade_key)
            runtime.lighter_position = Decimal("0.00309")
            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(qty="-0.003091", published_at=now),
                request_id="portfolio-stream-manual-hedge",
                captured_at=now.isoformat(),
            )
            reason = (
                "Account reconciliation failed: FRESH_MISMATCH: "
                "Var 0/-0.003091, Lighter 0/0.00309, active=0"
            )
            runtime.pause_automation(reason)
            runtime._reconcile_pause_reason = reason

            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertTrue(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.FRESH_MATCH,
            )

        asyncio.run(run_case())

    def test_confirmed_double_flat_discards_only_stale_manual_runtime_open(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            opened = filled_record(
                "stale-manual-open",
                "sell",
                now - timedelta(seconds=1),
            )
            opened.qty = Decimal("0.003091")
            opened.strategy_tag = "manual"
            opened.strategy_phase = "open"
            runtime.records = {opened.trade_key: opened}
            runtime.record_order.append(opened.trade_key)
            runtime.lighter_position = Decimal("0")
            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(qty=None, published_at=now),
                request_id="portfolio-stream-double-flat",
                captured_at=now.isoformat(),
            )
            reason = (
                "Account reconciliation failed: FRESH_MISMATCH: "
                "Var 0/-0.003091, Lighter 0/0.00309, active=0"
            )
            runtime.pause_automation(reason)
            runtime._reconcile_pause_reason = reason

            self.assertTrue(await runtime.reconcile_accounts(allow_resume=True))
            self.assertEqual(runtime.records, {})
            self.assertEqual(tuple(runtime.record_order), ())
            self.assertTrue(runtime.automation_ready)
            self.assertFalse(runtime.automation_paused)
            self.assertGreater(runtime.round_cooldown_remaining_seconds(), 0)

        asyncio.run(run_case())

    def test_confirmed_double_flat_never_discards_adaptive_runtime_open(self) -> None:
        async def run_case() -> None:
            runtime = ReconcileRuntime()
            now = datetime.now(timezone.utc)
            opened = filled_record(
                "adaptive-open",
                "sell",
                now - timedelta(seconds=1),
            )
            opened.qty = Decimal("0.003091")
            opened.strategy_tag = "adaptive-median-v6"
            opened.strategy_phase = "open"
            runtime.records = {opened.trade_key: opened}
            runtime.record_order.append(opened.trade_key)
            runtime.runtime.monitor._update_portfolio(
                portfolio_payload(qty=None, published_at=now),
                request_id="portfolio-stream-adaptive-flat",
                captured_at=now.isoformat(),
            )

            self.assertFalse(await runtime.reconcile_accounts(allow_resume=True))
            self.assertIn(opened.trade_key, runtime.records)
            self.assertEqual(
                runtime.last_reconcile_outcome,
                AccountReconcileOutcome.FRESH_MISMATCH,
            )

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
