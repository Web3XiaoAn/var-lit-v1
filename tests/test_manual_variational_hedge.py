from __future__ import annotations

import asyncio
import os
import unittest
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

from adaptive_strategy.serialization import open_candidate_to_payload
from main import ADAPTIVE_MODEL_VERSION, MANUAL_STRATEGY_TAG, OrderLifecycle, VariationalToLighterRuntime
from tests.test_adaptive_strategy import candidate, frame, MODEL


class ManualCaptureRuntime(VariationalToLighterRuntime):
    def __init__(self) -> None:
        super().__init__(Namespace(auto_hedge=True, lang="zh"))
        self.variational_ticker = "BTC"
        self.accepted_assets = {"BTC"}
        self.var_event_accept_after = datetime.now(timezone.utc) - timedelta(seconds=1)
        self.scheduled: list[OrderLifecycle] = []

    def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
        record.hedge_status = "queued"
        self.scheduled.append(record)
        return True

    async def persist_runtime_state(self) -> None:
        return None

    async def append_order_log(self, *_args, **_kwargs) -> None:
        return None


def manual_event(trade_id: str, side: str, qty: str) -> dict[str, str]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "trade_id": trade_id,
        "side": side,
        "qty": qty,
        "asset": "BTC",
        "status": "filled",
        "price": "100",
        "timestamp": now,
        "captured_at": now,
        "source_rfq": f"rfq-{trade_id}",
    }


class ManualVariationalHedgeTests(unittest.TestCase):
    def test_automatic_open_keeps_adaptive_context_frozen_at_intent_time(self) -> None:
        async def run_case() -> None:
            runtime = ManualCaptureRuntime()
            epoch = candidate(MODEL, now_ms=10_000)
            market = frame(
                at_ms=10_100,
                reference_buy=epoch.thresholds.buy.final + Decimal("0.001"),
                reference_sell=epoch.thresholds.sell.final - Decimal("0.001"),
            )
            frozen = runtime.strategy_engine.evaluate_open(
                frame=market,
                epoch=epoch,
                now_ms=10_100,
            ).open_candidate
            assert frozen is not None
            runtime.mark_var_intent_sent("open", "BUY", Decimal("200"))
            assert runtime.pending_var_intent is not None
            runtime.pending_var_intent.state = "VAR_COMMIT_AMBIGUOUS"
            runtime.pending_var_intent.firm_price = Decimal("100")
            runtime.pending_var_intent.firm_qty = Decimal("2")
            runtime.pending_var_intent.adaptive_strategy_context = open_candidate_to_payload(frozen)

            await runtime.process_variational_trade_event(
                manual_event("auto-open-frozen", "buy", "2")
            )

            self.assertEqual(len(runtime.scheduled), 1)
            record = runtime.scheduled[0]
            self.assertEqual(record.strategy_tag, ADAPTIVE_MODEL_VERSION)
            self.assertEqual(
                record.adaptive_strategy_context,
                open_candidate_to_payload(frozen),
            )
            self.assertEqual(record.open_notional_usd, Decimal("200"))

        asyncio.run(run_case())

    def test_fresh_manual_open_hedges_actual_qty_once(self) -> None:
        async def run_case() -> None:
            runtime = ManualCaptureRuntime()
            event = manual_event("manual-open", "buy", "3.25")

            await runtime.process_variational_trade_event(event)
            await runtime.process_variational_trade_event(event)

            self.assertEqual(len(runtime.records), 1)
            self.assertEqual(len(runtime.scheduled), 1)
            record = runtime.scheduled[0]
            self.assertEqual(record.qty, Decimal("3.25"))
            self.assertEqual(record.strategy_phase, "open")
            self.assertEqual(record.var_event_origin, "MANUAL_LIVE")
            self.assertEqual(record.strategy_tag, MANUAL_STRATEGY_TAG)
            self.assertIsNone(record.adaptive_strategy_context)
            self.assertEqual(record.open_notional_usd, Decimal("325.00"))
            self.assertFalse(record.lighter_reduce_only)
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_manual_full_close_uses_reduce_only_actual_qty(self) -> None:
        async def run_case() -> None:
            runtime = ManualCaptureRuntime()
            await runtime.process_variational_trade_event(
                manual_event("manual-open", "buy", "2")
            )
            open_record = runtime.scheduled[0]
            open_record.hedge_status = "filled"
            open_record.lighter_fill_price = Decimal("100.1")
            open_record.lighter_filled_qty = Decimal("2")

            await runtime.process_variational_trade_event(
                manual_event("manual-close", "sell", "2")
            )

            self.assertEqual(len(runtime.scheduled), 2)
            close_record = runtime.scheduled[1]
            self.assertEqual(close_record.qty, Decimal("2"))
            self.assertEqual(close_record.strategy_phase, "close")
            self.assertEqual(close_record.var_event_origin, "MANUAL_LIVE")
            self.assertTrue(close_record.lighter_reduce_only)

        asyncio.run(run_case())

    def test_manual_add_partial_and_reversal_require_recovery(self) -> None:
        async def run_case(side: str, qty: str) -> None:
            runtime = ManualCaptureRuntime()
            await runtime.process_variational_trade_event(
                manual_event("manual-open", "buy", "2")
            )
            open_record = runtime.scheduled[0]
            open_record.hedge_status = "filled"
            open_record.lighter_fill_price = Decimal("100.1")
            open_record.lighter_filled_qty = Decimal("2")

            await runtime.process_variational_trade_event(
                manual_event(f"unsupported-{side}-{qty}", side, qty)
            )

            self.assertEqual(len(runtime.scheduled), 1)
            self.assertTrue(runtime.automation_paused)
            recovery_records = [
                record
                for record in runtime.records.values()
                if record.execution_state == "RECOVERY_REQUIRED"
            ]
            self.assertEqual(len(recovery_records), 1)
            self.assertEqual(recovery_records[0].var_event_origin, "MANUAL_LIVE")

        for side, qty in (("buy", "1"), ("sell", "1"), ("sell", "3")):
            with self.subTest(side=side, qty=qty):
                asyncio.run(run_case(side, qty))


if __name__ == "__main__":
    unittest.main()
