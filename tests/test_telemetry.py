import asyncio
import json
import os
import tempfile
import unittest
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import main as main_module
from main import OrderLifecycle
from variational.telemetry import AsyncJsonlWriter

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


class TelemetryTests(unittest.TestCase):
    def test_bounded_writer_drops_without_waiting_and_flushes_started_queue(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="trace-writer-") as tmp:
                path = Path(tmp) / "execution_trace.jsonl"
                writer = AsyncJsonlWriter(path, max_queue_size=1)

                self.assertTrue(writer.emit({"event": "first"}))
                self.assertFalse(writer.emit({"event": "dropped"}))
                self.assertEqual(writer.dropped_events, 1)

                writer.start()
                await writer.close()

                rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                self.assertEqual(rows, [{"event": "first"}])

        asyncio.run(run_case())

    def test_trace_event_records_monotonic_timestamp_without_disk_wait(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="trace-event-") as tmp:
                path = Path(tmp) / "execution_trace.jsonl"
                writer = AsyncJsonlWriter(path)
                runtime = SimpleNamespace(trace_writer=writer)

                main_module.VariationalToLighterRuntime.trace_event(
                    runtime,
                    "firm_quote_guard",
                    "trace-123",
                    allowed=True,
                    expected_pnl=Decimal("0.0123"),
                )
                writer.start()
                await writer.close()

                row = json.loads(path.read_text(encoding="utf-8").strip())
                self.assertEqual(row["event"], "firm_quote_guard")
                self.assertEqual(row["trace_id"], "trace-123")
                self.assertIsInstance(row["monotonic_ns"], int)
                self.assertEqual(row["expected_pnl"], "0.0123")

        asyncio.run(run_case())

    def test_rolling_writer_compacts_old_samples_off_hot_path(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="rolling-writer-") as tmp:
                path = Path(tmp) / "strategy_market_samples.jsonl"
                path.write_text(
                    "\n".join(
                        [
                            json.dumps({"sample_timestamp_ms": 0, "value": "expired"}),
                            json.dumps({"sample_timestamp_ms": 19 * 60_000, "value": "boundary"}),
                            "{partial-tail",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                writer = AsyncJsonlWriter(
                    path,
                    rolling_timestamp_field="sample_timestamp_ms",
                    rolling_keep_ms=61 * 60_000,
                    rolling_compaction_interval_ms=9 * 60_000,
                )
                writer.start()
                self.assertTrue(
                    writer.emit({"sample_timestamp_ms": 80 * 60_000, "value": "current"})
                )
                await writer.close()

                rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
                self.assertEqual([row["value"] for row in rows], ["boundary", "current"])
                self.assertEqual(writer.compactions, 1)
                self.assertEqual(writer.write_failures, 0)

        asyncio.run(run_case())

    def test_size_rotation_keeps_bounded_backups(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="rotating-writer-") as tmp:
                path = Path(tmp) / "execution_trace.jsonl"
                writer = AsyncJsonlWriter(
                    path,
                    max_file_bytes=48,
                    backup_count=2,
                )
                writer.start()
                for index in range(8):
                    self.assertTrue(writer.emit({"event": f"row-{index}"}))
                await writer.close()

                self.assertTrue(path.is_file())
                self.assertTrue(path.with_name(f"{path.name}.1").is_file())
                self.assertTrue(path.with_name(f"{path.name}.2").is_file())
                self.assertFalse(path.with_name(f"{path.name}.3").exists())
                self.assertEqual(writer.write_failures, 0)

        asyncio.run(run_case())

    def test_order_lifecycle_round_trips_trace_id(self) -> None:
        record = OrderLifecycle(
            trade_key="trace-record",
            trade_id="trace-record",
            side="buy",
            qty=Decimal("1"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="accepted",
            trace_id="trace-456",
        )

        restored = OrderLifecycle.from_payload(record.to_payload())

        self.assertIsNotNone(restored)
        self.assertEqual(restored.trace_id, "trace-456")

    def test_guarded_quote_and_commit_share_one_trace_id(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                def __init__(self) -> None:
                    self.calls: list[dict] = []

                async def request_place_order(self, **kwargs):
                    self.calls.append(kwargs)
                    if kwargs["fetch_stage"] == "quote":
                        return {
                            "type": "ORDER_RESULT",
                            "requestId": "quote-request",
                            "ok": True,
                            "detail": {
                                "quote": {
                                    "quoteId": "firm-quote-1",
                                    "firmPrice": "100",
                                    "firmQty": "2",
                                }
                            },
                        }
                    return {"type": "ORDER_RESULT", "requestId": "commit-request", "ok": True, "detail": {}}

            broker = FakeBroker()
            events: list[tuple[str, str | None, dict]] = []

            def trace_event(event: str, trace_id: str | None, **fields) -> None:
                events.append((event, trace_id, fields))

            async def fresh_open_vwaps(
                *,
                var_side: str,
                firm_price: Decimal,
                firm_qty: Decimal,
                reference_notional_usd: Decimal,
            ):
                self.assertEqual(var_side, "BUY")
                self.assertEqual(firm_price, Decimal("100"))
                self.assertEqual(firm_qty, Decimal("2"))
                self.assertEqual(reference_notional_usd, Decimal("500"))
                return Decimal("100.103"), Decimal("100.103"), 12

            from tests.test_dashboard_calculations import make_open_candidate

            runtime = main_module.VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.runtime.command_broker = broker
            runtime.trace_event = trace_event
            runtime.get_fresh_lighter_open_vwaps = fresh_open_vwaps

            async def no_persist() -> None:
                return None

            runtime.persist_runtime_state = no_persist
            open_candidate = make_open_candidate(runtime)

            result = await runtime.request_guarded_var_order(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                base_qty=None,
                open_candidate=open_candidate,
            )

            trace_id = result["trace_id"]
            self.assertTrue(trace_id)
            self.assertEqual([call["trace_id"] for call in broker.calls], [trace_id, trace_id])
            self.assertEqual(
                [event for event, event_trace_id, _fields in events if event_trace_id == trace_id],
                [
                    "variational_quote_dispatch",
                    "variational_quote_result",
                    "firm_quote_guard",
                    "execution_intent_prepared",
                    "execution_intent_committing",
                    "variational_commit_dispatch",
                    "variational_commit_result",
                ],
            )

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
