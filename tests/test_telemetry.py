import asyncio
import json
import os
import tempfile
import time
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import main as main_module
from main import OrderLifecycle
from variational.telemetry import AsyncJsonlWriter

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


class TelemetryTests(unittest.TestCase):
    def test_market_observation_records_three_var_timestamps(self) -> None:
        async def run_case() -> None:
            runtime = main_module.VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.market_generation = 1
            runtime.base_amount_multiplier = 1_000_000
            runtime.price_multiplier = 100
            runtime.lighter_order_book = {
                "bids": {Decimal("100.2"): Decimal("10")},
                "asks": {Decimal("100.3"): Decimal("10")},
            }
            runtime.lighter_order_book_ticks = {
                "bids": {10_020: 10_000_000},
                "asks": {10_030: 10_000_000},
            }
            runtime.lighter_best_bid = Decimal("100.2")
            runtime.lighter_best_ask = Decimal("100.3")
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_order_book_ready = True
            runtime.lighter_book_received_monotonic = time.monotonic()
            quote_time = datetime.now(timezone.utc).isoformat()
            runtime.runtime.monitor.quotes["BTC"] = {
                "bid": "100",
                "ask": "100.1",
                "timestamp": quote_time,
                "captured_at": quote_time,
                "received_at": quote_time,
                "received_monotonic": time.monotonic(),
            }

            frame, observation = await runtime.current_adaptive_market_frame()

            self.assertIsNotNone(frame)
            self.assertEqual(
                observation["telemetry_schema"],
                main_module.STRATEGY_TELEMETRY_SCHEMA,
            )
            self.assertEqual(observation["sample_class"], "market_observation")
            self.assertIsNotNone(observation["var_quote_timestamp_ms"])
            self.assertIsNotNone(observation["var_bridge_captured_at_ms"])
            self.assertIsNotNone(observation["var_server_received_at_ms"])

        asyncio.run(run_case())

    def test_depth_features_keep_compact_band_totals(self) -> None:
        features = main_module.lighter_depth_features(
            {
                "bids": {Decimal("100"): Decimal("2")},
                "asks": {Decimal("100.01"): Decimal("1")},
            },
            Decimal("100"),
            Decimal("100.01"),
        )

        one_bps = features["bands_bps"]["1"]
        self.assertEqual(one_bps["bid_usd"], Decimal("200"))
        self.assertEqual(one_bps["ask_usd"], Decimal("100.01"))
        self.assertGreater(one_bps["imbalance"], Decimal("0"))

    def test_survival_observation_is_deduplicated_and_never_submits(self) -> None:
        async def run_case() -> None:
            from tests.test_dashboard_calculations import make_open_candidate

            runtime = main_module.VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            candidate = make_open_candidate(runtime)
            frame = runtime.last_market_frame
            assert frame is not None
            runtime.active_parameter_epoch = candidate.epoch
            runtime.trace_writer = object()
            order_calls: list[dict] = []

            async def record_order_call(**kwargs):
                order_calls.append(kwargs)

            runtime.runtime.command_broker.request_place_order = record_order_call
            runtime.base_amount_multiplier = 1_000_000
            runtime.price_multiplier = 100
            runtime.lighter_order_book = {
                "bids": {Decimal("100.2"): Decimal("10")},
                "asks": {Decimal("100.3"): Decimal("10")},
            }
            runtime.lighter_best_bid = Decimal("100.2")
            runtime.lighter_best_ask = Decimal("100.3")
            runtime.lighter_order_book_nonce = 1
            runtime.lighter_order_book_ready = True
            runtime.lighter_book_received_monotonic = asyncio.get_running_loop().time()
            events: list[tuple[str, dict]] = []
            runtime.trace_event = lambda event, _trace_id, **fields: events.append(
                (event, fields)
            )
            observation = {
                "valid": True,
                "var_age_ms": 1,
                "lighter_age_ms": 1,
                "source_skew_ms": 0,
            }

            with (
                patch.object(main_module, "OPEN_SURVIVAL_HORIZONS_MS", (0, 1, 2)),
                patch.object(main_module, "MICROSTRUCTURE_HORIZONS_MS", (1, 2)),
            ):
                self.assertTrue(
                    runtime.schedule_open_survival_observation(frame, observation)
                )
                self.assertFalse(
                    runtime.schedule_open_survival_observation(frame, observation)
                )
                tasks = list(runtime.open_survival_tasks)
                await asyncio.gather(*tasks)

            self.assertEqual(len(events), 1)
            event, fields = events[0]
            self.assertEqual(event, main_module.OPEN_SURVIVAL_OBSERVATION_VERSION)
            self.assertEqual(fields["sample_kind"], "observe_threshold_candidate")
            self.assertEqual(fields["sample_class"], fields["sample_kind"])
            self.assertEqual(fields["sample_family"], "open_survival")
            self.assertEqual(
                fields["telemetry_schema"],
                main_module.STRATEGY_TELEMETRY_SCHEMA,
            )
            self.assertTrue(fields["sample_id"].startswith("survival-"))
            self.assertTrue(fields["threshold_pass_sides"])
            self.assertFalse(fields["policy_pass_sides"])
            self.assertEqual(
                fields["open_survival_policy_version"],
                main_module.OPEN_SURVIVAL_POLICY_VERSION,
            )
            self.assertIn("horizons_ms", fields["microstructure"])
            self.assertEqual([row["target_offset_ms"] for row in fields["snapshots"]], [0, 1, 2])
            self.assertEqual(
                [row["target_offset_ms"] for row in fields["feature_snapshots"]],
                [1, 2],
            )
            self.assertTrue(all(row["available"] for row in fields["snapshots"]))
            self.assertEqual(order_calls, [])

        asyncio.run(run_case())

    def test_microstructure_features_separate_book_and_trade_flow(self) -> None:
        runtime = main_module.VariationalToLighterRuntime(
            Namespace(auto_hedge=True, lang="zh")
        )
        runtime.base_amount_multiplier = 1
        runtime.price_multiplier = 1
        runtime.lighter_order_book = {
            "bids": {Decimal("100"): Decimal("2")},
            "asks": {Decimal("101"): Decimal("2")},
        }
        runtime.lighter_order_book_ticks = {
            "bids": {100: 2},
            "asks": {101: 2},
        }
        runtime.lighter_best_bid = Decimal("100")
        runtime.lighter_best_ask = Decimal("101")

        bid_flow = runtime.update_lighter_order_book(
            "bids", [["100", "3"]], record_flow=True
        )
        ask_flow = runtime.update_lighter_order_book(
            "asks", [["101", "3"]], record_flow=True
        )
        runtime.refresh_lighter_best_prices_locked()
        runtime.record_lighter_book_microstructure(bid_flow + ask_flow, now=1.0)
        self.assertEqual(bid_flow, Decimal("100"))
        self.assertEqual(ask_flow, Decimal("-101"))

        trades = [
            {"trade_id": 1, "usd_amount": "100", "is_maker_ask": True},
            {"trade_id": 2, "usd_amount": "40", "is_maker_ask": False},
        ]
        self.assertEqual(runtime.record_lighter_public_trades(trades, now=1.0), 2)
        self.assertEqual(runtime.record_lighter_public_trades(trades, now=1.0), 0)

        features = runtime.lighter_microstructure_features(now=1.0)
        one_second = features["horizons_ms"]["1000"]
        self.assertEqual(one_second["book_flow_usd"], Decimal("-1"))
        self.assertEqual(one_second["trade_flow_usd"], Decimal("60"))
        self.assertEqual(one_second["bid_added_usd"], Decimal("100"))
        self.assertEqual(one_second["bid_removed_usd"], Decimal("0"))
        self.assertEqual(one_second["ask_added_usd"], Decimal("101"))
        self.assertEqual(one_second["ask_removed_usd"], Decimal("0"))
        self.assertEqual(features["microprice_bps"], Decimal("0"))

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

            async def fresh_marginal(**_kwargs):
                return Decimal("100.103"), Decimal("100.103"), 1, 12

            runtime.get_lighter_execution_snapshot = fresh_marginal

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
                    "open_firm_microstructure_guard",
                    "firm_quote_guard",
                    "open_precommit_depth_guard",
                    "execution_intent_prepared",
                    "execution_intent_committing",
                    "variational_commit_dispatch",
                    "variational_commit_result",
                ],
            )

        asyncio.run(run_case())


if __name__ == "__main__":
    unittest.main()
