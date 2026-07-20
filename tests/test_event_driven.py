import asyncio
import os
import unittest
from argparse import Namespace
from decimal import Decimal
from types import SimpleNamespace

from main import OrderLifecycle, VariationalToLighterRuntime
from variational.listener import VariationalMonitor


os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


class EventDrivenRuntimeTests(unittest.TestCase):
    def test_quote_update_notifies_without_waiting_for_poll_interval(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            previous = runtime._market_signal_revision
            waiter = asyncio.create_task(runtime.wait_for_market_signal(previous))
            await asyncio.sleep(0)
            await runtime.runtime.monitor.process_rest_event(
                {
                    "kind": "rest_response",
                    "url": "https://api.example/api/quotes/indicative",
                    "body": '{"instrument":{"underlying":"BTC"},"bid":"100","ask":"101"}',
                    "timestamp": "2026-01-01T00:00:00Z",
                }
            )
            self.assertGreater(await asyncio.wait_for(waiter, timeout=0.1), previous)
            quote = runtime.runtime.monitor.quotes["BTC"]
            self.assertEqual(quote["captured_at"], "2026-01-01T00:00:00Z")
            self.assertIsNotNone(quote["received_at"])

        asyncio.run(run_case())

    def test_signal_revisions_coalesce_bursts(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            previous = runtime._market_signal_revision
            runtime.notify_market_signal()
            runtime.notify_market_signal()
            self.assertEqual(await runtime.wait_for_market_signal(previous), previous + 2)

        asyncio.run(run_case())

    def test_variational_quote_wakes_sampler_and_decision_without_io(self):
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        previous = runtime._market_signal_revision

        runtime.notify_variational_quote_signal()

        self.assertTrue(runtime._strategy_sample_event.is_set())
        self.assertEqual(runtime._market_signal_revision, previous + 1)

    def test_sampler_reacts_to_var_event_before_one_second_fallback(self):
        async def run_case():
            class ProbeRuntime(VariationalToLighterRuntime):
                def __init__(self):
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.captures = 0
                    self.first_capture = asyncio.Event()

                async def capture_strategy_sample_once(self, *, now_ms=None):
                    del now_ms
                    self.captures += 1
                    if self.captures == 1:
                        self.first_capture.set()
                    elif self.captures == 2:
                        self.stop_flag = True
                    return True

            runtime = ProbeRuntime()
            task = asyncio.create_task(runtime.strategy_sample_loop())
            await asyncio.wait_for(runtime.first_capture.wait(), timeout=0.1)
            runtime.notify_variational_quote_signal()
            await asyncio.wait_for(task, timeout=0.1)
            self.assertEqual(runtime.captures, 2)

        asyncio.run(run_case())

    def test_execution_reports_are_applied_by_one_queue_writer_in_order(self):
        async def run_case():
            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            applied = []

            async def apply(order):
                applied.append(order["client_order_id"])
                await asyncio.sleep(0)

            runtime._apply_lighter_fill_update = apply  # type: ignore[method-assign]
            runtime.execution_event_task = asyncio.create_task(runtime.execution_event_loop())
            try:
                await asyncio.gather(
                    runtime.handle_lighter_fill_update({"client_order_id": 1}),
                    runtime.handle_lighter_fill_update({"client_order_id": 2}),
                )
                self.assertEqual(applied, [1, 2])
            finally:
                runtime.execution_event_task.cancel()
                await asyncio.gather(runtime.execution_event_task, return_exceptions=True)

        asyncio.run(run_case())

    def test_monitor_revision_callbacks_cover_quote_and_trade(self):
        async def run_case():
            monitor = VariationalMonitor()
            callbacks = []
            monitor.on_quote_update = lambda: callbacks.append("quote")
            monitor.on_trade_event = lambda: callbacks.append("trade")
            await monitor.process_rest_event(
                {
                    "kind": "rest_response",
                    "url": "https://api.example/api/quotes/indicative",
                    "body": '{"instrument":{"underlying":"BTC"},"bid":"100","ask":"101"}',
                }
            )
            await monitor.process_ws_event(
                {
                    "kind": "ws_frame",
                    "direction": "received",
                    "url": "wss://api.example/events",
                    "payloadData": '{"type":"trade","data":{"id":"1","side":"buy","price":"100","qty":"1","instrument":{"underlying":"BTC"}}}',
                }
            )
            quote_revision, trade_revision = await monitor.get_update_revisions()
            self.assertEqual(callbacks, ["quote", "trade"])
            self.assertEqual((quote_revision, trade_revision), (1, 1))

        asyncio.run(run_case())

    def test_strategy_signal_loop_evaluates_only_open_when_no_position(self):
        async def run_case():
            class ProbeRuntime(VariationalToLighterRuntime):
                def __init__(self):
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls: list[str] = []

                async def wait_for_market_signal(self, _revision):
                    return 1

                async def _current_open_record(self):
                    return None

                async def _evaluate_auto_open_once(self, _current_open):
                    self.calls.append("open")
                    self.stop_flag = True

                async def _evaluate_auto_close_once(self, _current_open):
                    self.calls.append("close")

            runtime = ProbeRuntime()
            await runtime.strategy_signal_loop()
            self.assertEqual(runtime.calls, ["open"])

        asyncio.run(run_case())

    def test_strategy_signal_loop_refreshes_frame_before_open_decision(self):
        async def run_case():
            fresh_frame = SimpleNamespace(captured_at_ms=1_000_000)

            class ProbeRuntime(VariationalToLighterRuntime):
                def __init__(self):
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.saw_fresh_frame = False

                async def wait_for_market_signal(self, _revision):
                    return 1

                async def current_adaptive_market_frame(self):
                    return fresh_frame, {"valid": True, "rejection_reason": None}

                async def _current_open_record(self):
                    return None

                async def _evaluate_auto_open_once(self, _current_open):
                    self.saw_fresh_frame = self.last_market_frame is fresh_frame
                    self.stop_flag = True

            runtime = ProbeRuntime()
            runtime.last_market_frame = SimpleNamespace(captured_at_ms=1)
            await runtime.strategy_signal_loop()
            self.assertTrue(runtime.saw_fresh_frame)

        asyncio.run(run_case())

    def test_strategy_signal_loop_drains_new_trade_before_position_decision(self):
        async def run_case():
            class ProbeRuntime(VariationalToLighterRuntime):
                def __init__(self):
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls: list[str] = []

                async def wait_for_market_signal(self, _revision):
                    self._trade_signal_revision += 1
                    return 1

                async def drain_pending_trade_events(self):
                    self.calls.append("drain")
                    return 1

                async def _current_open_record(self):
                    self.calls.append("position")
                    return None

                async def _evaluate_auto_open_once(self, _current_open):
                    self.calls.append("open")
                    self.stop_flag = True

            runtime = ProbeRuntime()
            await runtime.strategy_signal_loop()
            self.assertEqual(runtime.calls, ["drain", "position", "open"])

        asyncio.run(run_case())

    def test_invalid_latest_adapter_frame_clears_previous_decision_frame(self):
        async def run_case():
            class ProbeRuntime(VariationalToLighterRuntime):
                async def current_adaptive_market_frame(self):
                    return None, {
                        "valid": False,
                        "rejection_reason": "market_data_stale",
                    }

            runtime = ProbeRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.last_market_frame = SimpleNamespace(captured_at_ms=1)
            self.assertFalse(await runtime.refresh_adaptive_market_frame_for_decision())
            self.assertIsNone(runtime.last_market_frame)

        asyncio.run(run_case())

    def test_strategy_signal_loop_evaluates_only_close_when_position_exists(self):
        async def run_case():
            class ProbeRuntime(VariationalToLighterRuntime):
                def __init__(self):
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.calls: list[str] = []

                async def wait_for_market_signal(self, _revision):
                    return 1

                async def _current_open_record(self):
                    return OrderLifecycle(
                        trade_key="open",
                        trade_id="open",
                        side="buy",
                        qty=Decimal("1"),
                        asset="BTC",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                    )

                async def _evaluate_auto_open_once(self, _current_open):
                    self.calls.append("open")

                async def _evaluate_auto_close_once(self, _current_open):
                    self.calls.append("close")
                    self.stop_flag = True

            runtime = ProbeRuntime()
            await runtime.strategy_signal_loop()
            self.assertEqual(runtime.calls, ["close"])

        asyncio.run(run_case())
