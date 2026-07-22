import asyncio
import os
import tempfile
import time
import unittest
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import main as main_module
from main import OrderLifecycle, VariationalToLighterRuntime
from variational.lighter_order_entry import LighterOrderEntryUnknown


os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")


class ExecutionSafetyTests(unittest.TestCase):
    def test_quote_timeout_does_not_pause_or_leave_a_commit_intent(self) -> None:
        async def run_case() -> None:
            class FakeBroker:
                async def extension_connected(self) -> bool:
                    return True

            class QuoteTimeoutRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.runtime.command_broker = FakeBroker()
                    self.logged = 0

                async def _auto_var_signal_for_current_open(self, _current_open):
                    self._selected_open_candidate = SimpleNamespace(
                        direction=SimpleNamespace(value="BUY")
                    )
                    return "BUY", Decimal("0.01")

                def live_open_block_reason(self):
                    return None

                def automation_can_submit_var_order(
                    self,
                    _status_attr,
                    *,
                    allow_reconcile_degraded=False,
                ):
                    return True

                async def request_guarded_var_order(self, **_kwargs):
                    return {
                        "ok": False,
                        "error": "Firm quote request timed out",
                    }

                async def append_auto_var_result_log(self, **_kwargs):
                    self.logged += 1

                async def persist_runtime_state(self):
                    return None

            runtime = QuoteTimeoutRuntime()
            runtime.variational_ticker = "BTC"
            await runtime._evaluate_auto_open_once(None)

            self.assertEqual(runtime.logged, 1)
            self.assertIsNone(runtime.pending_var_intent)
            self.assertFalse(runtime.automation_paused)
            self.assertIn("timed out", runtime.last_auto_var_order_status)

        asyncio.run(run_case())

    def test_fresh_flat_portfolio_confirms_commit_was_not_filled(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic() - 2,
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTED,
                commit_accepted_monotonic=time.monotonic() - 1,
                firm_price=Decimal("100"),
                firm_qty=Decimal("2"),
            )
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.positions["BTC"] = {
                    "qty": Decimal("0"),
                }
                runtime.runtime.monitor._portfolio_received_monotonic = (
                    time.monotonic()
                )

            outcome = await runtime.inspect_pending_var_intent_from_portfolio()

            self.assertIs(
                outcome,
                main_module.VarPortfolioRecoveryOutcome.CONFIRMED_NOT_FILLED,
            )
            self.assertIsNotNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_stale_portfolio_never_claims_an_accepted_commit_was_not_filled(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            runtime.variational_ticker = "BTC"
            runtime.pending_var_intent = main_module.VarOrderIntent(
                phase="open",
                side="BUY",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic() - 10,
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTED,
                commit_accepted_monotonic=time.monotonic() - 9,
                firm_price=Decimal("100"),
                firm_qty=Decimal("2"),
            )
            async with runtime.runtime.monitor._lock:
                runtime.runtime.monitor.positions["BTC"] = {
                    "qty": Decimal("0"),
                }
                runtime.runtime.monitor._portfolio_received_monotonic = (
                    time.monotonic()
                    - main_module.VAR_PORTFOLIO_RECOVERY_MAX_AGE_SECONDS
                    - 1
                )

            outcome = await runtime.inspect_pending_var_intent_from_portfolio()

            self.assertIs(
                outcome,
                main_module.VarPortfolioRecoveryOutcome.UNKNOWN,
            )
            self.assertIsNotNone(runtime.pending_var_intent)

        asyncio.run(run_case())

    def test_shutdown_waits_for_active_hedge_before_closing_transports(self) -> None:
        async def run_case() -> None:
            events: list[str] = []

            class FakeEntry:
                async def close(self):
                    events.append("entry")

            class CloseRuntime(VariationalToLighterRuntime):
                async def stop_lighter_streams(self):
                    events.append("streams")

                async def persist_runtime_state(self):
                    return None

            runtime = CloseRuntime(Namespace(auto_hedge=True, lang="zh"))

            async def stop_listener():
                events.append("listener")

            runtime.runtime.stop = stop_listener
            runtime.lighter_order_entry = FakeEntry()
            runtime.execution_event_task = asyncio.create_task(
                runtime.execution_event_loop()
            )

            async def hedge():
                await asyncio.sleep(0)
                events.append("hedge")

            hedge_task = asyncio.create_task(hedge())
            runtime.hedge_tasks.add(hedge_task)

            await runtime.close()

            self.assertLess(events.index("hedge"), events.index("streams"))
            self.assertLess(events.index("streams"), events.index("entry"))
            self.assertTrue(runtime.execution_event_task.done())

        asyncio.run(run_case())

    def test_requested_shutdown_does_not_misclassify_finished_watchdog(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            finished_watchdog = asyncio.create_task(asyncio.sleep(0))
            await finished_watchdog
            runtime.lighter_order_watchdog_task = finished_watchdog

            runtime.stop_flag = True
            runtime._raise_for_exited_critical_tasks()

        asyncio.run(run_case())

    def test_unexpected_finished_watchdog_still_fails_runtime(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            finished_watchdog = asyncio.create_task(asyncio.sleep(0))
            await finished_watchdog
            runtime.lighter_order_watchdog_task = finished_watchdog

            with self.assertRaisesRegex(
                RuntimeError,
                "Critical task exited unexpectedly: Lighter-order-watchdog",
            ):
                runtime._raise_for_exited_critical_tasks()

        asyncio.run(run_case())

    def test_accepted_commit_without_provisional_becomes_ambiguous_once(self) -> None:
        async def run_case() -> None:
            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.logged: list[tuple[str, dict]] = []
                    self.persist_count = 0

                async def append_order_log(self, event_type: str, payload: dict) -> None:
                    self.logged.append((event_type, payload))

                async def persist_runtime_state(self) -> None:
                    self.persist_count += 1

            runtime = CaptureRuntime()
            intent = main_module.VarOrderIntent(
                phase="emergency_close",
                side="SELL",
                amount=Decimal("200"),
                sent_monotonic=time.monotonic() - 5,
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTING,
                trace_id="accepted-no-provisional",
                firm_quote_id="firm-accepted-no-provisional",
                commit_accepted_monotonic=time.monotonic() - 4,
            )
            runtime.pending_var_intent = intent

            self.assertFalse(
                await runtime.rollback_unconfirmed_var_commit(
                    expected_intent=intent,
                )
            )
            self.assertTrue(await runtime.mark_unconfirmed_var_commit_ambiguous(intent))
            self.assertFalse(await runtime.mark_unconfirmed_var_commit_ambiguous(intent))

            self.assertIs(runtime.pending_var_intent, intent)
            self.assertEqual(intent.state, main_module.VAR_INTENT_COMMIT_AMBIGUOUS)
            self.assertIsNone(intent.commit_accepted_monotonic)
            self.assertTrue(runtime.automation_paused)
            self.assertEqual(
                runtime._reconcile_pause_reason,
                runtime.automation_pause_reason,
            )
            self.assertEqual(runtime.persist_count, 1)
            self.assertEqual(
                [event_type for event_type, _payload in runtime.logged],
                ["variational_commit_confirmation_unresolved"],
            )

        asyncio.run(run_case())

    def test_commit_confirmation_watchdog_refreshes_before_pausing(self) -> None:
        async def run_case() -> None:
            runtime = VariationalToLighterRuntime(
                Namespace(auto_hedge=True, lang="zh")
            )
            intent = main_module.VarOrderIntent(
                phase="close",
                side="BUY",
                amount=Decimal("500"),
                sent_monotonic=time.monotonic() - 5,
                market="BTC",
                state=main_module.VAR_INTENT_COMMITTING,
                sent_at_iso=main_module.utc_now(),
                commit_accepted_monotonic=time.monotonic() - 4,
            )
            runtime.pending_var_intent = intent

            async def stop_after_first_tick(_delay: float) -> None:
                runtime.stop_flag = True

            with patch.object(
                main_module.asyncio,
                "sleep",
                AsyncMock(side_effect=stop_after_first_tick),
            ), patch.object(
                runtime,
                "inspect_pending_var_intent_from_portfolio",
                AsyncMock(
                    side_effect=[
                        main_module.VarPortfolioRecoveryOutcome.UNKNOWN,
                        main_module.VarPortfolioRecoveryOutcome.UNKNOWN,
                        main_module.VarPortfolioRecoveryOutcome.FILLED,
                    ]
                ),
            ), patch.object(
                runtime,
                "refresh_pending_lighter_orders",
                AsyncMock(),
            ), patch.object(
                runtime,
                "refresh_variational_page_when_safe",
                AsyncMock(),
            ) as refresh, patch.object(
                runtime,
                "wait_for_authoritative_portfolio_after",
                AsyncMock(return_value=True),
            ) as wait, patch.object(
                runtime,
                "append_order_log",
                AsyncMock(),
            ):
                await runtime.var_intent_watchdog_loop()

            refresh.assert_awaited_once()
            wait.assert_awaited_once()
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_lighter_reconciliation_paginates_until_target_is_found(self) -> None:
        async def run_case() -> None:
            class PaginatedOrderApi:
                def __init__(self) -> None:
                    self.cursors: list[str | None] = []

                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

                async def account_inactive_orders(self, **kwargs):
                    cursor = kwargs.get("cursor")
                    self.cursors.append(cursor)
                    if cursor is None:
                        return SimpleNamespace(
                            orders=[{"client_order_id": "111"}],
                            next_cursor="page-2",
                        )
                    return SimpleNamespace(
                        orders=[{"client_order_id": "777"}],
                        next_cursor=None,
                    )

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = PaginatedOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.lighter_client = FakeLighterClient()

            outcome = await runtime.reconcile_lighter_client_order(777)

            self.assertIs(outcome, main_module.LighterOrderReconcileOutcome.FOUND)
            self.assertEqual(runtime.lighter_client.order_api.cursors, [None, "page-2"])

        asyncio.run(run_case())

    def test_incomplete_lighter_history_never_resends_recovery_order(self) -> None:
        async def run_case() -> None:
            class NeverEndingOrderApi:
                def __init__(self) -> None:
                    self.calls = 0

                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

                async def account_inactive_orders(self, **_kwargs):
                    self.calls += 1
                    return SimpleNamespace(
                        orders=[],
                        next_cursor=f"page-{self.calls + 1}",
                    )

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = NeverEndingOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            class CaptureRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.scheduled: list[str] = []

                def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
                    self.scheduled.append(record.trade_key)
                    return True

            runtime = CaptureRuntime()
            runtime.lighter_client = FakeLighterClient()
            record = OrderLifecycle(
                trade_key="recovery-history-incomplete",
                trade_id="recovery-history-incomplete",
                side="buy",
                qty=Decimal("2"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                lighter_reserved_client_order_id=999,
                hedge_status="recovery_check",
                execution_state=main_module.EXECUTION_STATE_RECOVERY_REQUIRED,
            )
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)

            with patch.object(main_module, "LIGHTER_INACTIVE_ORDER_MAX_PAGES", 2):
                outcome = await runtime.reconcile_lighter_client_order(999)
                await runtime.refresh_pending_lighter_orders()

            self.assertIs(outcome, main_module.LighterOrderReconcileOutcome.UNKNOWN)
            self.assertEqual(record.hedge_status, "recovery_check")
            self.assertEqual(runtime.scheduled, [])

        asyncio.run(run_case())

    def test_timed_out_ioc_recovers_fill_from_lighter_trades(self) -> None:
        async def run_case() -> None:
            first_client_id = 118710650652145
            second_client_id = 201195233646277

            class TradeRecoveryOrderApi:
                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

                async def account_inactive_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[], next_cursor=None)

                async def trades(self, **_kwargs):
                    return SimpleNamespace(
                        trades=[
                            {
                                "trade_id": 25515418273,
                                "ask_client_id": first_client_id,
                                "bid_client_id": 999,
                                "size": "0.00309",
                                "price": "63692.2",
                                "usd_amount": "196.808898",
                                "timestamp": 1784248293488,
                                "transaction_time": 1784248293508213,
                            }
                        ],
                        next_cursor=None,
                    )

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = TradeRecoveryOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            class NoDiskRuntime(VariationalToLighterRuntime):
                async def append_order_log(self, *_args, **_kwargs) -> None:
                    return None

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = NoDiskRuntime(Namespace(auto_hedge=True, lang="zh"))
            runtime.base_amount_multiplier = 100_000
            runtime.lighter_market_index = 1
            runtime.lighter_client = FakeLighterClient()
            record = OrderLifecycle(
                trade_key="timed-out-close-trade-recovery",
                trade_id="timed-out-close-trade-recovery",
                side="buy",
                qty=Decimal("0.003091"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                var_fill_price=Decimal("63650.87"),
                var_fill_source="portfolio",
                firm_guard_pnl=Decimal("0.16731583"),
                strategy_phase="close",
                lighter_reduce_only=True,
                lighter_side="SELL",
                lighter_client_order_id=second_client_id,
                lighter_client_order_ids=[first_client_id, second_client_id],
                lighter_submitted_at_iso="2020-01-01T00:00:00+00:00",
                hedge_status="uncertain",
                execution_state=main_module.EXECUTION_STATE_RECOVERY_REQUIRED,
            )
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[first_client_id] = record.trade_key
            runtime.lighter_client_order_to_trade_key[second_client_id] = record.trade_key

            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(record.hedge_status, "filled")
            self.assertEqual(record.lighter_filled_qty, Decimal("0.00309"))
            self.assertEqual(record.lighter_fill_price, Decimal("63692.2"))
            self.assertEqual(record.execution_loss_usd, Decimal("0.03960613"))
            self.assertFalse(runtime.automation_paused)

        asyncio.run(run_case())

    def test_timed_out_reduce_only_close_stays_running_when_accounts_are_flat(self) -> None:
        async def run_case() -> None:
            class EmptyOrderApi:
                async def account_active_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[])

                async def account_inactive_orders(self, **_kwargs):
                    return SimpleNamespace(orders=[], next_cursor=None)

            class FakeLighterClient:
                def __init__(self) -> None:
                    self.order_api = EmptyOrderApi()

                def create_auth_token_with_expiry(self, **_kwargs):
                    return "test-auth", None

            class FlatRuntime(VariationalToLighterRuntime):
                def __init__(self) -> None:
                    super().__init__(Namespace(auto_hedge=True, lang="zh"))
                    self.logged_events: list[str] = []

                async def reconcile_accounts(
                    self,
                    *,
                    allow_resume: bool = False,
                    after_page_refresh: bool = False,
                ) -> bool:
                    self.last_account_snapshot = main_module.AccountSnapshot(
                        var_position=Decimal("0"),
                        lighter_position=Decimal("0"),
                        lighter_active_orders=0,
                        captured_at=main_module.utc_now(),
                    )
                    return True

                async def append_order_log(self, event_type, _payload) -> None:
                    self.logged_events.append(event_type)

                async def persist_runtime_state(self) -> None:
                    return None

            runtime = FlatRuntime()
            runtime.lighter_client = FakeLighterClient()
            runtime.lighter_market_index = 1
            record = OrderLifecycle(
                trade_key="timed-out-flat-close",
                trade_id="timed-out-flat-close",
                side="buy",
                qty=Decimal("0.003091"),
                asset="BTC",
                auto_hedge_enabled=True,
                last_variational_status="filled",
                var_fill_price=Decimal("63650.87"),
                var_fill_source="portfolio",
                strategy_phase="close",
                lighter_reduce_only=True,
                lighter_side="SELL",
                lighter_client_order_id=123456,
                lighter_client_order_ids=[123456],
                lighter_submitted_at_iso="2020-01-01T00:00:00+00:00",
                hedge_status="uncertain",
                execution_state=main_module.EXECUTION_STATE_RECOVERY_REQUIRED,
            )
            runtime.records[record.trade_key] = record
            runtime.record_order.append(record.trade_key)
            runtime.lighter_client_order_to_trade_key[123456] = record.trade_key

            await runtime.refresh_pending_lighter_orders()

            self.assertEqual(record.hedge_status, "reconciled_flat")
            self.assertTrue(record.lighter_outcome_final)
            self.assertFalse(runtime.automation_paused)
            self.assertIn("lighter_close_reconciled_flat", runtime.logged_events)

        asyncio.run(run_case())

    def test_pruning_bounds_settled_state_without_evicting_unfinished_records(self) -> None:
        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="settled-state-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                ):
                    runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
                    for index in range(10_000):
                        side = "buy" if index % 2 == 0 else "sell"
                        client_order_id = index + 1
                        record = OrderLifecycle(
                            trade_key=f"settled:{index}",
                            trade_id=f"settled:{index}",
                            side=side,
                            qty=Decimal("1"),
                            asset="BTC",
                            auto_hedge_enabled=True,
                            last_variational_status="filled",
                            var_fill_price=Decimal("100"),
                            var_fill_source="event",
                            lighter_fill_price=Decimal("100"),
                            lighter_filled_qty=Decimal("1"),
                            hedge_status="filled",
                        )
                        record.lighter_client_order_id = client_order_id
                        record.lighter_client_order_ids = [client_order_id]
                        runtime.records[record.trade_key] = record
                        runtime.record_order.append(record.trade_key)
                        runtime.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
                        runtime.lighter_order_fill_totals[client_order_id] = (
                            Decimal("1"),
                            Decimal("100"),
                        )
                        runtime.lighter_order_terminal_ids.add(client_order_id)
                        runtime._canary_completed_close_keys.add(record.trade_key)
                        runtime._round_cooldown_close_keys.add(record.trade_key)
                        runtime._max_hold_alerted_trade_keys.add(record.trade_key)

                    unfinished = OrderLifecycle(
                        trade_key="unfinished",
                        trade_id="unfinished",
                        side="buy",
                        qty=Decimal("1"),
                        asset="BTC",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                        var_fill_price=Decimal("100"),
                        hedge_status="uncertain",
                    )
                    runtime.records[unfinished.trade_key] = unfinished
                    runtime.record_order.append(unfinished.trade_key)
                    runtime.lighter_retry_pending_keys.add(unfinished.trade_key)

                    removed = await runtime.prune_settled_execution_state()

                    self.assertEqual(removed, 9_900)
                    self.assertEqual(len(runtime.records), 101)
                    self.assertEqual(len(runtime.record_order), 101)
                    self.assertIn("unfinished", runtime.records)
                    self.assertIn("unfinished", runtime.lighter_retry_pending_keys)
                    self.assertNotIn(1, runtime.lighter_client_order_to_trade_key)
                    self.assertNotIn(1, runtime.lighter_order_fill_totals)
                    self.assertNotIn(1, runtime.lighter_order_terminal_ids)
                    self.assertNotIn("settled:0", runtime._canary_completed_close_keys)
                    self.assertNotIn("settled:0", runtime._round_cooldown_close_keys)
                    self.assertNotIn("settled:0", runtime._max_hold_alerted_trade_keys)
                    self.assertIn("settled:9999", runtime._canary_completed_close_keys)
                    self.assertIn(10_000, runtime.lighter_client_order_to_trade_key)

        asyncio.run(run_case())

    def test_order_entry_not_ready_falls_back_before_nonce_or_signing(self) -> None:
        class NotReadyEntry:
            is_ready = False

        class RestOnlyClient:
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 2
            DEFAULT_IOC_EXPIRY = 3

            def __init__(self) -> None:
                self.create_order_calls: list[dict[str, object]] = []

            @property
            def nonce_manager(self):
                raise AssertionError("nonce allocation must not run before REST fallback")

            def sign_create_order(self, **_kwargs):
                raise AssertionError("WebSocket signing must not run before REST fallback")

            async def create_order(self, **kwargs):
                self.create_order_calls.append(kwargs)
                return None, {"code": 0}, None

        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="order-entry-preflight-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                ):
                    runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
                    client = RestOnlyClient()
                    runtime.lighter_client = client
                    runtime.lighter_order_entry_enabled = True
                    runtime.lighter_order_entry_rest_fallback = True
                    runtime.lighter_order_entry = NotReadyEntry()

                    response, error = await runtime.submit_lighter_create_order(
                        market_index=1,
                        client_order_id=123,
                        base_amount=1_000,
                        price=50_000,
                        is_ask=False,
                        reduce_only=False,
                        trace_id="preflight",
                    )

                    self.assertEqual(response, {"code": 0})
                    self.assertIsNone(error)
                    self.assertEqual(len(client.create_order_calls), 1)

        asyncio.run(run_case())

    def test_unknown_websocket_send_never_falls_back_to_rest(self) -> None:
        class ReadyEntry:
            is_ready = True

            async def submit(self, **_kwargs):
                raise LighterOrderEntryUnknown("response timed out")

        class NonceManager:
            def next_nonce(self):
                return 1, 2

            def acknowledge_failure(self, _api_key_index):
                raise AssertionError("unknown send outcome must retain its nonce")

        class WebSocketClient:
            ORDER_TYPE_MARKET = 1
            ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL = 2
            DEFAULT_IOC_EXPIRY = 3

            def __init__(self) -> None:
                self.nonce_manager = NonceManager()
                self.rest_calls = 0

            def sign_create_order(self, **_kwargs):
                return 1, "{}", "signed", None

            async def create_order(self, **_kwargs):
                self.rest_calls += 1
                return None, {"code": 0}, None

        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="order-entry-unknown-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                ):
                    runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
                    client = WebSocketClient()
                    runtime.lighter_client = client
                    runtime.lighter_order_entry_enabled = True
                    runtime.lighter_order_entry_rest_fallback = True
                    runtime.lighter_order_entry = ReadyEntry()

                    response, error = await runtime.submit_lighter_create_order(
                        market_index=1,
                        client_order_id=123,
                        base_amount=1_000,
                        price=50_000,
                        is_ask=False,
                        reduce_only=False,
                        trace_id="unknown",
                    )

                    self.assertIsNone(response)
                    self.assertIn("timed out", error or "")
                    self.assertEqual(client.rest_calls, 0)

        asyncio.run(run_case())

    def test_ambiguous_lighter_send_retries_only_after_confirmed_absence(self) -> None:
        class ProbeRuntime(VariationalToLighterRuntime):
            def __init__(self) -> None:
                self.retry_count = 0
                super().__init__(Namespace(auto_hedge=True, lang="zh"))

            async def submit_lighter_create_order(self, **_kwargs):
                raise RuntimeError("connection dropped after send")

            async def reconcile_lighter_client_order(self, _client_order_id):
                return main_module.LighterOrderReconcileOutcome.CONFIRMED_ABSENT

            def queue_lighter_retry_after_current(self, _record):
                self.retry_count += 1
                return True

            async def append_order_log(self, *_args, **_kwargs):
                return None

            async def persist_runtime_state(self):
                return None

            async def emergency_flatten_var(self, _record):
                return None

        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="execution-safety-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                    patch.object(main_module, "LIGHTER_ERROR_CONFIRM_SECONDS", 0),
                ):
                    runtime = ProbeRuntime()
                    runtime.lighter_market_index = 1
                    runtime.base_amount_multiplier = 1_000
                    runtime.price_multiplier = 100
                    runtime.lighter_order_book = {
                        "bids": {Decimal("100"): Decimal("10")},
                        "asks": {Decimal("101"): Decimal("10")},
                    }
                    runtime.lighter_order_book_ready = True
                    runtime.lighter_order_book_nonce = 1
                    runtime.lighter_book_received_monotonic = time.monotonic()
                    runtime.lighter_best_bid = Decimal("100")
                    runtime.lighter_best_ask = Decimal("101")
                    record = OrderLifecycle(
                        trade_key="absent",
                        trade_id="absent",
                        side="buy",
                        qty=Decimal("1"),
                        asset="BTC",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                    )
                    runtime.records[record.trade_key] = record
                    runtime.record_order.append(record.trade_key)

                    await runtime.place_lighter_order(record)

                    self.assertEqual(runtime.retry_count, 1)
                    self.assertEqual(record.hedge_status, "retrying")

        asyncio.run(run_case())

    def test_ambiguous_lighter_send_never_retries_when_reconciliation_fails(self) -> None:
        class ProbeRuntime(VariationalToLighterRuntime):
            def __init__(self) -> None:
                self.retry_count = 0
                self.reconcile_count = 0
                super().__init__(Namespace(auto_hedge=True, lang="zh"))

            async def submit_lighter_create_order(self, **_kwargs):
                raise RuntimeError("connection dropped after send")

            async def reconcile_lighter_client_order(self, _client_order_id):
                self.reconcile_count += 1
                return main_module.LighterOrderReconcileOutcome.UNKNOWN

            def queue_lighter_retry_after_current(self, _record):
                self.retry_count += 1
                return True

            async def append_order_log(self, *_args, **_kwargs):
                return None

            async def persist_runtime_state(self):
                return None

            async def emergency_flatten_var(self, _record):
                return None

        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="execution-safety-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                    patch.object(main_module, "LIGHTER_ERROR_CONFIRM_SECONDS", 0),
                ):
                    runtime = ProbeRuntime()
                    runtime.lighter_market_index = 1
                    runtime.base_amount_multiplier = 1_000
                    runtime.price_multiplier = 100
                    runtime.lighter_order_book = {
                        "bids": {Decimal("100"): Decimal("10")},
                        "asks": {Decimal("101"): Decimal("10")},
                    }
                    runtime.lighter_order_book_ready = True
                    runtime.lighter_order_book_nonce = 1
                    runtime.lighter_book_received_monotonic = time.monotonic()
                    runtime.lighter_best_bid = Decimal("100")
                    runtime.lighter_best_ask = Decimal("101")
                    record = OrderLifecycle(
                        trade_key="ambiguous",
                        trade_id="ambiguous",
                        side="buy",
                        qty=Decimal("1"),
                        asset="BTC",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                    )
                    runtime.records[record.trade_key] = record
                    runtime.record_order.append(record.trade_key)

                    await runtime.place_lighter_order(record)

                    self.assertEqual(runtime.reconcile_count, 1)
                    self.assertEqual(runtime.retry_count, 0)
                    self.assertEqual(record.hedge_status, "uncertain")
                    self.assertEqual(record.execution_state, "RECOVERY_REQUIRED")
                    self.assertTrue(runtime.automation_paused)

        asyncio.run(run_case())

    def test_ambiguous_lighter_send_does_not_retry_active_partial_order(self) -> None:
        class ProbeRuntime(VariationalToLighterRuntime):
            def __init__(self) -> None:
                self.retry_count = 0
                super().__init__(Namespace(auto_hedge=True, lang="zh"))

            async def submit_lighter_create_order(self, **_kwargs):
                raise RuntimeError("connection dropped after partial exchange acceptance")

            async def reconcile_lighter_client_order(self, _client_order_id):
                record = self.records["active-partial"]
                record.lighter_filled_qty = Decimal("0.4")
                record.lighter_filled_quote = Decimal("40")
                record.lighter_fill_price = Decimal("100")
                record.lighter_outcome_final = False
                record.hedge_status = "partial"
                record.execution_state = main_module.EXECUTION_STATE_HEDGE_PARTIAL
                return main_module.LighterOrderReconcileOutcome.FOUND

            def queue_lighter_retry_after_current(self, _record):
                self.retry_count += 1
                return True

            async def append_order_log(self, *_args, **_kwargs):
                return None

            async def persist_runtime_state(self):
                return None

            async def emergency_flatten_var(self, _record):
                return None

        async def run_case() -> None:
            with tempfile.TemporaryDirectory(prefix="execution-partial-active-") as temp_dir:
                root = Path(temp_dir)
                with (
                    patch.object(main_module, "LOG_DIR", root),
                    patch.object(main_module, "OUTPUT_DIR", root),
                    patch.object(main_module, "APP_LOG_FILE", root / "runtime.log"),
                    patch.object(main_module, "LIGHTER_ERROR_CONFIRM_SECONDS", 0),
                ):
                    runtime = ProbeRuntime()
                    runtime.lighter_market_index = 1
                    runtime.base_amount_multiplier = 1_000
                    runtime.price_multiplier = 100
                    runtime.lighter_order_book = {
                        "bids": {Decimal("100"): Decimal("10")},
                        "asks": {Decimal("101"): Decimal("10")},
                    }
                    runtime.lighter_order_book_ready = True
                    runtime.lighter_order_book_nonce = 1
                    runtime.lighter_book_received_monotonic = time.monotonic()
                    runtime.lighter_best_bid = Decimal("100")
                    runtime.lighter_best_ask = Decimal("101")
                    record = OrderLifecycle(
                        trade_key="active-partial",
                        trade_id="active-partial",
                        side="buy",
                        qty=Decimal("1"),
                        asset="BTC",
                        auto_hedge_enabled=True,
                        last_variational_status="filled",
                    )
                    runtime.records[record.trade_key] = record
                    runtime.record_order.append(record.trade_key)

                    await runtime.place_lighter_order(record)

                    self.assertEqual(runtime.retry_count, 0)
                    self.assertEqual(record.hedge_status, "partial")
                    self.assertFalse(record.lighter_outcome_final)

        asyncio.run(run_case())
