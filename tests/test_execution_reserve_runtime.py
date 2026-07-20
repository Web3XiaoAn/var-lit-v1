import asyncio
import os
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

import main as main_module
from execution_reserve import ExecutionLossSample, bps_to_usd, write_execution_samples
from main import OrderLifecycle, VariationalToLighterRuntime


class ExecutionReserveRuntimeTests(unittest.TestCase):
    def test_restart_load_uses_current_order_notional_bucket_only(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        timestamp = datetime.now(timezone.utc).isoformat()
        rows = [
            ExecutionLossSample.from_loss(
                timestamp=timestamp,
                asset="BTC",
                phase="open",
                side="BUY",
                notional_usd=notional,
                loss_usd=bps_to_usd(Decimal("1"), notional),
            )
            for notional in (Decimal("200"), Decimal("1000"))
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "execution_samples.json"
            write_execution_samples(path, main_module.EXECUTION_SAMPLE_VERSION, rows)
            with patch("main.EXECUTION_SAMPLES_FILE", path):
                runtime.strategy_config.order_notional_usd = Decimal("200")
                asyncio.run(runtime.load_execution_samples_for_asset("BTC"))
                self.assertEqual(
                    {
                        sample.notional_bucket
                        for sample in runtime._execution_loss_record_snapshot()
                    },
                    {Decimal("128")},
                )
                runtime.strategy_config.order_notional_usd = Decimal("1000")
                asyncio.run(runtime.load_execution_samples_for_asset("BTC"))
                self.assertEqual(
                    {
                        sample.notional_bucket
                        for sample in runtime._execution_loss_record_snapshot()
                    },
                    {Decimal("512")},
                )
                runtime.strategy_config.order_notional_usd = Decimal("200")
                asyncio.run(runtime.load_execution_samples_for_asset("BTC"))
                self.assertEqual(
                    {
                        sample.notional_bucket
                        for sample in runtime._execution_loss_record_snapshot()
                    },
                    {Decimal("128")},
                )

    def test_execution_reports_remain_diagnostic_with_fixed_small_entry_margin(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.variational_ticker = "BTC"
        captured_at = datetime.now(timezone.utc).isoformat()
        for notional, count, loss_bps in (
            (Decimal("200"), 20, Decimal("1.2")),
            (Decimal("1000"), 19, Decimal("1.5")),
        ):
            for index in range(count):
                sample = ExecutionLossSample.from_loss(
                    timestamp=captured_at,
                    asset="BTC",
                    phase="open",
                    side="BUY",
                    notional_usd=notional,
                    loss_usd=bps_to_usd(loss_bps, notional),
                )
                assert sample.notional_bucket is not None
                runtime.execution_loss_sample_records[
                    (sample.phase, sample.side, sample.notional_bucket)
                ].append(sample)

        self.assertEqual(runtime.provisional_phase_reserve_usd(Decimal("200")), Decimal("0.02"))
        self.assertEqual(runtime.provisional_phase_reserve_usd(Decimal("1000")), Decimal("0.10"))
        self.assertEqual(
            runtime.effective_open_execution_headroom_bps("BUY", Decimal("200")),
            Decimal("0.25"),
        )
        self.assertEqual(
            runtime.effective_open_execution_headroom_bps("BUY", Decimal("1000")),
            Decimal("0.25"),
        )
        self.assertEqual(
            runtime.effective_open_execution_headroom_bps("SELL", Decimal("200")),
            Decimal("0.25"),
        )
        self.assertEqual(
            runtime.firm_open_execution_reserve_usd(Decimal("200")),
            Decimal("0.01"),
        )
        self.assertEqual(
            {sample.notional_bucket for sample in runtime._execution_loss_record_snapshot()},
            {Decimal("128"), Decimal("512")},
        )

    def test_sell_live_samples_do_not_turn_signal_gate_into_tail_filter(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        captured_at = datetime.now(timezone.utc).isoformat()
        observed_loss_bps = (
            Decimal("0.4826011711999517790573556046"),
            Decimal("2.087184885807111651260649239"),
            Decimal("2.713664043364172599865318501"),
            Decimal("13.98184850213321172356052090"),
        )
        for loss_bps in observed_loss_bps:
            sample = ExecutionLossSample.from_loss(
                timestamp=captured_at,
                asset="BTC",
                phase="open",
                side="SELL",
                notional_usd=Decimal("200"),
                loss_usd=bps_to_usd(loss_bps, Decimal("200")),
            )
            runtime.execution_loss_sample_records[
                (sample.phase, sample.side, sample.notional_bucket)
            ].append(sample)

        headroom = runtime.effective_open_execution_headroom_bps(
            "SELL", Decimal("200")
        )

        self.assertEqual(headroom, Decimal("0.25"))

    def test_persistent_tail_samples_do_not_raise_open_gate(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        captured_at = datetime.now(timezone.utc).isoformat()
        for _index in range(3):
            sample = ExecutionLossSample.from_loss(
                timestamp=captured_at,
                asset="BTC",
                phase="open",
                side="BUY",
                notional_usd=Decimal("200"),
                loss_usd=bps_to_usd(Decimal("12"), Decimal("200")),
            )
            runtime.execution_loss_sample_records[
                (sample.phase, sample.side, sample.notional_bucket)
            ].append(sample)

        self.assertEqual(
            runtime.effective_open_execution_headroom_bps("BUY", Decimal("200")),
            Decimal("0.25"),
        )

    def test_firm_amount_tolerance_keeps_target_notional_sample_bucket(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        captured_at = datetime.now(timezone.utc).isoformat()
        for _index in range(3):
            sample = ExecutionLossSample.from_loss(
                timestamp=captured_at,
                asset="BTC",
                phase="open",
                side="SELL",
                notional_usd=Decimal("128"),
                loss_usd=bps_to_usd(Decimal("2"), Decimal("128")),
            )
            runtime.execution_loss_sample_records[
                (sample.phase, sample.side, sample.notional_bucket)
            ].append(sample)

        self.assertEqual(
            runtime.effective_open_execution_headroom_bps(
                "SELL",
                Decimal("127.50"),
                sample_notional_usd=Decimal("128"),
            ),
            Decimal("0.25"),
        )

    def test_capture_records_signed_loss_with_actual_matched_notional(self) -> None:
        runtime = VariationalToLighterRuntime(Namespace(auto_hedge=True, lang="zh"))
        runtime.variational_ticker = "BTC"
        record = OrderLifecycle(
            trade_key="sample",
            trade_id="sample",
            side="buy",
            qty=Decimal("2"),
            asset="BTC",
            auto_hedge_enabled=True,
            last_variational_status="filled",
            var_fill_price=Decimal("100"),
            var_fill_source="event",
            firm_guard_pnl=Decimal("0"),
            strategy_phase="open",
            lighter_fill_price=Decimal("101"),
            lighter_filled_qty=Decimal("1"),
            lighter_fill_ts_iso="2026-07-13T00:00:00Z",
            hedge_status="filled",
        )

        runtime._capture_execution_loss_locked(record)

        samples = runtime._execution_loss_record_snapshot()
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].notional_usd, Decimal("100"))
        self.assertEqual(samples[0].loss_usd, Decimal("-1"))
        self.assertEqual(samples[0].loss_bps, Decimal("-100"))
        self.assertEqual(runtime._execution_samples_revision, 1)
        self.assertEqual(dict(runtime.execution_loss_samples), {})


if __name__ == "__main__":
    unittest.main()
