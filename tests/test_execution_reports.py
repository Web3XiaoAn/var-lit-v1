from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from execution_reserve import (
    EXECUTION_SAMPLE_LIMIT_PER_BUCKET,
    ExecutionLossSample,
    bps_to_usd,
    power_of_two_notional_bucket,
    read_execution_samples,
    usd_to_bps,
    write_execution_samples,
)


class ExecutionReportTests(unittest.TestCase):
    @staticmethod
    def sample(
        *,
        asset: str = "BTC",
        phase: str = "open",
        side: str = "BUY",
        notional: str = "200",
        bps: str = "1",
    ):
        amount = Decimal(notional)
        return ExecutionLossSample.from_loss(
            timestamp=datetime.now(timezone.utc).isoformat(),
            asset=asset,
            phase=phase,
            side=side,
            notional_usd=amount,
            loss_usd=bps_to_usd(Decimal(bps), amount),
        )

    def test_report_round_trip_is_model_asset_and_notional_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution_samples.json"
            write_execution_samples(
                path,
                "adaptive-median-v1",
                [self.sample(), self.sample(asset="ETH"), self.sample(notional="500")],
            )
            rows = read_execution_samples(
                path,
                "adaptive-median-v1",
                "BTC",
                notional_usd=Decimal("200"),
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].notional_usd, Decimal("200"))
            self.assertEqual(
                read_execution_samples(path, "another-model", "BTC"),
                [],
            )

    def test_old_or_corrupt_formats_are_not_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution_samples.json"
            for payload in (
                {"version": 2, "samples": []},
                {"schema": "adaptive-execution-report-v1", "strategyVersion": "v", "samples": [{"bad": True}]},
            ):
                path.write_text(json.dumps(payload), encoding="utf-8")
                self.assertEqual(read_execution_samples(path, "v", "BTC"), [])

    def test_signed_loss_and_bps_are_preserved_exactly(self) -> None:
        sample = self.sample(bps="-1.25")
        self.assertEqual(sample.loss_usd, Decimal("-0.025"))
        self.assertEqual(sample.loss_bps, Decimal("-1.25"))

    def test_notional_bucket_is_the_lower_power_of_two(self) -> None:
        self.assertEqual(power_of_two_notional_bucket(Decimal("200")), Decimal("128"))
        self.assertEqual(power_of_two_notional_bucket(Decimal("500")), Decimal("256"))
        self.assertEqual(power_of_two_notional_bucket(Decimal("1000")), Decimal("512"))

    def test_bps_and_usd_conversion_round_trip_signed_values(self) -> None:
        for bps in (Decimal("-2.5"), Decimal("0"), Decimal("3.125")):
            usd = bps_to_usd(bps, Decimal("500"))
            self.assertEqual(usd_to_bps(usd, Decimal("500")), bps)

    def test_each_execution_cohort_keeps_only_the_latest_hundred(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution_samples.json"
            samples = [self.sample(bps=str(index)) for index in range(105)]
            write_execution_samples(path, "v5", samples)
            rows = read_execution_samples(
                path,
                "v5",
                "BTC",
                notional_usd=Decimal("200"),
            )
            self.assertEqual(len(rows), EXECUTION_SAMPLE_LIMIT_PER_BUCKET)
            self.assertEqual(rows[0].loss_bps, Decimal("5"))
            self.assertEqual(rows[-1].loss_bps, Decimal("104"))

    def test_four_argument_write_replaces_only_the_matching_cohort(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "execution_samples.json"
            write_execution_samples(
                path,
                "v5",
                [
                    self.sample(bps="1"),
                    self.sample(phase="close", side="SELL", bps="2"),
                    self.sample(asset="ETH", bps="3"),
                    self.sample(notional="500", bps="4"),
                ],
            )
            write_execution_samples(
                path,
                "v5",
                "BTC",
                [self.sample(bps="9")],
            )

            btc = read_execution_samples(path, "v5", "BTC")
            eth = read_execution_samples(path, "v5", "ETH")
            self.assertEqual(len(btc), 3)
            self.assertEqual(len(eth), 1)
            self.assertEqual(
                [row.loss_bps for row in btc if row.phase == "open" and row.notional_usd == Decimal("200")],
                [Decimal("9")],
            )
            self.assertEqual(eth[0].loss_bps, Decimal("3"))


if __name__ == "__main__":
    unittest.main()
