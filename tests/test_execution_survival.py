from decimal import Decimal
from pathlib import Path
import unittest

from adaptive_strategy.execution_survival import load_execution_survival_model


MODEL = (
    Path(__file__).resolve().parents[1]
    / "adaptive_strategy"
    / "models"
    / "execution-survival-v2.json"
)


class ExecutionSurvivalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.model = load_execution_survival_model(MODEL)

    def test_buffer_grows_with_recent_range_for_each_direction(self) -> None:
        for side in ("BUY", "SELL"):
            calibration = self.model.calibration("BTC", side)
            self.assertIsNotNone(calibration)
            assert calibration is not None
            calm = calibration.required_buffer_bps(Decimal("0.25"))
            volatile = calibration.required_buffer_bps(Decimal("1.00"))
            self.assertGreater(volatile, calm)
            self.assertGreaterEqual(calm, Decimal("0"))

    def test_two_adverse_microstructure_features_veto(self) -> None:
        calibration = self.model.calibration("BTC", "SELL")
        assert calibration is not None
        values = dict(calibration.feature_minimums)
        self.assertEqual(calibration.adverse_feature_count(values), 0)
        for name in list(values)[:2]:
            values[name] -= Decimal("0.01")
        self.assertEqual(calibration.adverse_feature_count(values), 2)
        self.assertGreaterEqual(
            calibration.adverse_feature_count(values),
            calibration.veto_count,
        )

    def test_asset_key_keeps_calibration_currency_specific(self) -> None:
        self.assertIsNotNone(self.model.calibration("btc", "buy"))
        self.assertIsNone(self.model.calibration("ETH", "BUY"))


if __name__ == "__main__":
    unittest.main()
