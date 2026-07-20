"""Versioned model artifact loader.

Disk access is confined to deployment/configuration.  The returned immutable
object is injected into the pure strategy components before runtime starts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from .models import (
    Side,
    WindowStats,
    require_non_negative,
    require_positive,
    require_sha256,
)


ADAPTIVE_MODEL_ARTIFACT_SHA256 = {
    "adaptive-median-v1": (
        "bcc87280c90fc28fdfbb856dbe355f99aa850c343e826b0742a3012c32b63c53"
    ),
    # Filled from the immutable artifact bytes below.  A formula or
    # calibration edit must update both the versioned file and this seal.
    "adaptive-median-v2": (
        "cd6e44f51d2784037200114047c2691422cb20a24a416c2c88a942798d0d140e"
    ),
    # v3 replaces the weighted entry quantile with the three-window median gate.
    "adaptive-median-v3": (
        "9229d17952f166ce7cd916475b23a5839817f932c37b0c8c6a52affe7497f38c"
    ),
    # v4 uses the three-window maximum plus a small fixed margin. Directional
    # balance and execution-loss history remain diagnostics, not entry vetoes.
    "adaptive-median-v4": (
        "7a54394321206eabc70c0efc0e4a9b94132f202c2fd833b058049db8cacc804d"
    ),
    # v5 keeps the three-window gate, adapts its cushion to 30m MAD and adds
    # pre-trade spike/short-range execution-quality gates in the runtime.
    "adaptive-median-v5": (
        "cfd7c30dd391bdb79432135c034c3a044e82451dc10a60d7c7fb945157e2083d"
    ),
    # v6 replaces fixed/MAD additions with a weighted empirical quantile.
    "adaptive-median-v6": (
        "dd4450ee3e53f7d2e185f12b45010767a50df1f8192ce42209a145d9e8917dbc"
    ),
}


@dataclass(frozen=True, slots=True)
class ModelConfig:
    model_version: str
    model_hash: str
    asset: str
    reference_notional_usd: Decimal
    coverage_hours: Decimal
    calibration_dataset_sha256: str
    calibration_stats: Mapping[Side, Mapping[int, WindowStats]]
    deadband_mad_1h: Decimal
    max_step_mad_1h: Decimal
    weight_5m: Decimal
    weight_30m: Decimal
    weight_1h: Decimal
    exit_quantile: int
    entry_median_margin_bps: Decimal
    entry_mad_multiplier_30m: Decimal
    entry_quantile_pct: int
    opportunity_merge_seconds: int
    balance_ratio_limit: Decimal
    balance_minimum_events: int
    epsilon: Decimal

    def __post_init__(self) -> None:
        if self.model_version not in ADAPTIVE_MODEL_ARTIFACT_SHA256:
            raise ValueError("unsupported adaptive strategy model")
        require_sha256("model_hash", self.model_hash)
        if self.asset != "BTC":
            raise ValueError(f"{self.model_version} supports BTC only")
        require_positive("reference_notional_usd", self.reference_notional_usd)
        if self.coverage_hours < Decimal("1"):
            raise ValueError("sealed calibration must cover at least one hour")
        require_sha256("calibration_dataset_sha256", self.calibration_dataset_sha256)
        require_non_negative("deadband_mad_1h", self.deadband_mad_1h)
        require_non_negative("max_step_mad_1h", self.max_step_mad_1h)
        require_non_negative("weight_5m", self.weight_5m)
        require_non_negative("weight_30m", self.weight_30m)
        require_non_negative("weight_1h", self.weight_1h)
        require_non_negative(
            "entry_median_margin_bps",
            self.entry_median_margin_bps,
        )
        require_non_negative(
            "entry_mad_multiplier_30m",
            self.entry_mad_multiplier_30m,
        )
        if self.weight_5m + self.weight_30m + self.weight_1h != Decimal("1"):
            raise ValueError("5m/30m/1h weights must sum to one")
        if not 1 <= self.exit_quantile <= 100:
            raise ValueError("exit_quantile must be between 1 and 100")
        if not 50 <= self.entry_quantile_pct <= 80:
            raise ValueError("entry_quantile_pct must be between 50 and 80")
        require_positive("balance_ratio_limit", self.balance_ratio_limit)
        if self.balance_minimum_events < 0:
            raise ValueError("balance_minimum_events must not be negative")
        require_positive("epsilon", self.epsilon)
        if self.opportunity_merge_seconds <= 0:
            raise ValueError("opportunity_merge_seconds must be positive")
        versioned_formula = {
            "adaptive-median-v1": {
                "weight_5m": Decimal("0.15"),
                "weight_30m": Decimal("0.55"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 50,
            },
            "adaptive-median-v2": {
                "weight_5m": Decimal("0.25"),
                "weight_30m": Decimal("0.45"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 95,
                "entry_median_margin_bps": Decimal("0"),
                "entry_mad_multiplier_30m": Decimal("0"),
            },
            "adaptive-median-v3": {
                "weight_5m": Decimal("0.25"),
                "weight_30m": Decimal("0.45"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 95,
                "entry_median_margin_bps": Decimal("0.50"),
                "entry_mad_multiplier_30m": Decimal("0.25"),
            },
            "adaptive-median-v4": {
                "weight_5m": Decimal("0.25"),
                "weight_30m": Decimal("0.45"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 95,
                "entry_median_margin_bps": Decimal("0.10"),
                "entry_mad_multiplier_30m": Decimal("0"),
            },
            "adaptive-median-v5": {
                "weight_5m": Decimal("0.25"),
                "weight_30m": Decimal("0.45"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 95,
                "entry_median_margin_bps": Decimal("0.05"),
                "entry_mad_multiplier_30m": Decimal("0.20"),
            },
            "adaptive-median-v6": {
                "weight_5m": Decimal("0.25"),
                "weight_30m": Decimal("0.45"),
                "weight_1h": Decimal("0.30"),
                "exit_quantile": 95,
                "entry_median_margin_bps": Decimal("0"),
                "entry_mad_multiplier_30m": Decimal("0"),
                "entry_quantile_pct": 58,
            },
        }[self.model_version]
        fixed_formula = {
            "reference_notional_usd": (
                self.reference_notional_usd,
                Decimal("500"),
            ),
            "deadband_mad_1h": (self.deadband_mad_1h, Decimal("0.25")),
            "max_step_mad_1h": (self.max_step_mad_1h, Decimal("0.50")),
            "weight_5m": (self.weight_5m, versioned_formula["weight_5m"]),
            "weight_30m": (self.weight_30m, versioned_formula["weight_30m"]),
            "weight_1h": (self.weight_1h, versioned_formula["weight_1h"]),
            "exit_quantile": (
                self.exit_quantile,
                versioned_formula["exit_quantile"],
            ),
            "entry_median_margin_bps": (
                self.entry_median_margin_bps,
                versioned_formula.get(
                    "entry_median_margin_bps",
                    Decimal("0"),
                ),
            ),
            "entry_mad_multiplier_30m": (
                self.entry_mad_multiplier_30m,
                versioned_formula.get(
                    "entry_mad_multiplier_30m",
                    Decimal("0"),
                ),
            ),
            "entry_quantile_pct": (
                self.entry_quantile_pct,
                versioned_formula.get("entry_quantile_pct", 50),
            ),
            "opportunity_merge_seconds": (
                self.opportunity_merge_seconds,
                15,
            ),
            "balance_ratio_limit": (
                self.balance_ratio_limit,
                Decimal("2"),
            ),
            "balance_minimum_events": (
                self.balance_minimum_events,
                8
                if self.model_version
                in {"adaptive-median-v4", "adaptive-median-v5", "adaptive-median-v6"}
                else 0,
            ),
        }
        changed = [
            name
            for name, (actual, expected) in fixed_formula.items()
            if actual != expected
        ]
        if changed:
            raise ValueError(
                f"{self.model_version} fixed formula changed without a model "
                f"version upgrade: {', '.join(changed)}"
            )


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be stored as a decimal string")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def load_model_config(path: str | Path) -> ModelConfig:
    model_path = Path(path)
    raw = model_path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("model artifact root must be an object")
    if payload.get("schemaVersion") != 1:
        raise ValueError("unsupported model artifact schema")
    artifact_hash = hashlib.sha256(raw).hexdigest()
    model_version = str(payload.get("modelVersion") or "")
    expected_hash = ADAPTIVE_MODEL_ARTIFACT_SHA256.get(model_version)
    if expected_hash is None:
        raise ValueError("unsupported adaptive strategy model")
    if artifact_hash != expected_hash:
        raise ValueError(
            f"{model_version} artifact hash mismatch; calibration changes "
            "require a new model version"
        )
    sides: dict[Side, Mapping[int, WindowStats]] = {}
    calibration = payload["calibration"]
    for side in Side:
        side_payload = calibration[side.value]
        windows: dict[int, WindowStats] = {}
        for minutes in (5, 30, 60):
            source = side_payload[str(minutes)]
            windows[minutes] = WindowStats(
                side=side,
                window_minutes=minutes,
                median=_decimal(source["median"], "median"),
                q80=_decimal(source["q80"], "q80"),
                mad=_decimal(source["mad"], "mad"),
                sample_count=int(source["sampleCount"]),
                span_ms=int(source["spanMs"]),
                density_per_second=_decimal(source["densityPerSecond"], "density"),
                max_gap_ms=int(source["maxGapMs"]),
                latest_age_ms=0,
                ready=True,
                reason="sealed_calibration_prior",
                source="sealed-prior",
                q95=(
                    _decimal(source["q95"], "q95")
                    if "q95" in source
                    else None
                ),
            )
        sides[side] = MappingProxyType(windows)
    formula = payload["formula"]
    return ModelConfig(
        model_version=model_version,
        model_hash=artifact_hash,
        asset=str(payload["asset"]).strip().upper(),
        reference_notional_usd=_decimal(
            payload["referenceNotionalUsd"],
            "referenceNotionalUsd",
        ),
        coverage_hours=_decimal(payload["coverageHours"], "coverageHours"),
        calibration_dataset_sha256=str(payload["calibrationDatasetSha256"]),
        calibration_stats=MappingProxyType(sides),
        deadband_mad_1h=_decimal(formula["deadbandMad1h"], "deadbandMad1h"),
        max_step_mad_1h=_decimal(formula["maxStepMad1h"], "maxStepMad1h"),
        weight_5m=_decimal(formula["weight5m"], "weight5m"),
        weight_30m=_decimal(formula["weight30m"], "weight30m"),
        weight_1h=_decimal(formula["weight1h"], "weight1h"),
        exit_quantile=int(formula.get("exitQuantile", 50)),
        entry_median_margin_bps=_decimal(
            formula.get("entryMedianMarginBps", "0"),
            "entryMedianMarginBps",
        ),
        entry_mad_multiplier_30m=_decimal(
            formula.get("entryMadMultiplier30m", "0"),
            "entryMadMultiplier30m",
        ),
        entry_quantile_pct=int(formula.get("entryQuantilePct", 50)),
        opportunity_merge_seconds=int(formula["opportunityMergeSeconds"]),
        balance_ratio_limit=_decimal(formula["balanceRatioLimit"], "balanceRatioLimit"),
        balance_minimum_events=int(formula.get("balanceMinimumEvents", 0)),
        epsilon=_decimal(formula["epsilon"], "epsilon"),
    )
