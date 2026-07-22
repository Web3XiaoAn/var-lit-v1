"""Small, asset-keyed execution-survival policy loader."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


ZERO = Decimal("0")


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be stored as a decimal string")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    return parsed


@dataclass(frozen=True, slots=True)
class SurvivalCalibration:
    mean_buffer_bps: Decimal
    std_buffer_bps: Decimal
    mean_range_5s_bps: Decimal
    std_range_5s_bps: Decimal
    intercept: Decimal
    buffer_coefficient: Decimal
    range_coefficient: Decimal
    gate_logit: Decimal
    feature_minimums: Mapping[str, Decimal]
    veto_count: int

    def __post_init__(self) -> None:
        if (
            self.std_buffer_bps <= ZERO
            or self.std_range_5s_bps <= ZERO
            or self.buffer_coefficient <= ZERO
            or self.veto_count <= 0
        ):
            raise ValueError("invalid execution-survival calibration")

    def required_buffer_bps(self, range_5s_bps: Decimal) -> Decimal:
        """Invert the logistic gate into the minimum current basis buffer."""

        range_z = (
            range_5s_bps - self.mean_range_5s_bps
        ) / self.std_range_5s_bps
        buffer_z = (
            self.gate_logit
            - self.intercept
            - self.range_coefficient * range_z
        ) / self.buffer_coefficient
        return max(
            ZERO,
            self.mean_buffer_bps + self.std_buffer_bps * buffer_z,
        )

    def adverse_feature_count(self, values: Mapping[str, Decimal]) -> int:
        return sum(
            values.get(name, minimum - Decimal("1")) < minimum
            for name, minimum in self.feature_minimums.items()
        )


@dataclass(frozen=True, slots=True)
class ExecutionSurvivalModel:
    model_version: str
    model_hash: str
    assets: Mapping[str, Mapping[str, SurvivalCalibration]]

    def calibration(self, asset: str, side: str) -> SurvivalCalibration | None:
        return self.assets.get(asset.strip().upper(), {}).get(side.strip().upper())


def load_execution_survival_model(path: str | Path) -> ExecutionSurvivalModel:
    model_path = Path(path)
    raw = model_path.read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("schemaVersion") != 1:
        raise ValueError("unsupported execution-survival model")
    assets: dict[str, Mapping[str, SurvivalCalibration]] = {}
    for asset, directions in payload.get("assets", {}).items():
        parsed_directions: dict[str, SurvivalCalibration] = {}
        for side in ("BUY", "SELL"):
            source = directions[side]
            minimums = {
                name: _decimal(value, name)
                for name, value in source["minimums"].items()
            }
            parsed_directions[side] = SurvivalCalibration(
                mean_buffer_bps=_decimal(source["meanBufferBps"], "meanBufferBps"),
                std_buffer_bps=_decimal(source["stdBufferBps"], "stdBufferBps"),
                mean_range_5s_bps=_decimal(source["meanRange5Bps"], "meanRange5Bps"),
                std_range_5s_bps=_decimal(source["stdRange5Bps"], "stdRange5Bps"),
                intercept=_decimal(source["intercept"], "intercept"),
                buffer_coefficient=_decimal(
                    source["bufferCoefficient"], "bufferCoefficient"
                ),
                range_coefficient=_decimal(
                    source["rangeCoefficient"], "rangeCoefficient"
                ),
                gate_logit=_decimal(source["gateLogit"], "gateLogit"),
                feature_minimums=MappingProxyType(minimums),
                veto_count=int(source["vetoCount"]),
            )
        assets[str(asset).strip().upper()] = MappingProxyType(parsed_directions)
    if not assets:
        raise ValueError("execution-survival model has no assets")
    return ExecutionSurvivalModel(
        model_version=str(payload.get("modelVersion") or ""),
        model_hash=hashlib.sha256(raw).hexdigest(),
        assets=MappingProxyType(assets),
    )
