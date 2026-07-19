"""Versioned storage for execution-wear samples and audit reports.

The runtime uses only the bounded, same-direction/same-notional cohort to
estimate opening execution headroom.  Raw samples remain available for
post-round review and model-version upgrades.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable


REPORT_SCHEMA = "adaptive-execution-report-v1"
EXECUTION_SAMPLE_LIMIT_PER_BUCKET = 100


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def power_of_two_notional_bucket(notional_usd: Decimal) -> Decimal:
    if not isinstance(notional_usd, Decimal) or not notional_usd.is_finite() or notional_usd <= 0:
        raise ValueError("notional_usd must be a positive finite Decimal")
    exponent = math.floor(math.log2(float(notional_usd)))
    return Decimal(2) ** exponent


def bps_to_usd(bps: Decimal, notional_usd: Decimal) -> Decimal:
    if not isinstance(bps, Decimal) or not isinstance(notional_usd, Decimal):
        raise TypeError("bps and notional_usd must be Decimal")
    return bps * notional_usd / Decimal("10000")


def usd_to_bps(loss_usd: Decimal, notional_usd: Decimal) -> Decimal:
    if not isinstance(loss_usd, Decimal) or not isinstance(notional_usd, Decimal):
        raise TypeError("loss_usd and notional_usd must be Decimal")
    if notional_usd <= 0:
        raise ValueError("notional_usd must be positive")
    return loss_usd * Decimal("10000") / notional_usd


@dataclass(frozen=True, slots=True)
class ExecutionLossSample:
    timestamp: str
    asset: str
    phase: str
    side: str
    notional_usd: Decimal
    loss_usd: Decimal
    loss_bps: Decimal
    notional_bucket: Decimal

    def __post_init__(self) -> None:
        try:
            datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError("timestamp must be ISO-8601") from exc
        if not self.asset.strip() or self.phase not in {"open", "close", "emergency_close"}:
            raise ValueError("asset and supported phase are required")
        if self.side.strip().upper() not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        for name in ("notional_usd", "loss_usd", "loss_bps", "notional_bucket"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ValueError(f"{name} must be a finite Decimal")
        if self.notional_usd <= 0 or self.notional_bucket <= 0:
            raise ValueError("notional and bucket must be positive")

    @classmethod
    def from_loss(
        cls,
        *,
        timestamp: str,
        asset: str,
        phase: str,
        side: str,
        notional_usd: Decimal,
        loss_usd: Decimal,
    ) -> "ExecutionLossSample":
        return cls(
            timestamp=timestamp,
            asset=asset.strip().upper(),
            phase=phase.strip().lower(),
            side=side.strip().upper(),
            notional_usd=notional_usd,
            loss_usd=loss_usd,
            loss_bps=usd_to_bps(loss_usd, notional_usd),
            notional_bucket=power_of_two_notional_bucket(notional_usd),
        )

    def to_payload(self) -> dict[str, str]:
        return {
            "timestamp": self.timestamp,
            "asset": self.asset,
            "phase": self.phase,
            "side": self.side,
            "notionalUsd": format(self.notional_usd, "f"),
            "lossUsd": format(self.loss_usd, "f"),
            "lossBps": format(self.loss_bps, "f"),
            "notionalBucket": format(self.notional_bucket, "f"),
        }

    @classmethod
    def from_payload(cls, payload: object) -> "ExecutionLossSample | None":
        if not isinstance(payload, dict):
            return None
        notional = _decimal(payload.get("notionalUsd"))
        loss = _decimal(payload.get("lossUsd"))
        loss_bps = _decimal(payload.get("lossBps"))
        bucket = _decimal(payload.get("notionalBucket"))
        if None in {notional, loss, loss_bps, bucket}:
            return None
        try:
            sample = cls(
                timestamp=str(payload.get("timestamp") or ""),
                asset=str(payload.get("asset") or "").strip().upper(),
                phase=str(payload.get("phase") or "").strip().lower(),
                side=str(payload.get("side") or "").strip().upper(),
                notional_usd=notional,
                loss_usd=loss,
                loss_bps=loss_bps,
                notional_bucket=bucket,
            )
        except (TypeError, ValueError):
            return None
        if sample.loss_bps != usd_to_bps(sample.loss_usd, sample.notional_usd):
            return None
        if sample.notional_bucket != power_of_two_notional_bucket(sample.notional_usd):
            return None
        return sample


def _read_payload(path: Path, strategy_version: str) -> list[ExecutionLossSample]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    if payload.get("schema") != REPORT_SCHEMA or payload.get("strategyVersion") != strategy_version:
        return []
    rows = payload.get("samples")
    if not isinstance(rows, list):
        return []
    return [sample for row in rows if (sample := ExecutionLossSample.from_payload(row)) is not None]


def read_execution_samples(
    path: Path,
    strategy_version: str,
    asset: str,
    *,
    notional_usd: Decimal | None = None,
) -> list[ExecutionLossSample]:
    asset_n = asset.strip().upper()
    expected_bucket = (
        power_of_two_notional_bucket(notional_usd)
        if notional_usd is not None
        else None
    )
    return [
        sample
        for sample in _read_payload(path, strategy_version)
        if sample.asset == asset_n
        and (expected_bucket is None or sample.notional_bucket == expected_bucket)
    ]


def write_execution_samples(
    path: Path,
    strategy_version: str,
    asset_or_samples: str | Iterable[ExecutionLossSample],
    samples: Iterable[ExecutionLossSample] | None = None,
) -> None:
    if samples is None:
        incoming = list(asset_or_samples) if not isinstance(asset_or_samples, str) else []
    else:
        asset = str(asset_or_samples).strip().upper()
        incoming = [sample for sample in samples if sample.asset == asset]
    replacement_cohorts = {
        (sample.asset, sample.phase, sample.side, sample.notional_bucket)
        for sample in incoming
    }
    existing = [
        sample
        for sample in _read_payload(path, strategy_version)
        if (sample.asset, sample.phase, sample.side, sample.notional_bucket)
        not in replacement_cohorts
    ]
    grouped: dict[tuple[str, str, str, Decimal], list[ExecutionLossSample]] = {}
    for sample in [*existing, *incoming]:
        key = (sample.asset, sample.phase, sample.side, sample.notional_bucket)
        grouped.setdefault(key, []).append(sample)
    bounded = [
        sample
        for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2], item[3]))
        for sample in grouped[key][-EXECUTION_SAMPLE_LIMIT_PER_BUCKET:]
    ]
    payload = {
        "schema": REPORT_SCHEMA,
        "strategyVersion": strategy_version,
        "samples": [sample.to_payload() for sample in bounded],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    os.replace(temporary, path)


__all__ = [
    "EXECUTION_SAMPLE_LIMIT_PER_BUCKET",
    "ExecutionLossSample",
    "bps_to_usd",
    "power_of_two_notional_bucket",
    "read_execution_samples",
    "usd_to_bps",
    "write_execution_samples",
]
