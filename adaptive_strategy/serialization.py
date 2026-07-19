"""Strict JSON-safe serialization for frozen adaptive position context."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from .models import (
    DirectionalThresholds,
    OpenCandidate,
    ParameterEpoch,
    Side,
    ThresholdComponents,
)


CONTEXT_SCHEMA = "adaptive-position-context-v1"


def _text(value: Decimal) -> str:
    return format(value, "f")


def _component_payload(component: ThresholdComponents) -> dict[str, str]:
    return {
        "baseline": _text(component.baseline),
        "q80": _text(component.q80),
        "economic": _text(component.economic),
        "balance": _text(component.balance),
        "final": _text(component.final),
        "mad30m": _text(component.mad_30m),
        "mad1h": _text(component.mad_1h),
        "exitOpportunity": _text(component.exit_opportunity),
        "entryOpportunity": _text(component.entry_opportunity),
    }


def epoch_to_payload(epoch: ParameterEpoch) -> dict[str, Any]:
    return {
        "epochId": epoch.epoch_id,
        "modelVersion": epoch.model_version,
        "modelHash": epoch.model_hash,
        "configHash": epoch.config_hash,
        "createdAtMs": epoch.created_at_ms,
        "validFromMs": epoch.valid_from_ms,
        "expiresAtMs": epoch.expires_at_ms,
        "windowSource": epoch.window_source,
        "referenceNotionalUsd": _text(epoch.reference_notional_usd),
        "orderNotionalUsd": _text(epoch.order_notional_usd),
        "reserveBpsPerLeg": _text(epoch.reserve_bps_per_leg),
        "maxNormalRoundWearBps": _text(epoch.max_normal_round_wear_bps),
        "thresholds": {
            "BUY": _component_payload(epoch.thresholds.buy),
            "SELL": _component_payload(epoch.thresholds.sell),
        },
        "readiness": dict(epoch.readiness),
    }


def open_candidate_to_payload(candidate: OpenCandidate) -> dict[str, Any]:
    return {
        "schema": CONTEXT_SCHEMA,
        "strategyTag": candidate.epoch.model_version,
        "direction": candidate.direction.value,
        "frameCapturedAtMs": candidate.frame_captured_at_ms,
        "referenceRate": _text(candidate.reference_rate),
        "actualRate": _text(candidate.actual_rate),
        "threshold": _text(candidate.threshold),
        "standardizedExcess": _text(candidate.standardized_excess),
        "theoreticalRoundLowerBoundUsd": _text(candidate.theoretical_round_lower_bound_usd),
        "actualRoundLowerBoundUsd": _text(candidate.actual_round_lower_bound_usd),
        "actualOpenPnlUsd": _text(candidate.actual_open_pnl_usd),
        "orderNotionalUsd": _text(candidate.order_notional_usd),
        "epoch": epoch_to_payload(candidate.epoch),
    }


def _decimal(payload: Mapping[str, Any], key: str) -> Decimal:
    value = payload[key]
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a decimal string")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(f"{key} must be finite")
    return parsed


def _component(payload: Mapping[str, Any]) -> ThresholdComponents:
    exit_opportunity = (
        _decimal(payload, "exitOpportunity")
        if "exitOpportunity" in payload
        else _decimal(payload, "baseline")
    )
    entry_opportunity = (
        _decimal(payload, "entryOpportunity")
        if "entryOpportunity" in payload
        else _decimal(payload, "q80")
    )
    return ThresholdComponents(
        baseline=_decimal(payload, "baseline"),
        q80=_decimal(payload, "q80"),
        economic=_decimal(payload, "economic"),
        balance=_decimal(payload, "balance"),
        final=_decimal(payload, "final"),
        mad_30m=_decimal(payload, "mad30m"),
        mad_1h=_decimal(payload, "mad1h"),
        exit_opportunity=exit_opportunity,
        entry_opportunity=entry_opportunity,
    )


def epoch_from_payload(payload: Mapping[str, Any]) -> ParameterEpoch:
    threshold_payload = payload["thresholds"]
    if not isinstance(threshold_payload, Mapping):
        raise TypeError("thresholds must be an object")
    readiness = payload["readiness"]
    if not isinstance(readiness, Mapping) or not all(
        isinstance(key, str) and isinstance(value, bool)
        for key, value in readiness.items()
    ):
        raise TypeError("readiness must be a string/bool object")
    return ParameterEpoch(
        epoch_id=str(payload["epochId"]),
        model_version=str(payload["modelVersion"]),
        model_hash=str(payload["modelHash"]),
        config_hash=str(payload["configHash"]),
        created_at_ms=int(payload["createdAtMs"]),
        valid_from_ms=int(payload["validFromMs"]),
        expires_at_ms=int(payload["expiresAtMs"]),
        window_source=str(payload["windowSource"]),
        reference_notional_usd=_decimal(payload, "referenceNotionalUsd"),
        order_notional_usd=_decimal(payload, "orderNotionalUsd"),
        reserve_bps_per_leg=_decimal(payload, "reserveBpsPerLeg"),
        max_normal_round_wear_bps=_decimal(payload, "maxNormalRoundWearBps"),
        thresholds=DirectionalThresholds(
            buy=_component(threshold_payload["BUY"]),
            sell=_component(threshold_payload["SELL"]),
        ),
        readiness=dict(readiness),
    )


def open_candidate_from_payload(payload: Mapping[str, Any] | None) -> OpenCandidate | None:
    if not isinstance(payload, Mapping):
        return None
    strategy_tag = payload.get("strategyTag")
    if payload.get("schema") != CONTEXT_SCHEMA or strategy_tag not in {
        "adaptive-median-v1",
        "adaptive-median-v2",
        "adaptive-median-v3",
        "adaptive-median-v4",
        "adaptive-median-v5",
    }:
        return None
    try:
        epoch_payload = payload["epoch"]
        if not isinstance(epoch_payload, Mapping):
            return None
        candidate = OpenCandidate(
            direction=Side(str(payload["direction"])),
            frame_captured_at_ms=int(payload["frameCapturedAtMs"]),
            epoch=epoch_from_payload(epoch_payload),
            reference_rate=_decimal(payload, "referenceRate"),
            actual_rate=_decimal(payload, "actualRate"),
            threshold=_decimal(payload, "threshold"),
            standardized_excess=_decimal(payload, "standardizedExcess"),
            theoretical_round_lower_bound_usd=_decimal(
                payload, "theoreticalRoundLowerBoundUsd"
            ),
            actual_round_lower_bound_usd=_decimal(payload, "actualRoundLowerBoundUsd"),
            actual_open_pnl_usd=_decimal(payload, "actualOpenPnlUsd"),
            order_notional_usd=_decimal(payload, "orderNotionalUsd"),
        )
        if candidate.epoch.model_version != strategy_tag:
            return None
        return candidate
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None
