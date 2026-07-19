"""Multi-window parameter formulas and epoch activation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Iterable, Mapping

from .model_config import ModelConfig
from .models import (
    DirectionalThresholds,
    ParameterEpoch,
    Side,
    ThresholdComponents,
    WindowStats,
    require_decimal,
    require_sha256,
)


BPS = Decimal("0.0001")
# A finite sentinel keeps all serialized/validated financial values finite.
# Rates can never approach this value in a valid market frame.
NO_BALANCE_THRESHOLD = Decimal("-1000000000")


def _windows(stats: Mapping[int, WindowStats]) -> tuple[WindowStats, WindowStats, WindowStats]:
    try:
        five = stats[5]
        thirty = stats[30]
        hourly = stats[60]
    except KeyError as exc:
        raise ValueError("5m, 30m and 1h statistics are all required") from exc
    if not (five.ready and thirty.ready and hourly.ready):
        raise ValueError("all parameter windows must be ready")
    return five, thirty, hourly


def compile_baseline(stats: Mapping[int, WindowStats], model: ModelConfig) -> Decimal:
    five, thirty, hourly = _windows(stats)
    return (
        model.weight_5m * five.median
        + model.weight_30m * thirty.median
        + model.weight_1h * hourly.median
    )


def compile_q80(stats: Mapping[int, WindowStats], model: ModelConfig) -> Decimal:
    five, thirty, hourly = _windows(stats)
    return (
        model.weight_5m * five.q80
        + model.weight_30m * thirty.q80
        + model.weight_1h * hourly.q80
    )


def compile_entry_opportunity(
    stats: Mapping[int, WindowStats],
    model: ModelConfig,
) -> Decimal:
    """Return the directional rate that a fresh quote must clear.

    V1/V2 retain their sealed weighted-Q80 behavior. V3 requires the quote to
    exceed every live window median plus its sealed volatility cushion. V4
    deliberately uses only a small fixed margin. V5 keeps the same gate shape
    but lets the cushion rise modestly with 30m MAD. Execution-quality history
    and directional balancing remain diagnostics.
    """

    five, thirty, hourly = _windows(stats)
    if model.model_version not in {
        "adaptive-median-v3",
        "adaptive-median-v4",
        "adaptive-median-v5",
    }:
        return compile_q80(stats, model)
    margin = (
        model.entry_median_margin_bps * BPS
        if model.model_version == "adaptive-median-v4"
        else max(
            model.entry_median_margin_bps * BPS,
            model.entry_mad_multiplier_30m * thirty.mad,
        )
    )
    return max(five.median, thirty.median, hourly.median) + margin


def compile_exit_opportunity(
    stats: Mapping[int, WindowStats],
    model: ModelConfig,
) -> Decimal:
    """Compile the favorable rate used to project a later exit.

    V1 remains exactly median-based for evidence/recovery compatibility.  V2
    uses Q95 from each live window; this affects whether an entry is worth
    attempting, never the exact-price floor enforced when the position closes.
    """

    five, thirty, hourly = _windows(stats)
    if model.exit_quantile == 50:
        return compile_baseline(stats, model)
    if model.exit_quantile != 95:
        raise ValueError("unsupported exit opportunity quantile")
    values = (five.q95, thirty.q95, hourly.q95)
    if any(value is None for value in values):
        raise ValueError("Q95 is required for adaptive-median-v2/v3/v4/v5")
    return (
        model.weight_5m * values[0]
        + model.weight_30m * values[1]
        + model.weight_1h * values[2]
    )


@dataclass(frozen=True, slots=True)
class OpportunitySample:
    timestamp_ms: int
    rate: Decimal

    def __post_init__(self) -> None:
        if isinstance(self.timestamp_ms, bool) or not isinstance(self.timestamp_ms, int):
            raise TypeError("timestamp_ms must be int")
        if self.timestamp_ms < 0:
            raise ValueError("timestamp_ms must not be negative")
        require_decimal("rate", self.rate)


def opportunity_events(
    samples: Iterable[OpportunitySample],
    *,
    passing_threshold: Decimal,
    merge_seconds: int,
) -> tuple[Decimal, ...]:
    """Return each opportunity episode's peak rate.

    Only passing samples participate.  Consecutive passing samples no more
    than ``merge_seconds`` apart are one episode.
    """

    require_decimal("passing_threshold", passing_threshold)
    if merge_seconds <= 0:
        raise ValueError("merge_seconds must be positive")
    ordered = sorted(samples, key=lambda sample: sample.timestamp_ms)
    gap_ms = merge_seconds * 1_000
    peaks: list[Decimal] = []
    last_ms: int | None = None
    peak: Decimal | None = None
    for sample in ordered:
        if sample.rate < passing_threshold:
            if peak is not None:
                peaks.append(peak)
                peak = None
                last_ms = None
            continue
        if last_ms is None or sample.timestamp_ms - last_ms > gap_ms:
            if peak is not None:
                peaks.append(peak)
            peak = sample.rate
        else:
            peak = max(peak if peak is not None else sample.rate, sample.rate)
        last_ms = sample.timestamp_ms
    if peak is not None:
        peaks.append(peak)
    return tuple(peaks)


def opportunity_balance_threshold(
    *,
    own_samples: Iterable[OpportunitySample],
    other_samples: Iterable[OpportunitySample],
    raw_threshold: Decimal,
    other_raw_threshold: Decimal,
    model: ModelConfig,
) -> Decimal:
    """Only raise an overactive direction to cap its opportunity episodes."""

    own_events = opportunity_events(
        own_samples,
        passing_threshold=raw_threshold,
        merge_seconds=model.opportunity_merge_seconds,
    )
    other_events = opportunity_events(
        other_samples,
        passing_threshold=other_raw_threshold,
        merge_seconds=model.opportunity_merge_seconds,
    )
    allowed = max(
        model.balance_minimum_events,
        int(model.balance_ratio_limit * Decimal(max(1, len(other_events)))),
    )
    if len(own_events) <= allowed:
        return raw_threshold
    descending = sorted(own_events, reverse=True)
    # Exclude the first event outside the allowance.  Adding the model epsilon
    # also handles tied peaks without ever retaining more than the cap.
    first_excluded = descending[allowed]
    return max(raw_threshold, first_excluded + model.epsilon)


def _economic_threshold(
    *,
    opposite_exit_opportunity: Decimal,
    wear_bps: Decimal,
    reserve_bps_per_leg: Decimal,
) -> Decimal:
    wear_rate = wear_bps * BPS
    # Each phase contains a Variational and a Lighter leg.
    phase_reserve_rate = Decimal("2") * reserve_bps_per_leg * BPS
    return (
        -wear_rate
        - opposite_exit_opportunity
        + phase_reserve_rate
        + phase_reserve_rate
    )


def _component(
    *,
    own_stats: Mapping[int, WindowStats],
    opposite_exit_opportunity: Decimal,
    balance: Decimal,
    wear_bps: Decimal,
    reserve_bps_per_leg: Decimal,
    model: ModelConfig,
) -> ThresholdComponents:
    _five, thirty, hourly = _windows(own_stats)
    baseline = compile_baseline(own_stats, model)
    q80 = compile_q80(own_stats, model)
    entry_opportunity = compile_entry_opportunity(own_stats, model)
    exit_opportunity = compile_exit_opportunity(own_stats, model)
    economic = _economic_threshold(
        opposite_exit_opportunity=opposite_exit_opportunity,
        wear_bps=wear_bps,
        reserve_bps_per_leg=reserve_bps_per_leg,
    )
    if model.model_version in {"adaptive-median-v4", "adaptive-median-v5"}:
        final = entry_opportunity
    elif model.model_version == "adaptive-median-v3":
        final = max(entry_opportunity, balance)
    else:
        final = max(q80, economic, balance)
    return ThresholdComponents(
        baseline=baseline,
        q80=q80,
        economic=economic,
        balance=balance,
        final=final,
        mad_30m=thirty.mad,
        mad_1h=hourly.mad,
        exit_opportunity=exit_opportunity,
        entry_opportunity=entry_opportunity,
    )


def build_parameter_candidate(
    *,
    now_ms: int,
    model: ModelConfig,
    config_hash: str,
    stats: Mapping[Side, Mapping[int, WindowStats]],
    reference_notional_usd: Decimal,
    order_notional_usd: Decimal,
    reserve_bps_per_leg: Decimal,
    max_normal_round_wear_bps: Decimal,
    balance_thresholds: Mapping[Side, Decimal] | None = None,
    valid_for_ms: int = 15 * 60 * 1_000,
) -> ParameterEpoch:
    """Compile a complete immutable proposal from ready window snapshots."""

    require_sha256("config_hash", config_hash)
    buy_baseline = compile_baseline(stats[Side.BUY], model)
    sell_baseline = compile_baseline(stats[Side.SELL], model)
    buy_exit_opportunity = compile_exit_opportunity(stats[Side.BUY], model)
    sell_exit_opportunity = compile_exit_opportunity(stats[Side.SELL], model)
    balances = balance_thresholds or {
        Side.BUY: NO_BALANCE_THRESHOLD,
        Side.SELL: NO_BALANCE_THRESHOLD,
    }
    buy = _component(
        own_stats=stats[Side.BUY],
        opposite_exit_opportunity=(
            sell_baseline
            if model.model_version == "adaptive-median-v1"
            else sell_exit_opportunity
        ),
        balance=balances.get(Side.BUY, NO_BALANCE_THRESHOLD),
        wear_bps=max_normal_round_wear_bps,
        reserve_bps_per_leg=reserve_bps_per_leg,
        model=model,
    )
    sell = _component(
        own_stats=stats[Side.SELL],
        opposite_exit_opportunity=(
            buy_baseline
            if model.model_version == "adaptive-median-v1"
            else buy_exit_opportunity
        ),
        balance=balances.get(Side.SELL, NO_BALANCE_THRESHOLD),
        wear_bps=max_normal_round_wear_bps,
        reserve_bps_per_leg=reserve_bps_per_leg,
        model=model,
    )
    sources = {
        stats[side][minutes].source
        for side in Side
        for minutes in (5, 30, 60)
    }
    if len(sources) != 1:
        raise ValueError("BUY and SELL 5m/30m/1h sources must match")
    source = sources.pop()
    identity = hashlib.sha256(
        (
            f"{model.model_hash}:{config_hash}:{now_ms}:"
            f"{buy.final}:{sell.final}:{source}"
        ).encode("utf-8")
    ).hexdigest()[:16]
    readiness = {
        f"{side.value}:{minutes}": stats[side][minutes].ready
        for side in stats
        for minutes in (5, 30, 60)
    }
    return ParameterEpoch(
        epoch_id=(
            "ame5-"
            if model.model_version == "adaptive-median-v5"
            else (
                "ame4-"
                if model.model_version == "adaptive-median-v4"
                else (
                    "ame3-"
                    if model.model_version == "adaptive-median-v3"
                    else (
                        "ame2-"
                        if model.model_version == "adaptive-median-v2"
                        else "ame1-"
                    )
                )
            )
        ) + identity,
        model_version=model.model_version,
        model_hash=model.model_hash,
        config_hash=config_hash,
        created_at_ms=now_ms,
        valid_from_ms=now_ms,
        expires_at_ms=now_ms + valid_for_ms,
        window_source=source,
        reference_notional_usd=reference_notional_usd,
        order_notional_usd=order_notional_usd,
        reserve_bps_per_leg=reserve_bps_per_leg,
        max_normal_round_wear_bps=max_normal_round_wear_bps,
        thresholds=DirectionalThresholds(buy=buy, sell=sell),
        readiness=readiness,
    )


def _confirmed_value(
    *,
    current: Decimal,
    first: Decimal,
    second: Decimal,
    mad_1h: Decimal,
    model: ModelConfig,
) -> Decimal:
    deadband = model.deadband_mad_1h * mad_1h
    max_step = model.max_step_mad_1h * mad_1h
    if first > current + deadband and second > current + deadband:
        target = min(first, second)
    elif first < current - deadband and second < current - deadband:
        target = max(first, second)
    else:
        return current
    return min(current + max_step, max(current - max_step, target))


def _activated_epoch_id(
    proposal: ParameterEpoch,
    *,
    thresholds: DirectionalThresholds,
    valid_from_ms: int,
    expires_at_ms: int,
) -> str:
    """Hash the parameters that are actually frozen after confirmation.

    Candidate IDs describe raw proposals.  Confirmation can replace each
    final threshold through conservative selection, deadband, and maximum-step
    smoothing, so the active epoch needs a new identity derived from those
    post-smoothing values.
    """

    fields = [
        proposal.model_version,
        proposal.model_hash,
        proposal.config_hash,
        str(proposal.created_at_ms),
        str(valid_from_ms),
        str(expires_at_ms),
        proposal.window_source,
        format(proposal.reference_notional_usd, "f"),
        format(proposal.order_notional_usd, "f"),
        format(proposal.reserve_bps_per_leg, "f"),
        format(proposal.max_normal_round_wear_bps, "f"),
    ]
    for side in Side:
        component = thresholds.for_side(side)
        fields.extend(
            (
                side.value,
                format(component.baseline, "f"),
                format(component.q80, "f"),
                format(component.economic, "f"),
                format(component.balance, "f"),
                format(component.final, "f"),
                format(component.mad_30m, "f"),
                format(component.mad_1h, "f"),
                format(component.exit_opportunity, "f"),
                format(component.entry_opportunity, "f"),
            )
        )
    fields.extend(
        f"{key}={int(value)}" for key, value in sorted(proposal.readiness.items())
    )
    identity = hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()[:16]
    prefix = (
        "ame5"
        if proposal.model_version == "adaptive-median-v5"
        else (
            "ame4"
            if proposal.model_version == "adaptive-median-v4"
            else (
                "ame3"
                if proposal.model_version == "adaptive-median-v3"
                else (
                    "ame2"
                    if proposal.model_version == "adaptive-median-v2"
                    else "ame1"
                )
            )
        )
    )
    return f"{prefix}-{identity}"


class EpochActivator:
    """Activate qualified parameter epochs with configurable confirmation."""

    def __init__(
        self,
        *,
        model: ModelConfig,
        confirmations: int = 2,
        minimum_epoch_ms: int = 10 * 60 * 1_000,
    ) -> None:
        if confirmations not in {1, 2}:
            raise ValueError("confirmations must be one or two")
        if minimum_epoch_ms <= 0:
            raise ValueError("minimum_epoch_ms must be positive")
        self.model = model
        self.confirmations = confirmations
        self.minimum_epoch_ms = minimum_epoch_ms
        self.active: ParameterEpoch | None = None
        self._pending: ParameterEpoch | None = None

    def offer(self, proposal: ParameterEpoch, *, now_ms: int) -> ParameterEpoch | None:
        if proposal.model_hash != self.model.model_hash:
            raise ValueError("proposal model hash mismatch")
        current = self.active
        if current is None:
            # The runtime only offers a proposal after all formal 5m/30m/1h
            # windows are complete.  Activate that first fully-qualified hour
            # immediately; later changes follow the configured confirmation
            # count (production v3/v4/v5 use one cold-path proposal).
            thresholds = proposal.thresholds
            expires_at_ms = max(
                proposal.expires_at_ms,
                now_ms + self.minimum_epoch_ms,
            )
            self.active = replace(
                proposal,
                epoch_id=_activated_epoch_id(
                    proposal,
                    thresholds=thresholds,
                    valid_from_ms=now_ms,
                    expires_at_ms=expires_at_ms,
                ),
                valid_from_ms=now_ms,
                expires_at_ms=expires_at_ms,
            )
            self._pending = None
            return self.active

        if self.confirmations == 1:
            thresholds = proposal.thresholds
            expires_at_ms = max(
                proposal.expires_at_ms,
                now_ms + self.minimum_epoch_ms,
            )
            self.active = replace(
                proposal,
                epoch_id=_activated_epoch_id(
                    proposal,
                    thresholds=thresholds,
                    valid_from_ms=now_ms,
                    expires_at_ms=expires_at_ms,
                ),
                valid_from_ms=now_ms,
                expires_at_ms=expires_at_ms,
            )
            self._pending = None
            return self.active

        first = self._pending
        self._pending = proposal
        if first is None:
            return current
        if first.config_hash != proposal.config_hash or first.window_source != proposal.window_source:
            return current
        if now_ms < current.valid_from_ms + self.minimum_epoch_ms:
            return current
        buy_final = _confirmed_value(
            current=current.thresholds.buy.final,
            first=first.thresholds.buy.final,
            second=proposal.thresholds.buy.final,
            mad_1h=proposal.thresholds.buy.mad_1h,
            model=self.model,
        )
        sell_final = _confirmed_value(
            current=current.thresholds.sell.final,
            first=first.thresholds.sell.final,
            second=proposal.thresholds.sell.final,
            mad_1h=proposal.thresholds.sell.mad_1h,
            model=self.model,
        )
        if proposal.model_version in {
            "adaptive-median-v3",
            "adaptive-median-v4",
            "adaptive-median-v5",
        }:
            # Upward protection must never be smoothed below the newly
            # confirmed three-window gate.  Downward changes may still move
            # gradually, which is conservative because a higher threshold
            # only suppresses entries.
            buy_final = max(buy_final, proposal.thresholds.buy.final)
            sell_final = max(sell_final, proposal.thresholds.sell.final)
        # Even when the smoothed final thresholds remain inside the deadband,
        # two fresh confirmations roll the epoch forward.  Otherwise an
        # unchanged market lets the original epoch expire permanently and
        # also leaves its baseline/components stale.
        buy = replace(proposal.thresholds.buy, final=buy_final)
        sell = replace(proposal.thresholds.sell, final=sell_final)
        thresholds = DirectionalThresholds(buy=buy, sell=sell)
        expires_at_ms = max(proposal.expires_at_ms, now_ms + self.minimum_epoch_ms)
        self.active = replace(
            proposal,
            epoch_id=_activated_epoch_id(
                proposal,
                thresholds=thresholds,
                valid_from_ms=now_ms,
                expires_at_ms=expires_at_ms,
            ),
            thresholds=thresholds,
            valid_from_ms=now_ms,
            expires_at_ms=expires_at_ms,
        )
        self._pending = None
        return self.active
