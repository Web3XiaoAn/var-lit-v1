"""Immutable domain objects for the versioned adaptive-median strategies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Mapping


ZERO = Decimal("0")
HEX_DIGITS = frozenset("0123456789abcdef")


def require_decimal(name: str, value: Decimal) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def require_non_negative(name: str, value: Decimal) -> Decimal:
    checked = require_decimal(name, value)
    if checked < ZERO:
        raise ValueError(f"{name} must not be negative")
    return checked


def require_positive(name: str, value: Decimal) -> Decimal:
    checked = require_decimal(name, value)
    if checked <= ZERO:
        raise ValueError(f"{name} must be positive")
    return checked


def require_timestamp(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer millisecond timestamp")
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


def require_sha256(name: str, value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be str")
    normalized = value.lower()
    if len(normalized) != 64 or any(character not in HEX_DIGITS for character in normalized):
        raise ValueError(f"{name} must be a SHA-256 hex string")
    return value


class Side(str, Enum):
    """Variational leg side; the Lighter leg is always the opposite."""

    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY


class Action(str, Enum):
    NO_ACTION = "NO_ACTION"
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    PAUSE = "PAUSE"


@dataclass(frozen=True, slots=True)
class DirectionalRates:
    buy: Decimal
    sell: Decimal

    def __post_init__(self) -> None:
        require_decimal("buy", self.buy)
        require_decimal("sell", self.sell)

    def for_side(self, side: Side) -> Decimal:
        return self.buy if side is Side.BUY else self.sell


@dataclass(frozen=True, slots=True)
class SourceClock:
    source_timestamp_ms: int
    received_timestamp_ms: int
    age_ms: int

    def __post_init__(self) -> None:
        require_timestamp("source_timestamp_ms", self.source_timestamp_ms)
        require_timestamp("received_timestamp_ms", self.received_timestamp_ms)
        if isinstance(self.age_ms, bool) or not isinstance(self.age_ms, int):
            raise TypeError("age_ms must be int")
        if self.age_ms < 0:
            raise ValueError("age_ms must not be negative")


@dataclass(frozen=True, slots=True)
class MarketFrame:
    """One synchronized dual-source market frame.

    ``reference_rates`` use the 500U depth calibration basis.  On opens,
    ``actual_rates`` use the configured target order quantity; close frames use
    the exact frozen BTC position and its current notional.  These are the
    values rechecked by Firm Guard.  A frame is immutable so an in-flight
    candidate cannot observe a half-updated book.
    """

    asset: str
    captured_at_ms: int
    variational_clock: SourceClock
    lighter_clock: SourceClock
    source_skew_ms: int
    var_bid: Decimal
    var_ask: Decimal
    lighter_reference_buy_vwap: Decimal
    lighter_reference_sell_vwap: Decimal
    lighter_actual_buy_vwap: Decimal
    lighter_actual_sell_vwap: Decimal
    reference_notional_usd: Decimal
    actual_notional_usd: Decimal
    reference_rates: DirectionalRates
    actual_rates: DirectionalRates

    def __post_init__(self) -> None:
        if not self.asset.strip():
            raise ValueError("asset must not be empty")
        require_timestamp("captured_at_ms", self.captured_at_ms)
        if isinstance(self.source_skew_ms, bool) or not isinstance(self.source_skew_ms, int):
            raise TypeError("source_skew_ms must be int")
        if self.source_skew_ms < 0:
            raise ValueError("source_skew_ms must not be negative")
        for name in (
            "var_bid",
            "var_ask",
            "lighter_reference_buy_vwap",
            "lighter_reference_sell_vwap",
            "lighter_actual_buy_vwap",
            "lighter_actual_sell_vwap",
            "reference_notional_usd",
            "actual_notional_usd",
        ):
            require_positive(name, getattr(self, name))
        if self.var_bid > self.var_ask:
            raise ValueError("var_bid must not exceed var_ask")


@dataclass(frozen=True, slots=True)
class WindowStats:
    side: Side
    window_minutes: int
    median: Decimal
    q80: Decimal
    mad: Decimal
    sample_count: int
    span_ms: int
    density_per_second: Decimal
    max_gap_ms: int
    latest_age_ms: int
    ready: bool
    reason: str
    source: str = "live"
    # Added in adaptive-median-v2.  Keeping this optional lets the runtime
    # deserialize/reconcile frozen v1 evidence without pretending that its
    # historical Q80 was a Q95 observation.
    q95: Decimal | None = None

    def __post_init__(self) -> None:
        if isinstance(self.window_minutes, bool) or not isinstance(self.window_minutes, int):
            raise TypeError("window_minutes must be int")
        if self.window_minutes <= 0:
            raise ValueError("window_minutes must be positive")
        require_decimal("median", self.median)
        require_decimal("q80", self.q80)
        if self.q95 is not None:
            require_decimal("q95", self.q95)
        require_non_negative("mad", self.mad)
        require_non_negative("density_per_second", self.density_per_second)
        for name in ("sample_count", "span_ms", "max_gap_ms", "latest_age_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be int")
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if not self.source:
            raise ValueError("source must not be empty")


@dataclass(frozen=True, slots=True)
class ThresholdComponents:
    baseline: Decimal
    q80: Decimal
    economic: Decimal
    balance: Decimal
    final: Decimal
    mad_30m: Decimal
    mad_1h: Decimal
    # Favorable rate used only to project a later exit opportunity.  Actual
    # closes remain governed by exact fill economics and the frozen wear floor.
    exit_opportunity: Decimal | None = None
    # V3's entry gate.  V1/V2 default to their weighted Q80 so old frozen
    # contexts remain fully readable without inventing a new field.
    entry_opportunity: Decimal | None = None

    def __post_init__(self) -> None:
        for name in (
            "baseline",
            "q80",
            "economic",
            "balance",
            "final",
            "mad_30m",
            "mad_1h",
        ):
            require_decimal(name, getattr(self, name))
        if self.mad_30m < ZERO or self.mad_1h < ZERO:
            raise ValueError("MAD values must not be negative")
        if self.exit_opportunity is None:
            object.__setattr__(self, "exit_opportunity", self.baseline)
        else:
            require_decimal("exit_opportunity", self.exit_opportunity)
        if self.entry_opportunity is None:
            object.__setattr__(self, "entry_opportunity", self.q80)
        else:
            require_decimal("entry_opportunity", self.entry_opportunity)
        # V1/V2 candidates use max(q80, economic, balance).  V3 uses its
        # three-window entry opportunity and keeps the economic value as a
        # visible diagnostic.  Activated thresholds may temporarily lag a
        # candidate because parameter changes require confirmation.


@dataclass(frozen=True, slots=True)
class DirectionalThresholds:
    buy: ThresholdComponents
    sell: ThresholdComponents

    def for_side(self, side: Side) -> ThresholdComponents:
        return self.buy if side is Side.BUY else self.sell


@dataclass(frozen=True, slots=True)
class ParameterEpoch:
    epoch_id: str
    model_version: str
    model_hash: str
    config_hash: str
    created_at_ms: int
    valid_from_ms: int
    expires_at_ms: int
    window_source: str
    reference_notional_usd: Decimal
    order_notional_usd: Decimal
    reserve_bps_per_leg: Decimal
    max_normal_round_wear_bps: Decimal
    thresholds: DirectionalThresholds
    readiness: Mapping[str, bool]

    def __post_init__(self) -> None:
        if not self.epoch_id or not self.model_version:
            raise ValueError("epoch_id and model_version are required")
        require_sha256("model_hash", self.model_hash)
        require_sha256("config_hash", self.config_hash)
        for name in ("created_at_ms", "valid_from_ms", "expires_at_ms"):
            require_timestamp(name, getattr(self, name))
        if not (self.created_at_ms <= self.valid_from_ms < self.expires_at_ms):
            raise ValueError("epoch validity timestamps are inconsistent")
        if self.window_source not in {"sealed-prior", "live"}:
            raise ValueError("window_source must be sealed-prior or live")
        require_positive("reference_notional_usd", self.reference_notional_usd)
        require_positive("order_notional_usd", self.order_notional_usd)
        require_non_negative("reserve_bps_per_leg", self.reserve_bps_per_leg)
        require_non_negative("max_normal_round_wear_bps", self.max_normal_round_wear_bps)
        object.__setattr__(self, "readiness", MappingProxyType(dict(self.readiness)))

    def component(self, side: Side) -> ThresholdComponents:
        return self.thresholds.for_side(side)


@dataclass(frozen=True, slots=True)
class OpenCandidate:
    direction: Side
    frame_captured_at_ms: int
    epoch: ParameterEpoch
    reference_rate: Decimal
    actual_rate: Decimal
    threshold: Decimal
    standardized_excess: Decimal
    theoretical_round_lower_bound_usd: Decimal
    actual_round_lower_bound_usd: Decimal
    actual_open_pnl_usd: Decimal
    order_notional_usd: Decimal

    def __post_init__(self) -> None:
        require_timestamp("frame_captured_at_ms", self.frame_captured_at_ms)
        for name in (
            "reference_rate",
            "actual_rate",
            "threshold",
            "standardized_excess",
            "theoretical_round_lower_bound_usd",
            "actual_round_lower_bound_usd",
            "actual_open_pnl_usd",
        ):
            require_decimal(name, getattr(self, name))
        require_positive("order_notional_usd", self.order_notional_usd)


@dataclass(frozen=True, slots=True)
class PositionContext:
    strategy_tag: str
    open_direction: Side
    opened_at_ms: int
    actual_base_qty: Decimal
    actual_notional_usd: Decimal
    actual_open_pnl_usd: Decimal
    epoch: ParameterEpoch | None

    def __post_init__(self) -> None:
        if self.strategy_tag not in {
            "adaptive-median-v1",
            "adaptive-median-v2",
            "adaptive-median-v3",
            "adaptive-median-v4",
            "adaptive-median-v5",
            "adaptive-median-v6",
            "manual",
        }:
            raise ValueError("unsupported strategy_tag")
        require_timestamp("opened_at_ms", self.opened_at_ms)
        require_positive("actual_base_qty", self.actual_base_qty)
        require_positive("actual_notional_usd", self.actual_notional_usd)
        require_decimal("actual_open_pnl_usd", self.actual_open_pnl_usd)


@dataclass(frozen=True, slots=True)
class CloseCandidate:
    close_direction: Side
    frame_captured_at_ms: int
    frozen_epoch_id: str
    held_seconds: int
    actual_close_rate: Decimal
    regression_target_rate: Decimal
    expected_close_pnl_usd: Decimal
    close_reserve_usd: Decimal
    round_lower_bound_usd: Decimal
    required_floor_usd: Decimal
    regression_passed: bool
    max_hold_alert: bool
    zero_wear_stability_passed: bool = False
    zero_wear_continuous_ms: int = 0
    zero_wear_accumulated_ms: int = 0

    def __post_init__(self) -> None:
        require_timestamp("frame_captured_at_ms", self.frame_captured_at_ms)
        if not self.frozen_epoch_id:
            raise ValueError("frozen_epoch_id is required")
        if isinstance(self.held_seconds, bool) or not isinstance(self.held_seconds, int):
            raise TypeError("held_seconds must be int")
        if self.held_seconds < 0:
            raise ValueError("held_seconds must not be negative")
        if not isinstance(self.regression_passed, bool):
            raise TypeError("regression_passed must be bool")
        if not isinstance(self.max_hold_alert, bool):
            raise TypeError("max_hold_alert must be bool")
        if not isinstance(self.zero_wear_stability_passed, bool):
            raise TypeError("zero_wear_stability_passed must be bool")
        for name in ("zero_wear_continuous_ms", "zero_wear_accumulated_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be int")
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        for name in (
            "actual_close_rate",
            "regression_target_rate",
            "expected_close_pnl_usd",
            "close_reserve_usd",
            "round_lower_bound_usd",
            "required_floor_usd",
        ):
            require_decimal(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class Decision:
    action: Action
    reason: str
    open_candidate: OpenCandidate | None = None
    close_candidate: CloseCandidate | None = None

    def __post_init__(self) -> None:
        if not self.reason:
            raise ValueError("reason is required")
        if self.action is Action.OPEN and self.open_candidate is None:
            raise ValueError("OPEN requires an open candidate")
        if self.action is Action.CLOSE and self.close_candidate is None:
            raise ValueError("CLOSE requires a close candidate")
        if self.action in {Action.CLOSE, Action.PAUSE} and self.open_candidate is not None:
            raise ValueError("CLOSE/PAUSE cannot carry an open candidate")
        if self.action in {Action.OPEN, Action.PAUSE} and self.close_candidate is not None:
            raise ValueError("OPEN/PAUSE cannot carry a close candidate")
