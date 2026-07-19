"""Pure O(1) open, Firm Guard and close decisions."""

from __future__ import annotations

from decimal import Decimal

from .models import (
    Action,
    CloseCandidate,
    Decision,
    MarketFrame,
    OpenCandidate,
    ParameterEpoch,
    PositionContext,
    Side,
)


BPS = Decimal("0.0001")
ZERO = Decimal("0")
FIRM_NOTIONAL_TOLERANCE_USD = Decimal("1.00")
# A close keeps half of the former phase reserve.  Open-side reserves remain
# unchanged; this factor only affects exit eligibility and its Firm recheck.
CLOSE_RESERVE_MULTIPLIER = Decimal("0.5")
V5_MAX_STANDARDIZED_EXCESS = Decimal("3.0")


def _phase_reserve_usd(epoch: ParameterEpoch, notional: Decimal) -> Decimal:
    return notional * Decimal("2") * epoch.reserve_bps_per_leg * BPS


def _wear_usd(epoch: ParameterEpoch, notional: Decimal) -> Decimal:
    return notional * epoch.max_normal_round_wear_bps * BPS


class StrategyEngine:
    """I/O-free strategy engine.

    All methods only read their immutable arguments.  The runtime owns market
    construction, authorization and the existing execution clients.
    """

    def __init__(
        self,
        *,
        buy_open_dynamic_threshold_minimum: Decimal,
        sell_open_dynamic_threshold_minimum: Decimal,
        max_frame_age_ms: int = 700,
        early_exit_seconds: int = 30 * 60,
        max_hold_seconds: int = 120 * 60,
        epsilon: Decimal = Decimal("0.000000000001"),
    ) -> None:
        if max_frame_age_ms <= 0:
            raise ValueError("freshness limit must be positive")
        if early_exit_seconds < 0 or max_hold_seconds <= early_exit_seconds:
            raise ValueError("exit timing is inconsistent")
        if epsilon <= ZERO:
            raise ValueError("epsilon must be positive")
        if (
            not isinstance(buy_open_dynamic_threshold_minimum, Decimal)
            or not buy_open_dynamic_threshold_minimum.is_finite()
            or buy_open_dynamic_threshold_minimum <= ZERO
        ):
            raise ValueError("BUY dynamic threshold minimum must be a positive Decimal")
        if (
            not isinstance(sell_open_dynamic_threshold_minimum, Decimal)
            or not sell_open_dynamic_threshold_minimum.is_finite()
            or sell_open_dynamic_threshold_minimum >= ZERO
        ):
            raise ValueError("SELL dynamic threshold minimum must be a negative Decimal")
        self.max_frame_age_ms = max_frame_age_ms
        self.early_exit_seconds = early_exit_seconds
        self.max_hold_seconds = max_hold_seconds
        self.epsilon = epsilon
        self.buy_open_dynamic_threshold_minimum = (
            buy_open_dynamic_threshold_minimum
        )
        self.sell_open_dynamic_threshold_minimum = (
            sell_open_dynamic_threshold_minimum
        )

    def _frame_block(self, frame: MarketFrame, now_ms: int) -> str | None:
        if frame.captured_at_ms > now_ms:
            return "market_frame_from_future"
        if now_ms - frame.captured_at_ms > self.max_frame_age_ms:
            return "market_frame_stale"
        if frame.variational_clock.age_ms > self.max_frame_age_ms:
            return "variational_quote_stale"
        if frame.lighter_clock.age_ms > self.max_frame_age_ms:
            return "lighter_quote_stale"
        return None

    def open_dynamic_threshold_block_reason(
        self,
        *,
        model_version: str,
        side: Side,
        threshold: Decimal,
    ) -> str | None:
        """Return the single authoritative dynamic-threshold admission result."""

        if model_version not in {"adaptive-median-v4", "adaptive-median-v5"}:
            return None
        if side is Side.BUY:
            return (
                "buy_dynamic_threshold_not_above_hard_limit"
                if threshold <= self.buy_open_dynamic_threshold_minimum
                else None
            )
        return (
            "sell_dynamic_threshold_below_hard_limit"
            if threshold < self.sell_open_dynamic_threshold_minimum
            else None
        )

    def evaluate_open(
        self,
        *,
        frame: MarketFrame,
        epoch: ParameterEpoch | None,
        now_ms: int,
    ) -> Decision:
        if epoch is None:
            return Decision(Action.PAUSE, "parameter_epoch_unavailable")
        if now_ms < epoch.valid_from_ms or now_ms >= epoch.expires_at_ms:
            return Decision(Action.PAUSE, "parameter_epoch_expired")
        if frame.asset != "BTC":
            return Decision(Action.PAUSE, "unsupported_asset")
        block = self._frame_block(frame, now_ms)
        if block:
            return Decision(Action.PAUSE, block)
        if frame.reference_notional_usd != epoch.reference_notional_usd:
            return Decision(Action.PAUSE, "reference_notional_mismatch")
        if frame.actual_notional_usd != epoch.order_notional_usd:
            return Decision(Action.PAUSE, "order_notional_mismatch")

        candidates: list[OpenCandidate] = []
        buy_dynamic_threshold_blocked = False
        sell_dynamic_threshold_blocked = False
        for side in Side:
            component = epoch.component(side)
            threshold_block = self.open_dynamic_threshold_block_reason(
                model_version=epoch.model_version,
                side=side,
                threshold=component.final,
            )
            if threshold_block is not None:
                buy_dynamic_threshold_blocked = (
                    buy_dynamic_threshold_blocked or side is Side.BUY
                )
                sell_dynamic_threshold_blocked = (
                    sell_dynamic_threshold_blocked or side is Side.SELL
                )
                continue
            reference_rate = frame.reference_rates.for_side(side)
            if reference_rate < component.final:
                continue
            actual_rate = frame.actual_rates.for_side(side)
            opposite_component = epoch.component(side.opposite)
            projected_exit_rate = (
                opposite_component.exit_opportunity
                if epoch.model_version in {
                    "adaptive-median-v2",
                    "adaptive-median-v3",
                    "adaptive-median-v4",
                    "adaptive-median-v5",
                }
                else opposite_component.baseline
            )
            reference_open = epoch.reference_notional_usd * reference_rate
            reference_close = epoch.reference_notional_usd * projected_exit_rate
            reference_reserve = Decimal("2") * _phase_reserve_usd(
                epoch, epoch.reference_notional_usd
            )
            actual_open = epoch.order_notional_usd * actual_rate
            actual_close = epoch.order_notional_usd * projected_exit_rate
            actual_reserve = Decimal("2") * _phase_reserve_usd(
                epoch, epoch.order_notional_usd
            )
            score = (reference_rate - component.final) / max(component.mad_30m, self.epsilon)
            if (
                epoch.model_version == "adaptive-median-v5"
                and score > V5_MAX_STANDARDIZED_EXCESS
            ):
                continue
            candidates.append(OpenCandidate(
                direction=side,
                frame_captured_at_ms=frame.captured_at_ms,
                epoch=epoch,
                reference_rate=reference_rate,
                actual_rate=actual_rate,
                threshold=component.final,
                standardized_excess=score,
                theoretical_round_lower_bound_usd=reference_open + reference_close - reference_reserve,
                actual_round_lower_bound_usd=actual_open + actual_close - actual_reserve,
                actual_open_pnl_usd=actual_open,
                order_notional_usd=epoch.order_notional_usd,
            ))
        if not candidates:
            if buy_dynamic_threshold_blocked and sell_dynamic_threshold_blocked:
                reason = "both_dynamic_threshold_hard_limits_blocked"
            elif buy_dynamic_threshold_blocked:
                reason = "buy_dynamic_threshold_not_above_hard_limit"
            elif sell_dynamic_threshold_blocked:
                reason = "sell_dynamic_threshold_below_hard_limit"
            else:
                reason = "no_direction_passed_frozen_threshold"
            return Decision(Action.NO_ACTION, reason)
        chosen = max(
            candidates,
            key=lambda candidate: (
                candidate.standardized_excess,
                candidate.actual_round_lower_bound_usd,
            ),
        )
        return Decision(Action.OPEN, "frozen_threshold_passed", open_candidate=chosen)

    def confirm_open(
        self,
        *,
        candidate: OpenCandidate,
        firm_frame: MarketFrame,
        firm_notional_usd: Decimal,
        target_notional_usd: Decimal,
        now_ms: int,
    ) -> Decision:
        """Recheck one in-flight candidate without reading the latest epoch."""

        if (
            not isinstance(firm_notional_usd, Decimal)
            or not firm_notional_usd.is_finite()
            or firm_notional_usd <= ZERO
            or not isinstance(target_notional_usd, Decimal)
            or not target_notional_usd.is_finite()
            or target_notional_usd <= ZERO
        ):
            return Decision(Action.PAUSE, "invalid_firm_notional")
        if firm_notional_usd > target_notional_usd + FIRM_NOTIONAL_TOLERANCE_USD:
            return Decision(Action.PAUSE, "firm_notional_exceeds_target_amount")
        if firm_notional_usd < target_notional_usd - FIRM_NOTIONAL_TOLERANCE_USD:
            return Decision(Action.PAUSE, "firm_notional_below_target_amount")
        if firm_frame.actual_notional_usd != firm_notional_usd:
            return Decision(Action.PAUSE, "firm_depth_notional_mismatch")
        epoch = candidate.epoch
        threshold_block = self.open_dynamic_threshold_block_reason(
            model_version=epoch.model_version,
            side=candidate.direction,
            threshold=candidate.threshold,
        )
        if threshold_block is not None:
            return Decision(Action.NO_ACTION, threshold_block)
        if firm_frame.asset != "BTC":
            return Decision(Action.PAUSE, "unsupported_asset")
        if firm_frame.reference_notional_usd != epoch.reference_notional_usd:
            return Decision(Action.PAUSE, "firm_reference_notional_mismatch")
        if candidate.order_notional_usd != epoch.order_notional_usd:
            return Decision(Action.PAUSE, "frozen_order_notional_mismatch")
        block = self._frame_block(firm_frame, now_ms)
        if block:
            return Decision(Action.PAUSE, block)
        reference_rate = firm_frame.reference_rates.for_side(candidate.direction)
        if reference_rate < candidate.threshold:
            return Decision(Action.NO_ACTION, "firm_reference_rate_below_frozen_threshold")
        if (
            epoch.model_version == "adaptive-median-v5"
            and (
                reference_rate - candidate.threshold
            ) / max(epoch.component(candidate.direction).mad_30m, self.epsilon)
            > V5_MAX_STANDARDIZED_EXCESS
        ):
            return Decision(Action.NO_ACTION, "firm_reference_rate_above_spike_band")
        actual_rate = firm_frame.actual_rates.for_side(candidate.direction)
        if actual_rate < candidate.threshold:
            return Decision(Action.NO_ACTION, "firm_rate_below_frozen_threshold")
        opposite_component = epoch.component(candidate.direction.opposite)
        projected_exit_rate = (
            opposite_component.exit_opportunity
            if epoch.model_version in {
                "adaptive-median-v2",
                "adaptive-median-v3",
                "adaptive-median-v4",
                "adaptive-median-v5",
            }
            else opposite_component.baseline
        )
        open_pnl = firm_notional_usd * actual_rate
        close_credit = firm_notional_usd * projected_exit_rate
        round_reserve = Decimal("2") * _phase_reserve_usd(epoch, firm_notional_usd)
        lower_bound = open_pnl + close_credit - round_reserve
        if (
            epoch.model_version
            not in {
                "adaptive-median-v3",
                "adaptive-median-v4",
                "adaptive-median-v5",
            }
            and lower_bound < -_wear_usd(epoch, firm_notional_usd)
        ):
            return Decision(Action.NO_ACTION, "firm_round_lower_bound_below_wear_floor")
        confirmed = OpenCandidate(
            direction=candidate.direction,
            frame_captured_at_ms=firm_frame.captured_at_ms,
            epoch=epoch,
            reference_rate=candidate.reference_rate,
            actual_rate=actual_rate,
            threshold=candidate.threshold,
            standardized_excess=candidate.standardized_excess,
            theoretical_round_lower_bound_usd=candidate.theoretical_round_lower_bound_usd,
            actual_round_lower_bound_usd=lower_bound,
            actual_open_pnl_usd=open_pnl,
            order_notional_usd=firm_notional_usd,
        )
        return Decision(Action.OPEN, "firm_guard_passed_frozen_context", open_candidate=confirmed)

    def evaluate_close(
        self,
        *,
        frame: MarketFrame,
        position: PositionContext,
        now_ms: int,
    ) -> Decision:
        if position.strategy_tag == "manual":
            return Decision(Action.NO_ACTION, "manual_position_not_strategy_managed")
        epoch = position.epoch
        if epoch is None:
            return Decision(Action.PAUSE, "missing_frozen_position_epoch")
        if frame.asset != "BTC":
            return Decision(Action.PAUSE, "unsupported_asset")
        block = self._frame_block(frame, now_ms)
        if block:
            return Decision(Action.PAUSE, block)
        held_seconds = max(0, (now_ms - position.opened_at_ms) // 1_000)
        close_side = position.open_direction.opposite
        if close_side is Side.BUY:
            current_var_price = frame.var_ask
            price_difference = frame.lighter_actual_sell_vwap - current_var_price
        else:
            current_var_price = frame.var_bid
            price_difference = current_var_price - frame.lighter_actual_buy_vwap
        current_close_notional = position.actual_base_qty * current_var_price
        if frame.actual_notional_usd != current_close_notional:
            return Decision(Action.PAUSE, "close_notional_mismatch")
        close_pnl = position.actual_base_qty * price_difference
        close_rate = close_pnl / current_close_notional
        regression_target = epoch.component(close_side).baseline
        regression_passed = close_rate >= regression_target
        close_reserve = (
            _phase_reserve_usd(epoch, current_close_notional)
            * CLOSE_RESERVE_MULTIPLIER
        )
        lower_bound = position.actual_open_pnl_usd + close_pnl - close_reserve
        # The normal-wear allowance remains tied to the actual opening amount;
        # only the still-hypothetical close reserve scales with current value.
        floor = ZERO if held_seconds < self.early_exit_seconds else -_wear_usd(
            epoch, position.actual_notional_usd
        )
        max_hold_alert = held_seconds >= self.max_hold_seconds
        candidate = CloseCandidate(
            close_direction=close_side,
            frame_captured_at_ms=frame.captured_at_ms,
            frozen_epoch_id=epoch.epoch_id,
            held_seconds=held_seconds,
            actual_close_rate=close_rate,
            regression_target_rate=regression_target,
            expected_close_pnl_usd=close_pnl,
            close_reserve_usd=close_reserve,
            round_lower_bound_usd=lower_bound,
            required_floor_usd=floor,
            regression_passed=regression_passed,
            max_hold_alert=max_hold_alert,
        )
        regression_required = epoch.model_version == "adaptive-median-v1"
        if lower_bound >= floor and (regression_passed or not regression_required):
            reason = "max_hold_alert_floor_passed" if max_hold_alert else "close_floor_passed"
            return Decision(Action.CLOSE, reason, close_candidate=candidate)
        if regression_required and not regression_passed:
            reason = (
                "max_hold_alert_waiting_baseline_regression"
                if max_hold_alert
                else "close_baseline_regression_not_met"
            )
        else:
            reason = (
                "max_hold_alert_waiting_controlled_floor"
                if max_hold_alert
                else "close_floor_not_met"
            )
        return Decision(Action.NO_ACTION, reason, close_candidate=candidate)
