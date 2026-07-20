from __future__ import annotations

import hashlib
import inspect
import json
import tempfile
import time
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from adaptive_strategy import (
    Action,
    DirectionalRates,
    EpochActivator,
    MarketFrame,
    OpportunitySample,
    PositionContext,
    RollingWindowStore,
    Side,
    SourceClock,
    StrategyEngine,
    build_parameter_candidate,
    compile_baseline,
    compile_entry_opportunity,
    compile_exit_opportunity,
    compile_q80,
    load_model_config,
    opportunity_balance_threshold,
)
from adaptive_strategy.parameters import opportunity_events


ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v1.json"
MODEL_V2_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v2.json"
MODEL_V3_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v3.json"
MODEL_V4_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v4.json"
MODEL_V5_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v5.json"
MODEL_V6_PATH = ROOT / "adaptive_strategy" / "models" / "adaptive-median-v6.json"
D = Decimal
MODEL = load_model_config(MODEL_PATH)


def strategy_engine(**overrides) -> StrategyEngine:
    options = {
        "buy_open_dynamic_threshold_minimum": D("0.0005"),
        "sell_open_dynamic_threshold_minimum": D("-0.00073"),
        **overrides,
    }
    return StrategyEngine(**options)


def config_hash() -> str:
    return hashlib.sha256(b"test-runtime-config").hexdigest()


def candidate(
    model,
    *,
    now_ms: int = 10_000,
    reserve: Decimal = D("0.50"),
    order_notional: Decimal = D("200"),
    balance=None,
):
    return build_parameter_candidate(
        now_ms=now_ms,
        model=model,
        config_hash=config_hash(),
        stats=model.calibration_stats,
        reference_notional_usd=D("500"),
        order_notional_usd=order_notional,
        reserve_bps_per_leg=reserve,
        max_normal_round_wear_bps=D("1.0"),
        balance_thresholds=balance,
    )


def frame(
    *,
    at_ms: int,
    reference_buy: Decimal,
    reference_sell: Decimal,
    actual_buy: Decimal | None = None,
    actual_sell: Decimal | None = None,
    actual_notional: Decimal = D("200"),
) -> MarketFrame:
    clock = SourceClock(at_ms, at_ms, 0)
    return MarketFrame(
        asset="BTC",
        captured_at_ms=at_ms,
        variational_clock=clock,
        lighter_clock=clock,
        source_skew_ms=0,
        var_bid=D("62500"),
        var_ask=D("62501"),
        lighter_reference_buy_vwap=D("62510"),
        lighter_reference_sell_vwap=D("62560"),
        lighter_actual_buy_vwap=D("62508"),
        lighter_actual_sell_vwap=D("62562"),
        reference_notional_usd=D("500"),
        actual_notional_usd=actual_notional,
        reference_rates=DirectionalRates(reference_buy, reference_sell),
        actual_rates=DirectionalRates(
            actual_buy if actual_buy is not None else reference_buy,
            actual_sell if actual_sell is not None else reference_sell,
        ),
    )


def test_model_artifact_matches_sealed_dataset(model):
    artifact = MODEL_PATH.read_bytes()
    assert model.model_hash == hashlib.sha256(artifact).hexdigest()
    payload = json.loads(artifact)
    assert payload["calibrationDatasetSha256"] == model.calibration_dataset_sha256
    assert model.calibration_dataset_sha256 == (
        "2796c9bab70988af717a2edeee97995ede5b4063186f722b15bc3979ec2e636d"
    )
    assert model.asset == "BTC"


def test_v2_short_window_weight_and_q95_exit_projection_are_sealed():
    model = load_model_config(MODEL_V2_PATH)
    assert (model.weight_5m, model.weight_30m, model.weight_1h) == (
        D("0.25"),
        D("0.45"),
        D("0.30"),
    )
    assert model.exit_quantile == 95
    projected = compile_exit_opportunity(model.calibration_stats[Side.BUY], model)
    assert projected > compile_q80(model.calibration_stats[Side.BUY], model)
    assert model.reference_notional_usd == D("500")
    assert model.coverage_hours >= D("4")


def test_v3_three_window_entry_gate_and_margin_are_sealed():
    model = load_model_config(MODEL_V3_PATH)
    assert model.entry_median_margin_bps == D("0.50")
    assert model.entry_mad_multiplier_30m == D("0.25")
    for side in Side:
        stats = model.calibration_stats[side]
        expected = max(
            stats[5].median,
            stats[30].median,
            stats[60].median,
        ) + max(D("0.00005"), D("0.25") * stats[30].mad)
        assert compile_entry_opportunity(stats, model) == expected

        epoch = candidate(model)
        component = epoch.component(side)
        assert component.entry_opportunity == expected
        assert component.final >= expected
    # The cost projection remains visible, but it no longer silently replaces
    # the user's explicit three-window market-opportunity definition.
    buy = candidate(model).thresholds.buy
    assert buy.economic > buy.entry_opportunity
    assert buy.final == buy.entry_opportunity


def test_v4_uses_small_three_window_margin_and_keeps_balance_diagnostic():
    model = load_model_config(MODEL_V4_PATH)
    assert model.entry_median_margin_bps == D("0.10")
    assert model.entry_mad_multiplier_30m == D("0")
    assert model.balance_ratio_limit == D("2")
    assert model.balance_minimum_events == 8
    for side in Side:
        stats = model.calibration_stats[side]
        expected = max(
            stats[5].median,
            stats[30].median,
            stats[60].median,
        ) + D("0.00001")
        assert compile_entry_opportunity(stats, model) == expected
        epoch = candidate(
            model,
            balance={
                Side.BUY: expected + D("0.01"),
                Side.SELL: expected + D("0.01"),
            },
        )
        component = epoch.component(side)
        assert component.entry_opportunity == expected
        assert component.final == expected


def test_v5_uses_adaptive_mad_cushion_and_keeps_balance_diagnostic():
    model = load_model_config(MODEL_V5_PATH)
    assert model.entry_median_margin_bps == D("0.05")
    assert model.entry_mad_multiplier_30m == D("0.20")
    for side in Side:
        stats = model.calibration_stats[side]
        expected = max(
            stats[5].median,
            stats[30].median,
            stats[60].median,
        ) + max(D("0.000005"), D("0.20") * stats[30].mad)
        assert compile_entry_opportunity(stats, model) == expected
        epoch = candidate(
            model,
            balance={
                Side.BUY: expected + D("0.01"),
                Side.SELL: expected + D("0.01"),
            },
        )
        assert epoch.epoch_id.startswith("ame5-")
        assert epoch.component(side).final == expected


def test_v6_uses_weighted_empirical_entry_quantile():
    model = load_model_config(MODEL_V6_PATH)
    assert model.entry_quantile_pct == 58
    assert model.entry_median_margin_bps == D("0")
    assert model.entry_mad_multiplier_30m == D("0")
    fraction = D("8") / D("30")
    for side in Side:
        stats = model.calibration_stats[side]
        spread = (
            model.weight_5m * (stats[5].q80 - stats[5].median)
            + model.weight_30m * (stats[30].q80 - stats[30].median)
            + model.weight_1h * (stats[60].q80 - stats[60].median)
        )
        expected = max(
            stats[5].median,
            stats[30].median,
            stats[60].median,
        ) + fraction * spread
        assert compile_entry_opportunity(stats, model) == expected
        epoch = candidate(model)
        assert epoch.epoch_id.startswith("ame6-")
        assert epoch.component(side).final == expected


def test_v5_rejects_reference_spikes_above_three_mad_at_both_open_checks():
    model = load_model_config(MODEL_V5_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    buy = epoch.thresholds.buy
    market = frame(
        at_ms=10_100,
        reference_buy=buy.final + D("3.01") * buy.mad_30m,
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
    )
    assert engine.evaluate_open(
        frame=market,
        epoch=epoch,
        now_ms=10_100,
    ).action is Action.NO_ACTION

    passing = replace(
        market,
        reference_rates=DirectionalRates(
            buy=buy.final + D("2.5") * buy.mad_30m,
            sell=market.reference_rates.sell,
        ),
        actual_rates=DirectionalRates(
            buy=buy.final + D("2.5") * buy.mad_30m,
            sell=market.actual_rates.sell,
        ),
    )
    opened = engine.evaluate_open(frame=passing, epoch=epoch, now_ms=10_100)
    assert opened.action is Action.OPEN
    assert opened.open_candidate is not None
    firm = replace(
        passing,
        captured_at_ms=10_200,
        reference_rates=DirectionalRates(
            buy=buy.final + D("3.01") * buy.mad_30m,
            sell=passing.reference_rates.sell,
        ),
        actual_rates=DirectionalRates(
            buy=buy.final + D("3.01") * buy.mad_30m,
            sell=passing.actual_rates.sell,
        ),
    )
    blocked = engine.confirm_open(
        candidate=opened.open_candidate,
        firm_frame=firm,
        firm_notional_usd=D("200"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert blocked.action is Action.NO_ACTION
    assert blocked.reason == "firm_reference_rate_above_spike_band"


def test_v4_sell_hard_limit_blocks_only_below_negative_point_zero_seventy_three_percent():
    model = load_model_config(MODEL_V4_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()

    below_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            sell=replace(epoch.thresholds.sell, final=D("-0.0007300001")),
        ),
    )
    sell_only_market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final - D("0.001"),
        reference_sell=D("0"),
        actual_sell=D("0"),
    )
    blocked = engine.evaluate_open(
        frame=sell_only_market,
        epoch=below_limit_epoch,
        now_ms=10_100,
    )
    assert blocked.action is Action.NO_ACTION
    assert blocked.reason == "sell_dynamic_threshold_below_hard_limit"

    at_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            sell=replace(epoch.thresholds.sell, final=D("-0.00073")),
        ),
    )
    allowed = engine.evaluate_open(
        frame=sell_only_market,
        epoch=at_limit_epoch,
        now_ms=10_100,
    )
    assert allowed.action is Action.OPEN
    assert allowed.open_candidate is not None
    assert allowed.open_candidate.direction is Side.SELL


def test_v4_buy_hard_limit_requires_strictly_above_positive_point_zero_five_percent():
    model = load_model_config(MODEL_V4_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    buy_only_market = frame(
        at_ms=10_100,
        reference_buy=D("1"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=D("1"),
    )

    at_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            buy=replace(epoch.thresholds.buy, final=D("0.0005")),
            sell=replace(epoch.thresholds.sell, final=D("-0.00073")),
        ),
    )
    blocked = engine.evaluate_open(
        frame=buy_only_market,
        epoch=at_limit_epoch,
        now_ms=10_100,
    )
    assert blocked.action is Action.NO_ACTION
    assert blocked.reason == "buy_dynamic_threshold_not_above_hard_limit"

    above_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            buy=replace(epoch.thresholds.buy, final=D("0.0005000001")),
            sell=replace(epoch.thresholds.sell, final=D("-0.00073")),
        ),
    )
    allowed = engine.evaluate_open(
        frame=buy_only_market,
        epoch=above_limit_epoch,
        now_ms=10_100,
    )
    assert allowed.action is Action.OPEN
    assert allowed.open_candidate is not None
    assert allowed.open_candidate.direction is Side.BUY


def test_v4_sell_hard_limit_leaves_buy_direction_unchanged():
    model = load_model_config(MODEL_V4_PATH)
    epoch = candidate(model, now_ms=10_000)
    below_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            sell=replace(epoch.thresholds.sell, final=D("-0.0007300001")),
        ),
    )
    market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=D("0"),
        actual_buy=epoch.thresholds.buy.final + D("0.0001"),
        actual_sell=D("0"),
    )
    decision = strategy_engine().evaluate_open(
        frame=market,
        epoch=below_limit_epoch,
        now_ms=10_100,
    )
    assert decision.action is Action.OPEN
    assert decision.open_candidate is not None
    assert decision.open_candidate.direction is Side.BUY


def test_v4_firm_guard_rejects_frozen_sell_candidate_below_hard_limit():
    model = load_model_config(MODEL_V4_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    at_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            sell=replace(epoch.thresholds.sell, final=D("-0.00073")),
        ),
    )
    market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final - D("0.001"),
        reference_sell=D("0"),
        actual_sell=D("0"),
    )
    open_candidate = engine.evaluate_open(
        frame=market,
        epoch=at_limit_epoch,
        now_ms=10_100,
    ).open_candidate
    assert open_candidate is not None

    below_limit_epoch = replace(
        at_limit_epoch,
        thresholds=replace(
            at_limit_epoch.thresholds,
            sell=replace(at_limit_epoch.thresholds.sell, final=D("-0.0007300001")),
        ),
    )
    blocked = engine.confirm_open(
        candidate=replace(
            open_candidate,
            epoch=below_limit_epoch,
            threshold=D("-0.0007300001"),
        ),
        firm_frame=replace(market, captured_at_ms=10_200),
        firm_notional_usd=D("200"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert blocked.action is Action.NO_ACTION
    assert blocked.reason == "sell_dynamic_threshold_below_hard_limit"


def test_v4_firm_guard_rejects_frozen_buy_candidate_at_hard_limit():
    model = load_model_config(MODEL_V4_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    above_limit_epoch = replace(
        epoch,
        thresholds=replace(
            epoch.thresholds,
            buy=replace(epoch.thresholds.buy, final=D("0.0005000001")),
        ),
    )
    market = frame(
        at_ms=10_100,
        reference_buy=D("1"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=D("1"),
    )
    open_candidate = engine.evaluate_open(
        frame=market,
        epoch=above_limit_epoch,
        now_ms=10_100,
    ).open_candidate
    assert open_candidate is not None

    at_limit_epoch = replace(
        above_limit_epoch,
        thresholds=replace(
            above_limit_epoch.thresholds,
            buy=replace(above_limit_epoch.thresholds.buy, final=D("0.0005")),
        ),
    )
    blocked = engine.confirm_open(
        candidate=replace(
            open_candidate,
            epoch=at_limit_epoch,
            threshold=D("0.0005"),
        ),
        firm_frame=replace(market, captured_at_ms=10_200),
        firm_notional_usd=D("200"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert blocked.action is Action.NO_ACTION
    assert blocked.reason == "buy_dynamic_threshold_not_above_hard_limit"


def test_dynamic_threshold_minimums_are_constructor_configurable():
    engine = StrategyEngine(
        buy_open_dynamic_threshold_minimum=D("0.0006"),
        sell_open_dynamic_threshold_minimum=D("-0.0008"),
    )
    assert engine.buy_open_dynamic_threshold_minimum == D("0.0006")
    assert engine.sell_open_dynamic_threshold_minimum == D("-0.0008")


def test_v3_firm_guard_rechecks_three_window_rate_without_economic_veto():
    model = load_model_config(MODEL_V3_PATH)
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    threshold = epoch.thresholds.buy.final
    initial = frame(
        at_ms=10_100,
        reference_buy=threshold + D("0.00001"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=threshold,
    )
    open_candidate = engine.evaluate_open(
        frame=initial,
        epoch=epoch,
        now_ms=10_100,
    ).open_candidate
    assert open_candidate is not None
    assert open_candidate.actual_round_lower_bound_usd < -D("0.02")

    confirmed = engine.confirm_open(
        candidate=open_candidate,
        firm_frame=replace(initial, captured_at_ms=10_200),
        firm_notional_usd=D("200"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert confirmed.action is Action.OPEN
    assert confirmed.open_candidate is not None
    assert confirmed.open_candidate.actual_rate == threshold


def test_v3_confirmed_epoch_never_smooths_below_three_window_gate():
    model = load_model_config(MODEL_V3_PATH)
    activator = EpochActivator(model=model)
    first = activator.offer(candidate(model, now_ms=10_000), now_ms=10_000)
    assert first is not None

    changed_stats = {
        side: dict(model.calibration_stats[side])
        for side in Side
    }
    for side in Side:
        original = changed_stats[side][5]
        changed_stats[side][5] = replace(
            original,
            median=original.median + D("0.01"),
        )

    def changed(now_ms):
        return build_parameter_candidate(
            now_ms=now_ms,
            model=model,
            config_hash=config_hash(),
            stats=changed_stats,
            reference_notional_usd=D("500"),
            order_notional_usd=D("200"),
            reserve_bps_per_leg=D("0.50"),
            max_normal_round_wear_bps=D("1.0"),
        )

    activator.offer(changed(610_000), now_ms=610_000)
    proposal = changed(910_000)
    active = activator.offer(proposal, now_ms=910_000)
    assert active is not None
    for side in Side:
        assert active.component(side).final >= proposal.component(side).final
        assert (
            active.component(side).final
            >= proposal.component(side).entry_opportunity
        )


def test_v1_model_artifact_rejects_silent_calibration_edit():
    payload = json.loads(MODEL_PATH.read_text(encoding="utf-8"))
    payload["calibration"]["BUY"]["5"]["median"] = "0.123"
    with tempfile.TemporaryDirectory(prefix="adaptive-model-") as tmp:
        changed = Path(tmp) / "adaptive-median-v1.json"
        changed.write_text(json.dumps(payload), encoding="utf-8")
        try:
            load_model_config(changed)
        except ValueError as exc:
            assert "artifact hash mismatch" in str(exc)
        else:
            raise AssertionError("silently modified v1 model artifact was accepted")


def test_parameter_candidate_rejects_non_hex_config_hash(model):
    try:
        build_parameter_candidate(
            now_ms=10_000,
            model=model,
            config_hash="z" * 64,
            stats=model.calibration_stats,
            reference_notional_usd=D("500"),
            order_notional_usd=D("200"),
            reserve_bps_per_leg=D("0.50"),
            max_normal_round_wear_bps=D("1.0"),
        )
    except ValueError as exc:
        assert "SHA-256" in str(exc)
    else:
        raise AssertionError("non-hex config hash was accepted")


def test_multi_window_formula_uses_exact_weights(model):
    assert compile_baseline(model.calibration_stats[Side.BUY], model) == D(
        "0.0009453979366087396656916772003"
    )
    assert compile_baseline(model.calibration_stats[Side.SELL], model) == D(
        "-0.001064144044561809131139125050"
    )
    assert compile_q80(model.calibration_stats[Side.BUY], model) == D(
        "0.0009810959618758721945475370536"
    )
    assert compile_q80(model.calibration_stats[Side.SELL], model) == D(
        "-0.001024309621138335851129621484"
    )
    assert (model.weight_5m, model.weight_30m, model.weight_1h) == (
        D("0.15"),
        D("0.55"),
        D("0.30"),
    )


def test_thirty_minute_weight_dominates_one_hour_and_five_minute(model):
    original = model.calibration_stats[Side.BUY]
    baseline = compile_baseline(original, model)
    deltas = {}
    for minutes in (5, 30, 60):
        changed = dict(original)
        changed[minutes] = replace(
            original[minutes],
            median=original[minutes].median + D("0.001"),
        )
        deltas[minutes] = compile_baseline(changed, model) - baseline

    assert abs(deltas[5] - D("0.00015")) < D("1e-27")
    assert abs(deltas[30] - D("0.00055")) < D("1e-27")
    assert abs(deltas[60] - D("0.00030")) < D("1e-27")
    assert deltas[30] > deltas[60] > deltas[5]


def test_economic_threshold_includes_both_legs_of_both_phases(model):
    epoch = candidate(model)
    assert epoch.thresholds.buy.economic == D("0.001164144044561809131139125050")
    assert epoch.thresholds.sell.economic == D("-0.000845397936608739665691677200")
    assert epoch.thresholds.buy.final == epoch.thresholds.buy.economic
    assert epoch.thresholds.sell.final == epoch.thresholds.sell.economic


def test_more_reserve_can_only_raise_thresholds(model):
    low = candidate(model, reserve=D("0.25"))
    high = candidate(model, reserve=D("0.75"))
    for side in Side:
        assert high.component(side).economic > low.component(side).economic
        assert high.component(side).final >= low.component(side).final


def test_balance_only_raises_overactive_direction(model):
    own = [OpportunitySample(index * 20_000, D("0.002") + D(index) / D("1000000")) for index in range(8)]
    other = [OpportunitySample(0, D("0.002")), OpportunitySample(20_000, D("0.0021"))]
    raised = opportunity_balance_threshold(
        own_samples=own,
        other_samples=other,
        raw_threshold=D("0.001"),
        other_raw_threshold=D("0.001"),
        model=model,
    )
    weak = opportunity_balance_threshold(
        own_samples=other,
        other_samples=own,
        raw_threshold=D("0.001"),
        other_raw_threshold=D("0.001"),
        model=model,
    )
    assert raised > D("0.001")
    assert weak == D("0.001")
    retained = opportunity_events(
        own,
        passing_threshold=raised,
        merge_seconds=model.opportunity_merge_seconds,
    )
    other_retained = opportunity_events(
        other,
        passing_threshold=D("0.001"),
        merge_seconds=model.opportunity_merge_seconds,
    )
    assert len(retained) <= int(
        max(
            model.balance_minimum_events,
            int(model.balance_ratio_limit * D(max(1, len(other_retained)))),
        )
    )


def test_v4_balance_never_suppresses_below_eight_independent_events():
    model = load_model_config(MODEL_V4_PATH)
    own = [
        OpportunitySample(index * 20_000, D("0.002") + D(index) / D("1000000"))
        for index in range(12)
    ]
    other = [OpportunitySample(0, D("0.002"))]
    raised = opportunity_balance_threshold(
        own_samples=own,
        other_samples=other,
        raw_threshold=D("0.001"),
        other_raw_threshold=D("0.001"),
        model=model,
    )
    retained = opportunity_events(
        own,
        passing_threshold=raised,
        merge_seconds=model.opportunity_merge_seconds,
    )
    assert raised > D("0.001")
    assert len(retained) == 8

    exactly_eight = opportunity_balance_threshold(
        own_samples=own[:8],
        other_samples=other,
        raw_threshold=D("0.001"),
        other_raw_threshold=D("0.001"),
        model=model,
    )
    assert exactly_eight == D("0.001")


def test_nonpassing_sample_breaks_opportunity_episode(model):
    samples = [
        OpportunitySample(0, D("0.002")),
        OpportunitySample(2_000, D("0")),
        OpportunitySample(4_000, D("0.0021")),
        OpportunitySample(30_000, D("0.0022")),
    ]
    other = [OpportunitySample(0, D("0.002"))]
    raised = opportunity_balance_threshold(
        own_samples=samples,
        other_samples=other,
        raw_threshold=D("0.001"),
        other_raw_threshold=D("0.001"),
        model=model,
    )
    assert raised > D("0.001")


def test_first_epoch_activates_immediately_then_updates_require_two_confirmations(model):
    activator = EpochActivator(model=model)
    first = candidate(model, now_ms=10_000)
    active = activator.offer(first, now_ms=first.created_at_ms)
    assert active is not None
    original_id = active.epoch_id
    changed = candidate(model, now_ms=310_000, reserve=D("1.00"))
    assert activator.offer(changed, now_ms=310_000).epoch_id == original_id
    changed_again = candidate(model, now_ms=609_999, reserve=D("1.00"))
    assert activator.offer(changed_again, now_ms=609_999).epoch_id == original_id
    confirmed = candidate(model, now_ms=610_000, reserve=D("1.00"))
    assert activator.offer(confirmed, now_ms=610_000).epoch_id != original_id


def test_single_confirmation_activates_each_five_minute_proposal(model):
    activator = EpochActivator(model=model, confirmations=1)
    first = activator.offer(candidate(model, now_ms=10_000), now_ms=10_000)
    assert first is not None

    changed = candidate(model, now_ms=310_000, reserve=D("1.00"))
    active = activator.offer(changed, now_ms=310_000)

    assert active is not None
    assert active.epoch_id != first.epoch_id
    assert active.thresholds == changed.thresholds
    assert active.valid_from_ms == 310_000


def test_activated_epoch_id_identifies_post_confirmation_thresholds(model):
    conservative = EpochActivator(model=model)
    conservative.offer(candidate(model, now_ms=10_000), now_ms=10_000)
    conservative.offer(candidate(model, now_ms=610_000, reserve=D("1.00")), now_ms=610_000)
    conservative_active = conservative.offer(
        candidate(model, now_ms=910_000, reserve=D("1.00")),
        now_ms=910_000,
    )

    unchanged = EpochActivator(model=model)
    unchanged.offer(candidate(model, now_ms=10_000), now_ms=10_000)
    unchanged.offer(candidate(model, now_ms=610_000), now_ms=610_000)
    unchanged_active = unchanged.offer(
        candidate(model, now_ms=910_000),
        now_ms=910_000,
    )

    assert conservative_active is not None
    assert unchanged_active is not None
    assert conservative_active.thresholds.buy.final != unchanged_active.thresholds.buy.final
    assert conservative_active.epoch_id != unchanged_active.epoch_id


def test_unchanged_confirmations_roll_epoch_before_expiry(model):
    activator = EpochActivator(model=model)
    active = activator.offer(candidate(model, now_ms=10_000), now_ms=10_000)
    assert active is not None
    original_id = active.epoch_id
    original_expiry = active.expires_at_ms

    assert activator.offer(candidate(model, now_ms=610_000), now_ms=610_000) is active
    rolled = activator.offer(candidate(model, now_ms=910_000), now_ms=910_000)
    assert rolled is not None
    assert rolled.epoch_id != original_id
    assert rolled.expires_at_ms > original_expiry
    assert rolled.thresholds.buy.final == active.thresholds.buy.final
    assert rolled.thresholds.sell.final == active.thresholds.sell.final


def test_epoch_step_is_limited_to_half_mad_1h(model):
    activator = EpochActivator(model=model)
    active = activator.offer(candidate(model, now_ms=310_000), now_ms=310_000)
    assert active is not None
    activator.offer(candidate(model, now_ms=1_210_000, reserve=D("10")), now_ms=1_210_000)
    proposal = candidate(model, now_ms=1_510_000, reserve=D("10"))
    updated = activator.offer(proposal, now_ms=1_510_000)
    assert updated is not None
    for side in Side:
        assert updated.component(side).final - active.component(side).final <= (
            D("0.50") * proposal.component(side).mad_1h
        )


def test_rolling_windows_are_live_five_thirty_and_sixty_minutes(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_601):
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001") + D(second) / D("1000000000"), D("-0.001")),
        )
    stats = store.snapshot(now_ms=start + 3_600_000)
    assert set(stats[Side.BUY]) == {5, 30, 60}
    assert stats[Side.BUY][5].ready
    assert stats[Side.BUY][30].ready
    assert stats[Side.BUY][60].ready
    assert all(window.source == "live" for window in stats[Side.BUY].values())


def test_rolling_hour_replaces_old_values_instead_of_freezing_first_hour(model):
    del model
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(70 * 60 + 1):
        old = second < 10 * 60
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(
                D("9") if old else D("0.001"),
                D("-9") if old else D("-0.001"),
            ),
        )

    stats = store.snapshot(now_ms=start + 70 * 60 * 1_000)

    assert stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].sample_count == 3_601
    assert stats[Side.BUY][60].median == D("0.001")
    assert stats[Side.SELL][60].median == D("-0.001")
    retained_count, retained_coverage_ms = store.coverage()
    assert retained_count <= 3_662
    assert retained_coverage_ms <= 3_660_000


def test_five_minute_window_is_complete_and_changes_formal_threshold(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(301):
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.123"), D("-0.456")),
        )
    stats = store.snapshot(now_ms=start + 300_000)

    assert stats[Side.BUY][5].ready
    assert stats[Side.BUY][5].median == D("0.123")
    assert store.coverage() == (301, 300_000)

    baseline_stats = {side: dict(model.calibration_stats[side]) for side in Side}
    changed_stats = {side: dict(windows) for side, windows in baseline_stats.items()}
    original_five = changed_stats[Side.BUY][5]
    changed_stats[Side.BUY][5] = replace(
        original_five,
        median=original_five.median + D("0.001"),
        q80=original_five.q80 + D("0.001"),
    )
    original = build_parameter_candidate(
        now_ms=10_000,
        model=model,
        config_hash=config_hash(),
        stats=baseline_stats,
        reference_notional_usd=D("500"),
        order_notional_usd=D("200"),
        reserve_bps_per_leg=D("0.50"),
        max_normal_round_wear_bps=D("1.0"),
    )
    changed = build_parameter_candidate(
        now_ms=10_000,
        model=model,
        config_hash=config_hash(),
        stats=changed_stats,
        reference_notional_usd=D("500"),
        order_notional_usd=D("200"),
        reserve_bps_per_leg=D("0.50"),
        max_normal_round_wear_bps=D("1.0"),
    )
    assert abs(
        changed.thresholds.buy.baseline
        - original.thresholds.buy.baseline
        - D("0.00015")
    ) < D("1e-27")
    assert abs(
        changed.thresholds.buy.q80
        - original.thresholds.buy.q80
        - D("0.00015")
    ) < D("1e-27")
    assert changed.epoch_id != original.epoch_id


def test_full_window_boundary_tolerates_sampling_clock_drift(model):
    store = RollingWindowStore()
    start = 1_000_000
    timestamp_ms = start
    while True:
        store.add(
            timestamp_ms=timestamp_ms,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
        if timestamp_ms - start >= 60 * 60 * 1_000:
            break
        timestamp_ms += 1_003
    now_ms = timestamp_ms
    stats = store.snapshot(now_ms=now_ms)
    assert stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].source == "live"


def test_one_hour_data_gap_blocks_formal_window(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(60 * 60 + 1):
        if 1_000 <= second <= 1_060:
            continue
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    stats = store.snapshot(now_ms=start + 60 * 60 * 1_000)
    assert not stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].reason == "data_gap"
    assert stats[Side.BUY][60].source == "live"


def test_one_hour_window_requires_a_full_hour(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_600):
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    stats = store.snapshot(now_ms=start + 3_599_000)
    assert not stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].reason == "insufficient_span"

    store.add(
        timestamp_ms=start + 3_600_000,
        rates=DirectionalRates(D("0.001"), D("-0.001")),
    )
    stats = store.snapshot(now_ms=start + 3_600_000)
    assert stats[Side.BUY][60].ready


def test_sixty_second_gap_blocks_window(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_601):
        if 1_000 <= second <= 1_061:
            continue
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    stats = store.snapshot(now_ms=start + 3_600_000)
    assert not stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].reason == "data_gap"


def test_exact_sixty_second_gap_blocks_window(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_601):
        if 1_001 <= second <= 1_059:
            continue
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    stats = store.snapshot(now_ms=start + 3_600_000)
    assert stats[Side.BUY][60].max_gap_ms == 60_000
    assert stats[Side.BUY][60].reason == "data_gap"


def test_explicit_restart_bridge_allows_only_that_gap(model):
    del model
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_601):
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    resumed_ms = start + 3_720_000
    store.add(
        timestamp_ms=resumed_ms,
        rates=DirectionalRates(D("0.001"), D("-0.001")),
        bridges_previous=True,
    )
    stats = store.snapshot(now_ms=resumed_ms)
    assert stats[Side.BUY][60].ready
    assert stats[Side.BUY][60].max_gap_ms < 60_000

    strict = RollingWindowStore()
    for second in range(3_601):
        strict.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    strict.add(
        timestamp_ms=resumed_ms,
        rates=DirectionalRates(D("0.001"), D("-0.001")),
    )
    assert strict.snapshot(now_ms=resumed_ms)[Side.BUY][60].reason == "data_gap"


def test_frozen_window_copy_is_detached_from_live_store(model):
    store = RollingWindowStore()
    start = 1_000_000
    for second in range(3_601):
        store.add(
            timestamp_ms=start + second * 1_000,
            rates=DirectionalRates(D("0.001"), D("-0.001")),
        )
    frozen = store.frozen_copy()
    assert not hasattr(frozen, "add")
    before = frozen.snapshot(now_ms=start + 3_600_000)

    store.add(
        timestamp_ms=start + 3_601_000,
        rates=DirectionalRates(D("0.5"), D("0.4")),
    )
    after = frozen.snapshot(now_ms=start + 3_600_000)
    assert after == before

    live_later = store.snapshot(now_ms=start + 3_601_000)
    frozen_later = frozen.snapshot(now_ms=start + 3_601_000)
    assert live_later[Side.BUY][60].sample_count == 3_601
    assert frozen_later[Side.BUY][60].sample_count == 3_600


def test_open_chooses_standardized_excess_then_lower_bound(model):
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    buy_threshold = epoch.thresholds.buy.final
    sell_threshold = epoch.thresholds.sell.final
    market = frame(
        at_ms=10_100,
        reference_buy=buy_threshold + epoch.thresholds.buy.mad_30m,
        reference_sell=sell_threshold + D("2") * epoch.thresholds.sell.mad_30m,
        actual_buy=buy_threshold + D("0.0002"),
        actual_sell=sell_threshold + D("0.0001"),
    )
    decision = engine.evaluate_open(frame=market, epoch=epoch, now_ms=10_100)
    assert decision.action is Action.OPEN
    assert decision.open_candidate.direction is Side.SELL
    assert decision.open_candidate.epoch is epoch


def test_independent_feed_update_cadence_is_not_a_trading_block(model):
    epoch = candidate(model, now_ms=10_000)
    market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=epoch.thresholds.buy.final + D("0.0002"),
    )
    decision = strategy_engine().evaluate_open(
        frame=replace(market, source_skew_ms=5_000),
        epoch=epoch,
        now_ms=10_100,
    )
    assert decision.action is Action.OPEN


def test_two_hundred_usd_economics_scale_every_monetary_term(model):
    epoch = candidate(model, now_ms=10_000)
    actual_rate = epoch.thresholds.buy.final + D("0.0002")
    market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=actual_rate,
        actual_notional=D("200"),
    )
    decision = strategy_engine().evaluate_open(
        frame=market,
        epoch=epoch,
        now_ms=10_100,
    )
    opened = decision.open_candidate
    assert opened is not None
    phase_reserve = D("200") * D("2") * D("0.50") / D("10000")
    expected = (
        D("200") * actual_rate
        + D("200") * epoch.thresholds.sell.baseline
        - D("2") * phase_reserve
    )
    assert opened.order_notional_usd == D("200")
    assert opened.actual_open_pnl_usd == D("200") * actual_rate
    assert opened.actual_round_lower_bound_usd == expected


def test_inflight_firm_guard_uses_frozen_epoch_and_symmetric_one_usd_tolerance(model):
    epoch = candidate(model, now_ms=10_000)
    engine = strategy_engine()
    initial = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=epoch.thresholds.buy.final + D("0.0002"),
    )
    candidate_open = engine.evaluate_open(frame=initial, epoch=epoch, now_ms=10_100).open_candidate
    assert candidate_open is not None
    firm = frame(
        at_ms=10_200,
        reference_buy=initial.reference_rates.buy,
        reference_sell=initial.reference_rates.sell,
        actual_buy=candidate_open.threshold + D("0.0002"),
        actual_sell=initial.actual_rates.sell,
    )
    upper_edge = engine.confirm_open(
        candidate=candidate_open,
        firm_frame=replace(firm, actual_notional_usd=D("201")),
        firm_notional_usd=D("201"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert upper_edge.action is Action.OPEN
    assert upper_edge.open_candidate.order_notional_usd == D("201")

    lower_real_quote = engine.confirm_open(
        candidate=candidate_open,
        firm_frame=replace(firm, actual_notional_usd=D("199.7438201")),
        firm_notional_usd=D("199.7438201"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert lower_real_quote.action is Action.OPEN
    assert lower_real_quote.open_candidate.order_notional_usd == D("199.7438201")

    blocked = engine.confirm_open(
        candidate=candidate_open,
        firm_frame=replace(firm, actual_notional_usd=D("201.00000001")),
        firm_notional_usd=D("201.00000001"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert blocked.action is Action.PAUSE
    assert blocked.reason == "firm_notional_exceeds_target_amount"
    passed = engine.confirm_open(
        candidate=candidate_open,
        firm_frame=firm,
        firm_notional_usd=D("200"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert passed.action is Action.OPEN
    assert passed.open_candidate.epoch is epoch

    undersized = engine.confirm_open(
        candidate=candidate_open,
        firm_frame=replace(firm, actual_notional_usd=D("198.99999999")),
        firm_notional_usd=D("198.99999999"),
        target_notional_usd=D("200"),
        now_ms=10_200,
    )
    assert undersized.action is Action.PAUSE
    assert undersized.reason == "firm_notional_below_target_amount"


def test_firm_tolerance_is_relative_to_any_configured_target(model):
    engine = strategy_engine()
    for target in (D("50"), D("500"), D("1234.56")):
        epoch = candidate(model, now_ms=10_000, order_notional=target)
        initial = frame(
            at_ms=10_100,
            reference_buy=epoch.thresholds.buy.final + D("0.0001"),
            reference_sell=epoch.thresholds.sell.final - D("0.001"),
            actual_buy=epoch.thresholds.buy.final + D("0.0002"),
            actual_notional=target,
        )
        opened = engine.evaluate_open(
            frame=initial,
            epoch=epoch,
            now_ms=10_100,
        ).open_candidate
        assert opened is not None
        for firm_notional in (target - D("1"), target, target + D("1")):
            confirmed = engine.confirm_open(
                candidate=opened,
                firm_frame=replace(
                    initial,
                    captured_at_ms=10_200,
                    actual_notional_usd=firm_notional,
                ),
                firm_notional_usd=firm_notional,
                target_notional_usd=target,
                now_ms=10_200,
            )
            assert confirmed.action is Action.OPEN
            assert confirmed.open_candidate is not None
            assert confirmed.open_candidate.order_notional_usd == firm_notional

        too_high = target + D("1.00000001")
        blocked = engine.confirm_open(
            candidate=opened,
            firm_frame=replace(initial, captured_at_ms=10_200, actual_notional_usd=too_high),
            firm_notional_usd=too_high,
            target_notional_usd=target,
            now_ms=10_200,
        )
        assert blocked.action is Action.PAUSE
        assert blocked.reason == "firm_notional_exceeds_target_amount"


def test_captured_template_target_can_differ_from_sampling_notional(model):
    epoch = candidate(model, now_ms=10_000, order_notional=D("200"))
    engine = strategy_engine()
    initial = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=epoch.thresholds.sell.final - D("0.001"),
        actual_buy=epoch.thresholds.buy.final + D("0.0002"),
        actual_notional=D("200"),
    )
    opened = engine.evaluate_open(
        frame=initial,
        epoch=epoch,
        now_ms=10_100,
    ).open_candidate
    assert opened is not None
    firm_notional = D("500.60")
    confirmed = engine.confirm_open(
        candidate=opened,
        firm_frame=replace(
            initial,
            captured_at_ms=10_200,
            actual_notional_usd=firm_notional,
        ),
        firm_notional_usd=firm_notional,
        target_notional_usd=D("500"),
        now_ms=10_200,
    )
    assert confirmed.action is Action.OPEN
    assert confirmed.open_candidate is not None
    assert confirmed.open_candidate.order_notional_usd == firm_notional


def test_close_floors(model):
    cases = [
        (1_000, D("0.05"), D("-0.0001"), Action.NO_ACTION, D("0")),
        (1_000, D("0"), D("-0.0001"), Action.NO_ACTION, D("0")),
        (1_900, D("0"), D("0"), Action.CLOSE, D("-0.02")),
        (7_300, D("0.05"), D("-0.0001"), Action.CLOSE, D("-0.02")),
    ]
    for held_seconds, open_pnl, close_rate, expected_action, floor in cases:
        epoch = candidate(model, now_ms=10_000)
        now_ms = 20_000_000
        market = frame(
            at_ms=now_ms,
            reference_buy=D("0"),
            reference_sell=close_rate,
            actual_sell=close_rate,
        )
        position = PositionContext(
            strategy_tag="adaptive-median-v1",
            open_direction=Side.BUY,
            opened_at_ms=now_ms - held_seconds * 1_000,
            actual_base_qty=D("2"),
            actual_notional_usd=D("200"),
            actual_open_pnl_usd=open_pnl,
            epoch=epoch,
        )
        close_pnl = D("200") * close_rate
        market = replace(
            market,
            var_bid=D("100"),
            var_ask=D("100"),
            lighter_actual_buy_vwap=D("100") - close_pnl / D("2"),
        )
        decision = strategy_engine().evaluate_close(frame=market, position=position, now_ms=now_ms)
        assert decision.action is expected_action
        assert decision.close_candidate is not None
        assert decision.close_candidate.required_floor_usd == floor
        assert decision.close_candidate.max_hold_alert is (held_seconds >= 7_200)


def test_close_uses_actual_base_qty_and_current_close_notional(model):
    epoch = candidate(model, now_ms=10_000)
    now_ms = 20_000_000
    market = replace(
        frame(
            at_ms=now_ms,
            reference_buy=D("0"),
            reference_sell=D("0"),
            # Deliberately inconsistent: close economics must use exact prices,
            # not this fixed-order-size rate.
            actual_buy=D("9"),
            actual_sell=D("9"),
        ),
        var_bid=D("150"),
        var_ask=D("151"),
        lighter_actual_buy_vwap=D("149.99"),
        lighter_actual_sell_vwap=D("151.02"),
    )
    engine = strategy_engine()

    buy_open = PositionContext(
        "adaptive-median-v1",
        Side.BUY,
        now_ms - 1_000_000,
        D("2"),
        D("200"),
        D("0.01"),
        epoch,
    )
    buy_close = engine.evaluate_close(
        frame=replace(market, actual_notional_usd=D("300")),
        position=buy_open,
        now_ms=now_ms,
    )
    assert buy_close.close_candidate is not None
    assert buy_close.close_candidate.expected_close_pnl_usd == D("0.02")
    assert buy_close.close_candidate.actual_close_rate == D("0.02") / D("300")
    assert buy_close.close_candidate.close_reserve_usd == D("0.015")
    assert buy_close.close_candidate.required_floor_usd == D("0")

    sell_open = PositionContext(
        "adaptive-median-v1",
        Side.SELL,
        now_ms - 1_900_000,
        D("2"),
        D("200"),
        D("0"),
        epoch,
    )
    sell_close = engine.evaluate_close(
        frame=replace(market, actual_notional_usd=D("302")),
        position=sell_open,
        now_ms=now_ms,
    )
    assert sell_close.close_candidate is not None
    assert sell_close.close_candidate.expected_close_pnl_usd == D("0.04")
    assert sell_close.close_candidate.actual_close_rate == D("0.04") / D("302")
    assert sell_close.close_candidate.close_reserve_usd == D("0.01510")
    # Wear remains 1bps of the actual 200U opening amount, not 302U.
    assert sell_close.close_candidate.required_floor_usd == D("-0.02")


def test_close_requires_weighted_baseline_regression_and_pnl_floor(model):
    epoch = candidate(model, now_ms=10_000)
    now_ms = 20_000_000
    position = PositionContext(
        "adaptive-median-v1",
        Side.BUY,
        now_ms - 31 * 60 * 1_000,
        D("2"),
        D("200"),
        D("1"),
        epoch,
    )
    market = replace(
        frame(at_ms=now_ms, reference_buy=D("0"), reference_sell=D("0")),
        var_bid=D("100"),
        lighter_actual_buy_vwap=D("100.2"),
        actual_notional_usd=D("200"),
    )
    waiting = strategy_engine().evaluate_close(
        frame=market,
        position=position,
        now_ms=now_ms,
    )
    assert waiting.action is Action.NO_ACTION
    assert waiting.reason == "close_baseline_regression_not_met"
    assert waiting.close_candidate is not None
    assert waiting.close_candidate.round_lower_bound_usd > D("0")
    assert not waiting.close_candidate.regression_passed
    assert waiting.close_candidate.regression_target_rate == epoch.thresholds.sell.baseline

    regressed = strategy_engine().evaluate_close(
        frame=replace(market, lighter_actual_buy_vwap=D("100.05")),
        position=position,
        now_ms=now_ms,
    )
    assert regressed.action is Action.CLOSE
    assert regressed.close_candidate is not None
    assert regressed.close_candidate.regression_passed


def test_manual_position_is_not_strategy_closed(model):
    epoch = candidate(model, now_ms=10_000)
    market = frame(at_ms=20_000, reference_buy=D("1"), reference_sell=D("1"))
    position = PositionContext("manual", Side.BUY, 10_000, D("2"), D("200"), D("1"), epoch)
    decision = strategy_engine().evaluate_close(frame=market, position=position, now_ms=20_000)
    assert decision.action is Action.NO_ACTION
    assert decision.reason == "manual_position_not_strategy_managed"


def test_missing_frozen_close_context_fails_closed():
    market = frame(at_ms=20_000, reference_buy=D("1"), reference_sell=D("1"))
    position = PositionContext(
        "adaptive-median-v1", Side.BUY, 10_000, D("2"), D("200"), D("1"), None
    )
    decision = strategy_engine().evaluate_close(frame=market, position=position, now_ms=20_000)
    assert decision.action is Action.PAUSE


def test_hot_decision_p95_below_one_millisecond(model):
    epoch = candidate(model, now_ms=10_000)
    market = frame(
        at_ms=10_100,
        reference_buy=epoch.thresholds.buy.final + D("0.0001"),
        reference_sell=epoch.thresholds.sell.final - D("0.0001"),
    )
    engine = strategy_engine()
    durations = []
    for _ in range(10_000):
        started = time.perf_counter_ns()
        engine.evaluate_open(frame=market, epoch=epoch, now_ms=10_100)
        durations.append(time.perf_counter_ns() - started)
    durations.sort()
    p95_ms = D(durations[9_499]) / D("1000000")
    assert p95_ms < D("1")


def load_tests(loader, standard_tests, pattern):
    """Expose compact function-style formula cases to unittest discovery."""

    del loader, standard_tests, pattern
    suite = unittest.TestSuite()
    for name, function in sorted(globals().items()):
        if not name.startswith("test_") or not callable(function):
            continue
        parameters = inspect.signature(function).parameters

        def run(function=function, parameters=parameters):
            if parameters:
                function(MODEL)
            else:
                function()

        suite.addTest(unittest.FunctionTestCase(run, description=name))
    return suite
