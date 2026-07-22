#!/usr/bin/env python3
"""Read-only report for execution-survival observations and live rounds."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


SURVIVAL_HORIZONS = (100, 250, 450, 1000)
FLOW_HORIZONS = (100, 250, 500, 1000, 2000)
LATENCY_PAIRS = {
    "var_quote_ms": ("variational_quote_dispatch", "variational_quote_result"),
    "var_commit_ms": ("variational_commit_dispatch", "variational_commit_result"),
    "commit_to_lighter_ack_ms": ("variational_commit_result", "lighter_order_ack"),
    "commit_to_lighter_fill_ms": ("variational_commit_result", "lighter_fill"),
    "lighter_ack_to_fill_ms": ("lighter_order_ack", "lighter_fill"),
}


def decimal(value: object) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def percentile(values: Iterable[Decimal], fraction: Decimal) -> str | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = int((len(ordered) * fraction).to_integral_value(rounding="ROUND_CEILING"))
    index = max(0, min(len(ordered) - 1, rank - 1))
    return format(ordered[index], "f")


def ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return format(Decimal(numerator) / Decimal(denominator), ".6f")


def summarize(values: Iterable[Decimal]) -> dict[str, Any] | None:
    clean = [value for value in values if value.is_finite() and value >= 0]
    if not clean:
        return None
    return {
        "count": len(clean),
        "p50": percentile(clean, Decimal("0.50")),
        "p95": percentile(clean, Decimal("0.95")),
        "p99": percentile(clean, Decimal("0.99")),
        "max": format(max(clean), "f"),
        "mean": format(sum(clean) / len(clean), "f"),
    }


def latency_report(payloads: Iterable[dict[str, Any]]) -> dict[str, Any]:
    traces: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    outcomes: dict[str, Counter[str]] = defaultdict(Counter)
    direct: dict[str, list[Decimal]] = defaultdict(list)
    for row in payloads:
        event = str(row.get("event") or "")
        trace_id = str(row.get("trace_id") or "")
        monotonic_ns = row.get("monotonic_ns")
        if trace_id and isinstance(monotonic_ns, int) and not isinstance(monotonic_ns, bool):
            traces[trace_id][event].append(monotonic_ns)
        if event in {"variational_quote_result", "variational_commit_result"}:
            outcomes[event]["ok" if row.get("ok") is True else "error"] += 1
        for metric, field, divisor in (
            ("var_browser_quote_ms", "browser_quote_elapsed_ms", Decimal("1")),
            ("var_browser_commit_ms", "browser_elapsed_ms", Decimal("1")),
            ("lighter_round_trip_ms", "round_trip_ns", Decimal("1000000")),
        ):
            if (value := decimal(row.get(field))) is not None:
                direct[metric].append(value / divisor)

    metrics: dict[str, Any] = {}
    for name, (start_event, end_event) in LATENCY_PAIRS.items():
        values: list[Decimal] = []
        for events in traces.values():
            starts = sorted(events.get(start_event, ()))
            ends = sorted(events.get(end_event, ()))
            if starts and (end := next((item for item in ends if item >= starts[0]), None)):
                values.append(Decimal(end - starts[0]) / Decimal("1000000"))
        if (summary := summarize(values)) is not None:
            metrics[name] = summary
    for name, values in direct.items():
        if (summary := summarize(values)) is not None:
            metrics[name] = summary
    return {
        "trace_count": len(traces),
        "outcomes": {event: dict(counts) for event, counts in outcomes.items()},
        "metrics_ms": metrics,
    }


def _market_quality_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = sorted(
        (
            row
            for row in rows
            if isinstance(row.get("sample_timestamp_ms"), int)
        ),
        key=lambda row: row["sample_timestamp_ms"],
    )
    if not rows:
        return {"rows": 0, "valid_rows": 0, "coverage_hours": "0"}
    valid = [row for row in rows if row.get("valid") is True]
    timestamps = [row["sample_timestamp_ms"] for row in valid]
    coverage_ms = rows[-1]["sample_timestamp_ms"] - rows[0]["sample_timestamp_ms"]
    max_gap_ms = max(
        (current - previous for previous, current in zip(timestamps, timestamps[1:])),
        default=None,
    )
    density = (
        Decimal(max(0, len(valid) - 1)) * 1_000 / coverage_ms
        if coverage_ms > 0
        else Decimal(0)
    )
    return {
        "rows": len(rows),
        "valid_rows": len(valid),
        "invalid_rows": len(rows) - len(valid),
        "coverage_hours": format(Decimal(coverage_ms) / Decimal("3600000"), "f"),
        "valid_density_per_second": format(density, "f"),
        "max_valid_gap_ms": max_gap_ms,
        "invalid_reasons": dict(
            Counter(
                str(row.get("rejection_reason") or "unclassified")
                for row in rows
                if row.get("valid") is not True
            )
        ),
    }


def market_quality_report(payloads: Iterable[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in payloads:
        groups[
            (
                str(row.get("asset") or "unknown").upper(),
                str(row.get("mode") or "unknown"),
                str(row.get("sample_kind") or "market_background"),
                str(row.get("model_version") or "unversioned"),
            )
        ].append(row)
    return {
        "/".join(key): _market_quality_group(rows)
        for key, rows in sorted(groups.items())
    }


def observation_report(payloads: Iterable[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in payloads:
        if row.get("event") != "open-survival-observation-v2":
            continue
        key = (
            str(row.get("asset") or "unknown").upper(),
            str(row.get("mode") or "unknown"),
            str(row.get("sample_kind") or "unknown"),
            str(row.get("model_version") or "unversioned"),
            str(row.get("open_survival_policy_version") or "unversioned"),
        )
        group = groups.setdefault(
            key,
            {
                "observations": 0,
                "policy_versions": defaultdict(int),
                "sides": {},
                "feature_completeness": defaultdict(lambda: defaultdict(int)),
            },
        )
        group["observations"] += 1
        group["policy_versions"][str(row.get("open_survival_policy_version") or "unknown")] += 1
        threshold_sides = set(row.get("threshold_pass_sides") or ())
        policy_sides = set(row.get("policy_pass_sides") or ())
        snapshots = {
            int(item["target_offset_ms"]): item
            for item in row.get("snapshots") or ()
            if isinstance(item, dict) and isinstance(item.get("target_offset_ms"), int)
        }
        reserves = row.get("survival_reserve_bps") or {}
        for side in ("BUY", "SELL"):
            stats = group["sides"].setdefault(
                side,
                {
                    "threshold_candidates": 0,
                    "policy_candidates": 0,
                    "reserve_bps": [],
                    "survival": {h: {"available": 0, "survived": 0} for h in SURVIVAL_HORIZONS},
                },
            )
            if side in threshold_sides:
                stats["threshold_candidates"] += 1
            if side not in policy_sides:
                continue
            stats["policy_candidates"] += 1
            if (reserve := decimal(reserves.get(side))) is not None:
                stats["reserve_bps"].append(reserve)
            for horizon in SURVIVAL_HORIZONS:
                snapshot = snapshots.get(horizon)
                margin = decimal((snapshot or {}).get(f"{side.lower()}_margin"))
                if snapshot and snapshot.get("available") is True and margin is not None:
                    stats["survival"][horizon]["available"] += 1
                    stats["survival"][horizon]["survived"] += margin >= 0

        feature_snapshots = {
            int(item["target_offset_ms"]): item
            for item in row.get("feature_snapshots") or ()
            if isinstance(item, dict)
            and isinstance(item.get("target_offset_ms"), int)
        }
        for horizon in FLOW_HORIZONS:
            values = feature_snapshots.get(horizon) or {}
            for field in ("book_flow_usd", "trade_flow_usd", "past_return_bps"):
                group["feature_completeness"][horizon][field] += decimal(values.get(field)) is not None
            group["feature_completeness"][horizon]["microprice_bps"] += (
                decimal(values.get("microprice_bps")) is not None
            )
            one_bps = (values.get("depth") or {}).get("bands_bps", {}).get("1")
            group["feature_completeness"][horizon]["depth_1bps"] += (
                isinstance(one_bps, dict)
                and decimal(one_bps.get("bid_usd")) is not None
                and decimal(one_bps.get("ask_usd")) is not None
            )

    report: dict[str, Any] = {}
    for (asset, mode, sample_kind, model_version, policy_version), group in sorted(groups.items()):
        observations = group["observations"]
        sides = {}
        for side, stats in group["sides"].items():
            survival = {
                str(horizon): {
                    **counts,
                    "rate": ratio(counts["survived"], counts["available"]),
                }
                for horizon, counts in stats["survival"].items()
            }
            sides[side] = {
                "threshold_candidates": stats["threshold_candidates"],
                "policy_candidates": stats["policy_candidates"],
                "reserve_bps_p50": percentile(stats["reserve_bps"], Decimal("0.50")),
                "reserve_bps_p90": percentile(stats["reserve_bps"], Decimal("0.90")),
                "economic_survival": survival,
            }
        completeness = {
            str(horizon): {
                field: {"present": count, "rate": ratio(count, observations)}
                for field, count in fields.items()
            }
            for horizon, fields in group["feature_completeness"].items()
        }
        report[
            f"{asset}/{mode}/{sample_kind}/{model_version}/{policy_version}"
        ] = {
            "observations": observations,
            "policy_versions": dict(group["policy_versions"]),
            "sides": sides,
            "feature_completeness": completeness,
        }
    return report


def round_report(rows: Iterable[sqlite3.Row]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        context = json.loads(row["open_context_json"] or "{}")
        payload = json.loads(row["payload_json"] or "{}")
        strategy = str(row["strategy"] or context.get("strategyTag") or "unversioned")
        policy = str(context.get("openSurvivalPolicyVersion") or "unversioned")
        hedge_policy = str(
            context.get("openHedgeRecoveryPolicyVersion")
            or "legacy-open-hedge-recovery"
        )
        sample_kind = str(payload.get("round_class") or "strategy_round")
        key = (
            str(row["asset"] or "unknown").upper(),
            "live",
            sample_kind,
            str(row["direction"]),
            strategy,
            policy,
            hedge_policy,
        )
        group = groups.setdefault(
            key,
            {
                "rounds": 0,
                "positive": 0,
                "negative": 0,
                "recoveries": 0,
                "quality": Counter(),
                "pnl": Decimal(0),
                "open_loss": Decimal(0),
                "close_loss": Decimal(0),
            },
        )
        pnl = decimal(row["round_pnl_usd"]) or Decimal(0)
        group["rounds"] += 1
        group["positive"] += pnl >= 0
        group["negative"] += pnl < 0
        group["recoveries"] += payload.get("round_class") == "protective_recovery"
        group["quality"][str(row["effective_quality"] or "unflagged")] += 1
        group["pnl"] += pnl
        group["open_loss"] += decimal(row["open_execution_loss_usd"]) or Decimal(0)
        group["close_loss"] += decimal(row["close_execution_loss_usd"]) or Decimal(0)
    return {
        "/".join(key): {
            **{
                name: dict(value) if isinstance(value, Counter) else value
                for name, value in group.items()
                if not isinstance(value, Decimal)
            },
            "pnl_usd": format(group["pnl"], "f"),
            "average_pnl_usd": format(group["pnl"] / group["rounds"], "f"),
            "open_execution_loss_usd": format(group["open_loss"], "f"),
            "close_execution_loss_usd": format(group["close_loss"], "f"),
        }
        for key, group in sorted(groups.items())
    }


def build_report(path: Path, since_ms: int | None = None) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    with sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        filters = "stream IN ('execution_trace', 'strategy_market_sample')"
        params: tuple[Any, ...] = ()
        if since_ms is not None:
            filters += " AND event_time_ms >= ?"
            params = (since_ms,)
        payloads = [
            (row[0], json.loads(row[1]))
            for row in connection.execute(
                "SELECT stream, payload_json FROM research_events "
                f"WHERE {filters} ORDER BY event_time_ms",
                params,
            )
        ]
        round_where = "WHERE closed_at_ms >= ?" if since_ms is not None else ""
        rounds = connection.execute(
            f"SELECT * FROM research_round_quality {round_where} ORDER BY closed_at_ms",
            params,
        ).fetchall()
    traces = [payload for stream, payload in payloads if stream == "execution_trace"]
    market = [payload for stream, payload in payloads if stream == "strategy_market_sample"]
    return {
        "schema": "execution-survival-analysis-v1",
        "database": str(resolved),
        "database_bytes": resolved.stat().st_size,
        "since_ms": since_ms,
        "market_quality": market_quality_report(market),
        "latency": latency_report(traces),
        "observations": observation_report(traces),
        "rounds": round_report(rounds),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--since-hours", type=Decimal)
    args = parser.parse_args()
    since_ms = None
    if args.since_hours is not None:
        since_ms = int(time.time() * 1_000) - int(args.since_hours * 3_600_000)
    print(json.dumps(build_report(args.database, since_ms), indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
