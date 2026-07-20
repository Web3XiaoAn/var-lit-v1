#!/usr/bin/env python3
"""Analyze server-side open-signal survival observations without copying the DB."""

from __future__ import annotations

import argparse
import json
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any, Iterable


EVENT = "open-survival-observation-v1"
ONE_HOUR_MS = 3_600_000
MIN_ROWS_PER_HOUR = 600
MAX_GAP_MS = 10_000
NEAR_TRIGGER_BPS = Decimal("0.5")


def _decimal(value: object) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _quantile(values: Iterable[Decimal | int], percentile: int) -> str | None:
    ordered = sorted(Decimal(value) for value in values)
    if not ordered:
        return None
    rank = max(0, (percentile * len(ordered) + 99) // 100 - 1)
    return format(ordered[rank], "f")


def load_rows(database: Path, session_id: str | None = None) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        payloads = connection.execute(
            "SELECT payload_json FROM research_events "
            "WHERE stream = 'execution_trace' AND payload_json LIKE ? "
            "ORDER BY event_time_ms",
            (f'%"event":"{EVENT}"%',),
        )
        rows = [json.loads(payload) for (payload,) in payloads]
    finally:
        connection.close()
    rows = [row for row in rows if row.get("event") == EVENT]
    if not rows:
        return []
    selected = session_id or rows[-1].get("session_id")
    return [row for row in rows if row.get("session_id") == selected]


def _rates(rows: list[dict[str, Any]], side: str, cohort: str) -> dict[str, Any]:
    selected = []
    for row in rows:
        start = _decimal(row.get("margins", {}).get(side))
        if start is None:
            continue
        if cohort == "trigger" and start <= 0:
            continue
        if cohort == "near" and start < -(NEAR_TRIGGER_BPS / Decimal(10_000)):
            continue
        selected.append((row, start))

    horizons: dict[str, Any] = {}
    for target in (0, 100, 250, 450, 1000):
        outcomes: list[Decimal] = []
        adverse_bps: list[Decimal] = []
        for row, start in selected:
            snapshot = next(
                (
                    item
                    for item in row.get("snapshots", [])
                    if item.get("target_offset_ms") == target and item.get("available") is True
                ),
                None,
            )
            future = _decimal(snapshot.get(f"{side.lower()}_margin")) if snapshot else None
            if future is None:
                continue
            outcomes.append(future)
            adverse_bps.append((start - future) * Decimal(10_000))
        horizons[str(target)] = {
            "available": len(outcomes),
            "future_positive_rate": (
                format(Decimal(sum(value > 0 for value in outcomes)) / Decimal(len(outcomes)), ".6f")
                if outcomes
                else None
            ),
            "adverse_move_bps_p50": _quantile(adverse_bps, 50),
            "adverse_move_bps_p80": _quantile(adverse_bps, 80),
            "adverse_move_bps_p95": _quantile(adverse_bps, 95),
        }
    return {"count": len(selected), "horizons_ms": horizons}


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"qualified_one_hour": False, "reason": "no survival observations"}
    rows.sort(key=lambda row: int(row.get("sample_timestamp_ms") or 0))
    timestamps = [int(row["sample_timestamp_ms"]) for row in rows]
    gaps = [current - previous for previous, current in zip(timestamps, timestamps[1:])]
    coverage_ms = timestamps[-1] - timestamps[0]
    snapshots = [item for row in rows for item in row.get("snapshots", [])]
    available = [item for item in snapshots if item.get("available") is True]
    timing_errors = [
        int(item["actual_offset_ms"]) - int(item["target_offset_ms"])
        for item in available
    ]
    expected_snapshots = len(rows) * 5
    complete_rate = Decimal(len(available)) / Decimal(expected_snapshots)
    return {
        "qualified_one_hour": (
            coverage_ms >= ONE_HOUR_MS
            and len(rows) >= MIN_ROWS_PER_HOUR
            and (max(gaps) if gaps else MAX_GAP_MS + 1) <= MAX_GAP_MS
            and complete_rate >= Decimal("0.95")
        ),
        "session_id": rows[-1].get("session_id"),
        "mode": rows[-1].get("mode"),
        "asset": rows[-1].get("asset"),
        "model_version": rows[-1].get("model_version"),
        "first_timestamp_ms": timestamps[0],
        "last_timestamp_ms": timestamps[-1],
        "coverage_ms": coverage_ms,
        "coverage_hours": format(Decimal(coverage_ms) / Decimal(ONE_HOUR_MS), ".6f"),
        "rows": len(rows),
        "cadence_ms": {
            "median": str(median(gaps)) if gaps else None,
            "p95": _quantile(gaps, 95),
            "max": max(gaps) if gaps else None,
        },
        "snapshot_complete_rate": format(complete_rate, ".6f"),
        "timing_error_ms": {
            "p50": _quantile(timing_errors, 50),
            "p95": _quantile(timing_errors, 95),
            "max": max(timing_errors) if timing_errors else None,
        },
        "source_latency_ms": {
            name: {
                "p50": _quantile(
                    (
                        int(row[name])
                        for row in rows
                        if isinstance(row.get(name), int)
                    ),
                    50,
                ),
                "p95": _quantile(
                    (
                        int(row[name])
                        for row in rows
                        if isinstance(row.get(name), int)
                    ),
                    95,
                ),
            }
            for name in ("var_age_ms", "lighter_age_ms", "source_skew_ms")
        },
        "sides": {
            side: {
                "trigger": _rates(rows, side, "trigger"),
                "within_0_5bps": _rates(rows, side, "near"),
            }
            for side in ("BUY", "SELL")
        },
        "requirements": {
            "coverage_ms": ONE_HOUR_MS,
            "min_rows": MIN_ROWS_PER_HOUR,
            "max_gap_ms": MAX_GAP_MS,
            "min_snapshot_complete_rate": "0.95",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--session-id")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.database.is_file():
        print(f"database not found: {args.database}")
        return 2
    print(json.dumps(build_report(load_rows(args.database, args.session_id)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
