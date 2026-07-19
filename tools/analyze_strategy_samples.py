#!/usr/bin/env python3
"""Quality report for strategy market sample files.

This is deliberately formula-neutral. It checks whether the retained sample
file can rebuild the live 5m/30m/1h runtime windows. Longer research history
belongs in the external research database, not in this rolling runtime file.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


ONE_HOUR_MS = 60 * 60 * 1_000
SAMPLE_VERSION = "adaptive-market-sample-v1"
MAX_QUALIFIED_GAP_MS = 60 * 1_000
MIN_VALID_DENSITY_PER_SECOND = Decimal("0.10")
WINDOWS_MS = {
    "5m": 5 * 60 * 1_000,
    "30m": 30 * 60 * 1_000,
    "1h": ONE_HOUR_MS,
}


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _median(values: Iterable[Decimal]) -> Decimal | None:
    ordered = sorted(values)
    if not ordered:
        return None
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal("2")


def _nearest_rank(values: Iterable[Decimal], numerator: int) -> Decimal | None:
    ordered = sorted(values)
    if not ordered:
        return None
    rank = (numerator * len(ordered) + 99) // 100
    return ordered[rank - 1]


def _decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    malformed = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if isinstance(row, dict):
                rows.append(row)
            else:
                malformed += 1
    return rows, malformed


def _identity(row: dict[str, Any]) -> tuple[str, int, str, str] | None:
    if row.get("version") != SAMPLE_VERSION:
        return None
    asset = str(row.get("asset") or "").strip().upper()
    reference_notional = str(row.get("reference_notional_usd") or "").strip()
    order_notional = str(row.get("order_notional_usd") or "").strip()
    try:
        generation = int(row.get("market_generation"))
    except (TypeError, ValueError):
        return None
    if not asset or generation <= 0 or not reference_notional or not order_notional:
        return None
    return asset, generation, reference_notional, order_notional


def latest_identity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timestamped = [
        row
        for row in rows
        if isinstance(row.get("sample_timestamp_ms"), int)
        and _identity(row) is not None
    ]
    if not timestamped:
        return []
    latest = max(timestamped, key=lambda row: row["sample_timestamp_ms"])
    selected_identity = _identity(latest)
    selected = [row for row in timestamped if _identity(row) == selected_identity]
    selected.sort(key=lambda row: row["sample_timestamp_ms"])
    return selected


def build_report(
    rows: list[dict[str, Any]],
    *,
    malformed_rows: int = 0,
) -> dict[str, Any]:
    selected = latest_identity_rows(rows)
    if not selected:
        return {
            "qualified_runtime_1h": False,
            "reason": "no timestamped rows with a complete identity",
            "malformed_rows": malformed_rows,
        }

    first_ms = selected[0]["sample_timestamp_ms"]
    latest_ms = selected[-1]["sample_timestamp_ms"]
    coverage_ms = max(0, latest_ms - first_ms)
    valid_rows = [
        row
        for row in selected
        if row.get("valid") is True
        and _decimal(row.get("reference_buy_rate")) is not None
        and _decimal(row.get("reference_sell_rate")) is not None
    ]
    valid_timestamps = [row["sample_timestamp_ms"] for row in valid_rows]
    max_gap_ms = (
        max(current - previous for previous, current in zip(valid_timestamps, valid_timestamps[1:]))
        if len(valid_timestamps) >= 2
        else None
    )
    density = (
        Decimal(len(valid_rows) - 1) * Decimal(1_000) / Decimal(coverage_ms)
        if len(valid_rows) >= 2 and coverage_ms > 0
        else Decimal("0")
    )
    invalid_reasons = Counter(
        str(row.get("rejection_reason") or "unclassified")
        for row in selected
        if row.get("valid") is not True
    )

    window_stats: dict[str, Any] = {}
    for label, window_ms in WINDOWS_MS.items():
        cutoff = latest_ms - window_ms
        window_rows = [
            row for row in valid_rows if row["sample_timestamp_ms"] >= cutoff
        ]
        side_stats: dict[str, Any] = {"count": len(window_rows)}
        for side in ("buy", "sell"):
            values = [
                value
                for row in window_rows
                if (value := _decimal(row.get(f"reference_{side}_rate"))) is not None
            ]
            center = _median(values)
            deviations = (
                [abs(value - center) for value in values]
                if center is not None
                else []
            )
            side_stats[side] = {
                "median": _decimal_text(center),
                "q20": _decimal_text(_nearest_rank(values, 20)),
                "q80": _decimal_text(_nearest_rank(values, 80)),
                "mad": _decimal_text(_median(deviations)),
            }
        window_stats[label] = side_stats

    qualified = (
        coverage_ms >= ONE_HOUR_MS
        and density >= MIN_VALID_DENSITY_PER_SECOND
        and max_gap_ms is not None
        and max_gap_ms < MAX_QUALIFIED_GAP_MS
        and malformed_rows == 0
    )
    identity = _identity(selected[-1])
    assert identity is not None
    return {
        "qualified_runtime_1h": qualified,
        "identity": {
            "asset": identity[0],
            "market_generation": identity[1],
            "reference_notional_usd": identity[2],
            "order_notional_usd": identity[3],
        },
        "first_timestamp_ms": first_ms,
        "latest_timestamp_ms": latest_ms,
        "coverage_ms": coverage_ms,
        "coverage_hours": str(Decimal(coverage_ms) / Decimal(3_600_000)),
        "rows": len(selected),
        "valid_rows": len(valid_rows),
        "invalid_rows": len(selected) - len(valid_rows),
        "malformed_rows": malformed_rows,
        "valid_density_per_second": _decimal_text(density),
        "max_valid_gap_ms": max_gap_ms,
        "invalid_reasons": dict(sorted(invalid_reasons.items())),
        "windows": window_stats,
        "requirements": {
            "coverage_ms": ONE_HOUR_MS,
            "min_valid_density_per_second": str(MIN_VALID_DENSITY_PER_SECOND),
            "max_valid_gap_ms": MAX_QUALIFIED_GAP_MS,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=Path("log/strategy_market_samples.jsonl"),
    )
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.path.is_file():
        print(f"sample file not found: {args.path}")
        return 2
    rows, malformed = load_jsonl(args.path)
    report = build_report(rows, malformed_rows=malformed)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"qualified_runtime_1h: {report.get('qualified_runtime_1h')}")
        print(f"identity: {report.get('identity', '-')}")
        print(f"coverage_hours: {report.get('coverage_hours', '0')}")
        print(
            "rows: "
            f"valid={report.get('valid_rows', 0)} "
            f"invalid={report.get('invalid_rows', 0)} "
            f"malformed={report.get('malformed_rows', 0)}"
        )
        print(f"density_per_second: {report.get('valid_density_per_second', '0')}")
        print(f"max_valid_gap_ms: {report.get('max_valid_gap_ms', '-')}")
        print(f"invalid_reasons: {report.get('invalid_reasons', {})}")
        print(json.dumps(report.get("windows", {}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
