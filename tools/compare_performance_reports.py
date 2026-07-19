#!/usr/bin/env python3
"""Compare two runtime-performance reports and fail on latency regression.

Both inputs must be JSON emitted by ``analyze_runtime_performance.py --json``.
The comparison is offline and never imports or contacts the trading runtime.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


SCHEMA = "variational-runtime-performance-v1"
GATED_METRICS = (
    "variational_quote_total_ms",
    "variational_browser_quote_ms",
    "variational_commit_total_ms",
    "variational_browser_commit_ms",
    "commit_result_to_lighter_send_ms",
    "lighter_queue_wait_ms",
    "lighter_order_round_trip_ms",
)
ABSOLUTE_P95_CAPS_MS = {
    "commit_result_to_lighter_send_ms": 10.0,
    "lighter_queue_wait_ms": 5.0,
}


def _finite_number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def load_report(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    if not isinstance(report, dict) or report.get("schema") != SCHEMA:
        raise ValueError(f"{path}: expected schema {SCHEMA}")
    if not isinstance(report.get("metrics_ms"), dict):
        raise ValueError(f"{path}: metrics_ms is missing")
    return report


def select_runtime_build(
    report: dict[str, Any], runtime_build: str | None
) -> dict[str, Any]:
    if runtime_build is None:
        return report
    by_runtime_build = report.get("by_runtime_build")
    if not isinstance(by_runtime_build, dict):
        raise ValueError("by_runtime_build is missing")
    selected = by_runtime_build.get(runtime_build)
    if not isinstance(selected, dict) or not isinstance(
        selected.get("metrics_ms"), dict
    ):
        raise ValueError(f"runtime build not found: {runtime_build}")
    return {**report, "metrics_ms": selected["metrics_ms"]}


def compare_reports(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    relative_tolerance: float = 0.05,
    absolute_grace_ms: float = 10.0,
    min_samples: int = 5,
) -> dict[str, Any]:
    if relative_tolerance < 0 or absolute_grace_ms < 0 or min_samples < 1:
        raise ValueError("comparison tolerances must be non-negative")
    baseline_metrics = baseline["metrics_ms"]
    candidate_metrics = candidate["metrics_ms"]
    comparisons: dict[str, dict[str, Any]] = {}
    regressions: list[str] = []
    warnings: list[str] = []

    for metric in GATED_METRICS:
        base = baseline_metrics.get(metric)
        current = candidate_metrics.get(metric)
        if not isinstance(base, dict) or not isinstance(current, dict):
            warnings.append(f"{metric}: missing from baseline or candidate")
            continue
        base_count = int(base.get("count") or 0)
        current_count = int(current.get("count") or 0)
        base_p95 = _finite_number(base.get("p95"))
        current_p95 = _finite_number(current.get("p95"))
        if base_p95 is None or current_p95 is None:
            warnings.append(f"{metric}: invalid p95")
            continue
        if base_count < min_samples or current_count < min_samples:
            warnings.append(
                f"{metric}: insufficient samples baseline={base_count} candidate={current_count}"
            )
            continue
        allowed_p95 = base_p95 + max(
            absolute_grace_ms,
            base_p95 * relative_tolerance,
        )
        absolute_cap = ABSOLUTE_P95_CAPS_MS.get(metric)
        if absolute_cap is not None:
            allowed_p95 = min(allowed_p95, absolute_cap)
        passed = current_p95 <= allowed_p95
        comparisons[metric] = {
            "baseline_count": base_count,
            "candidate_count": current_count,
            "baseline_p95_ms": base_p95,
            "candidate_p95_ms": current_p95,
            "allowed_p95_ms": round(allowed_p95, 3),
            "absolute_cap_ms": absolute_cap,
            "delta_ms": round(current_p95 - base_p95, 3),
            "passed": passed,
        }
        if not passed:
            regressions.append(metric)

    conclusive = not warnings
    return {
        "schema": "variational-performance-comparison-v1",
        "passed": not regressions and conclusive,
        "conclusive": conclusive,
        "regressions": regressions,
        "warnings": warnings,
        "relative_tolerance": relative_tolerance,
        "absolute_grace_ms": absolute_grace_ms,
        "min_samples": min_samples,
        "comparisons": comparisons,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--baseline-build", default=None)
    parser.add_argument("--candidate-build", default=None)
    parser.add_argument("--relative-tolerance", type=float, default=0.05)
    parser.add_argument("--absolute-grace-ms", type=float, default=10.0)
    parser.add_argument("--min-samples", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        baseline = select_runtime_build(
            load_report(args.baseline), args.baseline_build
        )
        candidate = select_runtime_build(
            load_report(args.candidate), args.candidate_build
        )
        report = compare_reports(
            baseline,
            candidate,
            relative_tolerance=args.relative_tolerance,
            absolute_grace_ms=args.absolute_grace_ms,
            min_samples=args.min_samples,
        )
        report["baseline_build"] = args.baseline_build
        report["candidate_build"] = args.candidate_build
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
