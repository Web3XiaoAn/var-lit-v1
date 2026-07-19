#!/usr/bin/env python3
"""Summarize execution latency from bounded runtime trace files.

The analyzer is read-only and deliberately separate from the trading process.
It gives local and server runs one stable comparison format without adding any
work to the live execution path.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from variational.local_config import resolve_configured_path  # noqa: E402


PAIR_METRICS = {
    "variational_quote_total_ms": (
        "variational_quote_dispatch",
        "variational_quote_result",
    ),
    "variational_commit_total_ms": (
        "variational_commit_dispatch",
        "variational_commit_result",
    ),
    "commit_result_to_lighter_prepare_ms": (
        "variational_commit_result",
        "lighter_order_prepared",
    ),
    "commit_result_to_lighter_send_ms": (
        "variational_commit_result",
        "lighter_sign_and_send_start",
    ),
    "commit_result_to_lighter_ack_ms": (
        "variational_commit_result",
        "lighter_order_ack",
    ),
    "commit_result_to_lighter_fill_ms": (
        "variational_commit_result",
        "lighter_fill",
    ),
    "lighter_ack_to_fill_ms": (
        "lighter_order_ack",
        "lighter_fill",
    ),
}

DIRECT_METRICS = {
    "variational_browser_quote_ms": (
        "variational_quote_result",
        "browser_quote_elapsed_ms",
        1.0,
    ),
    "variational_browser_commit_ms": (
        "variational_commit_result",
        "browser_elapsed_ms",
        1.0,
    ),
    "lighter_queue_wait_ms": (
        "lighter_order_entry_receipt",
        "queue_wait_ns",
        1_000_000.0,
    ),
    "lighter_order_round_trip_ms": (
        "lighter_order_entry_receipt",
        "round_trip_ns",
        1_000_000.0,
    ),
}
VARIATIONAL_RESULT_EVENTS = {
    "variational_quote_result",
    "variational_commit_result",
}


def _finite_number(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _nearest_rank(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered) / 100))
    return ordered[rank - 1]


def summarize(values: Iterable[float]) -> dict[str, int | float] | None:
    clean = sorted(value for value in values if math.isfinite(value) and value >= 0)
    if not clean:
        return None
    return {
        "count": len(clean),
        "min": round(clean[0], 3),
        "p50": round(_nearest_rank(clean, 50) or 0.0, 3),
        "p95": round(_nearest_rank(clean, 95) or 0.0, 3),
        "p99": round(_nearest_rank(clean, 99) or 0.0, 3),
        "max": round(clean[-1], 3),
        "mean": round(sum(clean) / len(clean), 3),
    }


def default_trace_paths(runtime_dir: Path) -> list[Path]:
    base = runtime_dir / "execution_trace.jsonl"
    rotated = sorted(
        (
            path
            for path in runtime_dir.glob("execution_trace.jsonl.*")
            if path.name.removeprefix("execution_trace.jsonl.").isdigit()
        ),
        key=lambda path: int(path.name.rsplit(".", 1)[1]),
        reverse=True,
    )
    return [*rotated, *([base] if base.is_file() else [])]


def _paired_duration_ms(
    events: dict[str, list[int]],
    start_event: str,
    end_event: str,
) -> float | None:
    starts = sorted(events.get(start_event, ()))
    ends = sorted(events.get(end_event, ()))
    if not starts or not ends:
        return None
    for start in starts:
        end = next((candidate for candidate in ends if candidate >= start), None)
        if end is not None:
            return (end - start) / 1_000_000.0
    return None


def _compile_metrics(
    trace_events: dict[str, dict[str, list[int]]],
    direct_values: dict[str, list[float]],
) -> dict[str, dict[str, int | float]]:
    metrics: dict[str, dict[str, int | float]] = {}
    for metric, (start_event, end_event) in PAIR_METRICS.items():
        values = [
            duration
            for events in trace_events.values()
            if (duration := _paired_duration_ms(events, start_event, end_event))
            is not None
        ]
        summary = summarize(values)
        if summary is not None:
            metrics[metric] = summary
    for metric, values in direct_values.items():
        summary = summarize(values)
        if summary is not None:
            metrics[metric] = summary
    return dict(sorted(metrics.items()))


def build_report(paths: Iterable[Path]) -> dict[str, Any]:
    selected_paths = [path.expanduser().resolve() for path in paths if path.is_file()]
    trace_events_by_build: dict[str, dict[str, dict[str, list[int]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    direct_values_by_build: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    outcomes_by_build: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"ok": 0, "error": 0})
    )
    seen_rows: set[tuple[str, str, int]] = set()
    builds: set[str] = set()
    first_utc: str | None = None
    last_utc: str | None = None
    rows = 0
    malformed = 0

    direct_by_event: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
    for metric, (event, field, divisor) in DIRECT_METRICS.items():
        direct_by_event[event].append((metric, field, divisor))

    for path in selected_paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(payload, dict):
                    malformed += 1
                    continue
                event = str(payload.get("event") or "").strip()
                monotonic_ns = payload.get("monotonic_ns")
                if not event or isinstance(monotonic_ns, bool) or not isinstance(monotonic_ns, int):
                    malformed += 1
                    continue
                trace_id = str(payload.get("trace_id") or "").strip()
                identity = (trace_id, event, monotonic_ns)
                if identity in seen_rows:
                    continue
                seen_rows.add(identity)
                rows += 1

                utc_time = str(payload.get("utc_time") or "").strip()
                if utc_time:
                    first_utc = utc_time if first_utc is None else min(first_utc, utc_time)
                    last_utc = utc_time if last_utc is None else max(last_utc, utc_time)
                build = str(payload.get("runtime_build") or "unknown").strip() or "unknown"
                builds.add(build)
                successful_result = (
                    event not in VARIATIONAL_RESULT_EVENTS
                    or payload.get("ok") is True
                )
                if event in VARIATIONAL_RESULT_EVENTS:
                    outcome = "ok" if payload.get("ok") is True else "error"
                    for bucket in ("__all__", build):
                        outcomes_by_build[bucket][event][outcome] += 1
                if trace_id and successful_result:
                    for bucket in ("__all__", build):
                        trace_events_by_build[bucket][trace_id][event].append(monotonic_ns)
                if successful_result:
                    for metric, field, divisor in direct_by_event.get(event, ()):
                        value = _finite_number(payload.get(field))
                        if value is not None and value >= 0:
                            for bucket in ("__all__", build):
                                direct_values_by_build[bucket][metric].append(value / divisor)

    metrics = _compile_metrics(
        trace_events_by_build["__all__"],
        direct_values_by_build["__all__"],
    )
    by_runtime_build = {
        build: {
            "trace_count": len(trace_events_by_build[build]),
            "outcomes": dict(sorted(outcomes_by_build[build].items())),
            "metrics_ms": _compile_metrics(
                trace_events_by_build[build],
                direct_values_by_build[build],
            ),
        }
        for build in sorted(builds)
    }

    return {
        "schema": "variational-runtime-performance-v1",
        "files": [str(path) for path in selected_paths],
        "trace_rows": rows,
        "malformed_rows": malformed,
        "trace_count": len(trace_events_by_build["__all__"]),
        "first_utc": first_utc,
        "last_utc": last_utc,
        "runtime_builds": sorted(builds),
        "outcomes": dict(sorted(outcomes_by_build["__all__"].items())),
        "metrics_ms": metrics,
        "by_runtime_build": by_runtime_build,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=None,
        help="Runtime directory; defaults to VARIATIONAL_RUNTIME_DIR in .env.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.paths:
        paths = args.paths
    else:
        runtime_dir = resolve_configured_path(
            PROJECT_ROOT,
            "VARIATIONAL_RUNTIME_DIR",
            args.runtime_dir,
        )
        paths = default_trace_paths(runtime_dir)
    report = build_report(paths)
    if not report["files"]:
        print("no execution trace files found", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    print(f"trace rows: {report['trace_rows']} (malformed={report['malformed_rows']})")
    print(f"period: {report['first_utc']} -> {report['last_utc']}")
    for name, metric in report["metrics_ms"].items():
        print(
            f"{name}: n={metric['count']} p50={metric['p50']:.3f}ms "
            f"p95={metric['p95']:.3f}ms p99={metric['p99']:.3f}ms "
            f"max={metric['max']:.3f}ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
