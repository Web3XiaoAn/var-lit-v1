#!/usr/bin/env python3
"""Sample CPU and resident memory for process trees on macOS or Linux.

This observer runs out of process and never imports the trading runtime. Use
the same command locally and on the server to compare Python and Chrome costs.
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ProcessRow:
    pid: int
    ppid: int
    rss_kib: int
    cpu_percent: float
    command: str


def parse_ps_output(raw: str) -> list[ProcessRow]:
    rows: list[ProcessRow] = []
    for line in raw.splitlines():
        fields = line.strip().split(maxsplit=4)
        if len(fields) < 4:
            continue
        try:
            pid = int(fields[0])
            ppid = int(fields[1])
            rss_kib = int(fields[2])
            cpu_percent = float(fields[3])
        except ValueError:
            continue
        if pid <= 0 or ppid < 0 or rss_kib < 0 or not math.isfinite(cpu_percent):
            continue
        rows.append(
            ProcessRow(
                pid=pid,
                ppid=ppid,
                rss_kib=rss_kib,
                cpu_percent=max(0.0, cpu_percent),
                command=fields[4] if len(fields) == 5 else "",
            )
        )
    return rows


def process_tree(rows: Iterable[ProcessRow], root_pid: int) -> list[ProcessRow]:
    materialized = list(rows)
    by_parent: dict[int, list[int]] = defaultdict(list)
    by_pid = {row.pid: row for row in materialized}
    for row in materialized:
        by_parent[row.ppid].append(row.pid)
    selected: set[int] = set()
    pending = [root_pid]
    while pending:
        pid = pending.pop()
        if pid in selected:
            continue
        selected.add(pid)
        pending.extend(by_parent.get(pid, ()))
    return [by_pid[pid] for pid in selected if pid in by_pid]


def process_snapshot() -> list[ProcessRow]:
    completed = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,rss=,%cpu=,command="],
        check=True,
        capture_output=True,
        text=True,
    )
    return parse_ps_output(completed.stdout)


def _nearest_rank(values: list[float], percentile: int) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered) / 100))
    return ordered[rank - 1]


def summarize_samples(samples: list[dict[str, float | int]]) -> dict[str, float | int]:
    if not samples:
        return {"samples": 0}
    cpu = [float(sample["cpu_percent"]) for sample in samples]
    rss = [float(sample["rss_mib"]) for sample in samples]
    processes = [int(sample["processes"]) for sample in samples]
    return {
        "samples": len(samples),
        "processes_max": max(processes),
        "cpu_p50_percent": round(_nearest_rank(cpu, 50), 3),
        "cpu_p95_percent": round(_nearest_rank(cpu, 95), 3),
        "cpu_max_percent": round(max(cpu), 3),
        "rss_p50_mib": round(_nearest_rank(rss, 50), 3),
        "rss_p95_mib": round(_nearest_rank(rss, 95), 3),
        "rss_max_mib": round(max(rss), 3),
    }


def parse_group(value: str) -> tuple[str, int]:
    label, separator, raw_pid = value.partition("=")
    if not separator or not label.strip():
        raise argparse.ArgumentTypeError("group must use LABEL=PID")
    try:
        pid = int(raw_pid)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("group PID must be an integer") from exc
    if pid <= 0:
        raise argparse.ArgumentTypeError("group PID must be positive")
    return label.strip(), pid


def collect(
    groups: list[tuple[str, int]],
    *,
    duration_seconds: float,
    interval_seconds: float,
) -> dict[str, object]:
    samples: dict[str, list[dict[str, float | int]]] = {
        label: [] for label, _pid in groups
    }
    started = time.monotonic()
    deadline = started + duration_seconds
    while True:
        rows = process_snapshot()
        elapsed = time.monotonic() - started
        for label, root_pid in groups:
            tree = process_tree(rows, root_pid)
            samples[label].append(
                {
                    "elapsed_seconds": round(elapsed, 3),
                    "processes": len(tree),
                    "cpu_percent": round(sum(row.cpu_percent for row in tree), 3),
                    "rss_mib": round(sum(row.rss_kib for row in tree) / 1024.0, 3),
                }
            )
        if time.monotonic() >= deadline:
            break
        time.sleep(min(interval_seconds, max(0.0, deadline - time.monotonic())))
    return {
        "schema": "variational-process-resources-v1",
        "duration_seconds": round(time.monotonic() - started, 3),
        "interval_seconds": interval_seconds,
        "groups": {
            label: {
                "root_pid": root_pid,
                "summary": summarize_samples(samples[label]),
                "samples": samples[label],
            }
            for label, root_pid in groups
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--group",
        action="append",
        type=parse_group,
        required=True,
        metavar="LABEL=PID",
    )
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--summary-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration < 0 or args.interval <= 0:
        raise SystemExit("duration must be non-negative and interval must be positive")
    report = collect(
        args.group,
        duration_seconds=args.duration,
        interval_seconds=args.interval,
    )
    if args.summary_only:
        report = {
            **report,
            "groups": {
                label: {
                    "root_pid": group["root_pid"],
                    "summary": group["summary"],
                }
                for label, group in report["groups"].items()
            },
        }
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
