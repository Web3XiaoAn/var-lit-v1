#!/usr/bin/env python3
"""Bootstrap, inspect, or continuously synchronize the strategy research DB."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from variational.research_database import (  # noqa: E402
    DEFAULT_MAX_DATABASE_BYTES,
    DEFAULT_SYNC_INTERVAL_SECONDS,
    ResearchDatabase,
    ResearchDatabaseSynchronizer,
    default_runtime_sources,
)
from variational.local_config import resolve_configured_path  # noqa: E402


JSONL_STREAM_NAMES = {
    "strategy-market-samples.jsonl": "strategy_market_sample",
    "adaptive-median-v1-calibration.jsonl": "strategy_market_sample",
    "order-metrics.jsonl": "order_metric",
    "pre-unified-order-metrics.jsonl": "order_metric",
    "execution-trace.jsonl": "execution_trace",
    "pre-unified-execution-trace.jsonl": "execution_trace",
    "execution-trace-pre-v4-live1.jsonl.gz": "execution_trace",
    "executions.jsonl": "normalized_execution",
    "rounds.jsonl": "normalized_round",
    "sessions.jsonl": "runtime_session",
}


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _bootstrap_sources(project_root: Path) -> Iterable[tuple[Path, str, bool]]:
    historical = project_root / "research_data" / "historical"
    snapshots = project_root / "research_data" / "snapshots"
    for path in sorted(historical.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        stream = JSONL_STREAM_NAMES.get(path.name)
        if stream is not None:
            yield path, stream, True
            continue
        if path.name == "runtime.log" or path.name.endswith("-runtime.log"):
            yield path, "runtime_log", True
            continue
        if path.suffix == ".json":
            yield path, f"historical_{path.stem.replace('-', '_')}", True
    snapshot_dirs = sorted(path for path in snapshots.glob("*") if path.is_dir())
    latest_snapshot = snapshot_dirs[-1] if snapshot_dirs else None
    for snapshot in snapshot_dirs:
        if not snapshot.is_dir():
            continue
        for path in sorted(snapshot.rglob("*")):
            if not path.is_file() or path.name == "manifest.json":
                continue
            if path.parent.name == "derived" and snapshot != latest_snapshot:
                # Derived rows contain snapshot-specific session identifiers,
                # so importing every preliminary snapshot would duplicate the
                # same completed rounds. Raw source rows remain hash-deduped.
                continue
            stream = JSONL_STREAM_NAMES.get(path.name)
            if stream is not None:
                yield path, stream, False
                continue
            if path.name == "runtime.log":
                yield path, "runtime_log", False
                continue
            if path.suffix == ".json":
                yield path, f"snapshot_{path.stem.replace('-', '_')}", False


def bootstrap(
    database: ResearchDatabase,
    project_root: Path,
    runtime_dir: Path,
) -> dict[str, int]:
    result = {"inserted": 0, "duplicates": 0, "malformed": 0, "files": 0}
    for path, stream, pinned in _bootstrap_sources(project_root):
        if path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz"):
            added, repeated, malformed = database.ingest_jsonl(
                path,
                stream=stream,
                pinned=pinned,
            )
            result["malformed"] += malformed
        elif stream == "runtime_log":
            added, repeated = database.ingest_runtime_log(path, pinned=pinned)
        else:
            added, repeated = database.ingest_json_document(
                path,
                stream=stream,
                pinned=pinned,
            )
        result["inserted"] += added
        result["duplicates"] += repeated
        result["files"] += 1
    runtime_sources = default_runtime_sources(runtime_dir)
    runtime_sync = ResearchDatabaseSynchronizer(database, runtime_sources)
    synced = runtime_sync.sync_once()
    for key in ("inserted", "duplicates", "malformed"):
        result[key] += synced[key]
    result["files"] += len(runtime_sources)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="Research database path; defaults to RESEARCH_DATABASE_FILE in .env.",
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=None,
        help="Runtime artifact directory; defaults to VARIATIONAL_RUNTIME_DIR in .env.",
    )
    parser.add_argument(
        "--max-mib",
        type=int,
        default=DEFAULT_MAX_DATABASE_BYTES // (1024 * 1024),
    )
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--follow-pid", type=int)
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_SYNC_INTERVAL_SECONDS,
    )
    parser.add_argument("--stats", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = args.project_root.expanduser().resolve()
    database_path = resolve_configured_path(
        project_root,
        "RESEARCH_DATABASE_FILE",
        args.database,
    )
    runtime_dir = resolve_configured_path(
        project_root,
        "VARIATIONAL_RUNTIME_DIR",
        args.runtime_dir,
    )
    database = ResearchDatabase(
        database_path,
        max_bytes=args.max_mib * 1024 * 1024,
    )
    output: dict[str, object] = {}
    if args.bootstrap:
        output["bootstrap"] = bootstrap(database, project_root, runtime_dir)
    synchronizer = ResearchDatabaseSynchronizer(
        database,
        default_runtime_sources(runtime_dir),
    )
    if args.once:
        output["sync"] = synchronizer.sync_once()
    if args.follow_pid is not None:
        stop = False

        def handle_stop(_signum: int, _frame: object) -> None:
            nonlocal stop
            stop = True

        signal.signal(signal.SIGTERM, handle_stop)
        signal.signal(signal.SIGINT, handle_stop)
        while not stop and _alive(args.follow_pid):
            synchronizer.sync_once()
            time.sleep(max(0.1, args.interval))
        synchronizer.sync_once()
        output["follow"] = {
            "pid": args.follow_pid,
            "inserted": synchronizer.inserted_events,
            "duplicates": synchronizer.duplicate_events,
            "malformed": synchronizer.malformed_lines,
            "failures": synchronizer.sync_failures,
        }
    if args.stats or not output:
        output["stats"] = database.stats()
    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
