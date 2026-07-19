#!/usr/bin/env python3
"""Attach a human review label to one completed research round."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from variational.research_database import ResearchDatabase  # noqa: E402
from variational.local_config import resolve_configured_path  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--open-trade-key", required=True)
    parser.add_argument("--close-trade-key", required=True)
    parser.add_argument(
        "--label",
        choices=("bad_execution", "acceptable_execution"),
        required=True,
    )
    parser.add_argument("--reason", default="manual_review")
    parser.add_argument("--note", required=True)
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="Research database path; defaults to RESEARCH_DATABASE_FILE in .env.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_path = resolve_configured_path(
        PROJECT_ROOT,
        "RESEARCH_DATABASE_FILE",
        args.database,
    )
    database = ResearchDatabase(database_path)
    round_key = database.label_round(
        open_trade_key=args.open_trade_key,
        close_trade_key=args.close_trade_key,
        label=args.label,
        reason_code=args.reason,
        note=args.note,
    )
    print(
        json.dumps(
            {
                "round_key": round_key,
                "label": args.label,
                "reason": args.reason,
                "note": args.note,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
