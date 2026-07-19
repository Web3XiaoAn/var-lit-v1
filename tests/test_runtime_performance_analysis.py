from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.analyze_runtime_performance import build_report, default_trace_paths


class RuntimePerformanceAnalysisTests(unittest.TestCase):
    def test_pairs_trace_events_and_reports_nearest_rank_percentiles(self) -> None:
        with tempfile.TemporaryDirectory(prefix="runtime-performance-") as tmp:
            path = Path(tmp) / "execution_trace.jsonl"
            rows = []
            for index in range(1, 21):
                trace_id = f"trace-{index}"
                start_ns = index * 1_000_000_000
                rows.extend(
                    [
                        {
                            "event": "variational_quote_dispatch",
                            "trace_id": trace_id,
                            "monotonic_ns": start_ns,
                            "utc_time": f"2026-07-19T00:00:{index:02d}+00:00",
                            "runtime_build": "test-build",
                        },
                        {
                            "event": "variational_quote_result",
                            "trace_id": trace_id,
                            "monotonic_ns": start_ns + index * 10_000_000,
                            "browser_quote_elapsed_ms": index * 9,
                            "ok": True,
                            "runtime_build": "test-build",
                        },
                    ]
                )
            rows.append(
                {
                    "event": "lighter_order_entry_receipt",
                    "trace_id": "lighter",
                    "monotonic_ns": 99_000_000_000,
                    "queue_wait_ns": 750_000,
                    "round_trip_ns": 500_000_000,
                    "runtime_build": "test-build",
                }
            )
            path.write_text(
                "\n".join(json.dumps(row) for row in rows) + "\n{bad-json}\n",
                encoding="utf-8",
            )

            report = build_report([path])

            quote = report["metrics_ms"]["variational_quote_total_ms"]
            self.assertEqual(quote["count"], 20)
            self.assertEqual(quote["p50"], 100.0)
            self.assertEqual(quote["p95"], 190.0)
            self.assertEqual(quote["p99"], 200.0)
            self.assertEqual(
                report["metrics_ms"]["lighter_queue_wait_ms"]["p95"],
                0.75,
            )
            self.assertEqual(report["malformed_rows"], 1)
            self.assertEqual(report["runtime_builds"], ["test-build"])
            self.assertEqual(
                report["by_runtime_build"]["test-build"]["metrics_ms"]
                ["variational_quote_total_ms"]["p95"],
                190.0,
            )

    def test_default_paths_are_oldest_rotation_then_current(self) -> None:
        with tempfile.TemporaryDirectory(prefix="runtime-performance-") as tmp:
            root = Path(tmp)
            for name in (
                "execution_trace.jsonl",
                "execution_trace.jsonl.1",
                "execution_trace.jsonl.2",
                "execution_trace.jsonl.note",
            ):
                (root / name).touch()
            self.assertEqual(
                [path.name for path in default_trace_paths(root)],
                [
                    "execution_trace.jsonl.2",
                    "execution_trace.jsonl.1",
                    "execution_trace.jsonl",
                ],
            )


if __name__ == "__main__":
    unittest.main()
