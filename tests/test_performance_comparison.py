from __future__ import annotations

import unittest

from tools.compare_performance_reports import (
    GATED_METRICS,
    compare_reports,
    select_runtime_build,
)


def report(metrics: dict[str, dict[str, float | int]]) -> dict[str, object]:
    return {
        "schema": "variational-runtime-performance-v1",
        "metrics_ms": metrics,
    }


def complete_metrics(
    overrides: dict[str, dict[str, float | int]],
) -> dict[str, dict[str, float | int]]:
    metrics = {metric: {"count": 100, "p95": 100.0} for metric in GATED_METRICS}
    metrics["commit_result_to_lighter_send_ms"] = {"count": 100, "p95": 2.0}
    metrics["lighter_queue_wait_ms"] = {"count": 100, "p95": 1.0}
    metrics.update(overrides)
    return metrics


class PerformanceComparisonTests(unittest.TestCase):
    def test_runtime_build_selection_uses_only_requested_cohort(self) -> None:
        source = report(complete_metrics({}))
        source["by_runtime_build"] = {
            "build-a": {"metrics_ms": {"metric-a": {"count": 5, "p95": 1.0}}}
        }

        selected = select_runtime_build(source, "build-a")

        self.assertEqual(
            selected["metrics_ms"],
            {"metric-a": {"count": 5, "p95": 1.0}},
        )

    def test_small_absolute_change_passes(self) -> None:
        baseline = report(complete_metrics(
            {"variational_quote_total_ms": {"count": 100, "p95": 700.0}}
        ))
        candidate = report(complete_metrics(
            {"variational_quote_total_ms": {"count": 100, "p95": 725.0}}
        ))

        result = compare_reports(baseline, candidate)

        self.assertTrue(result["passed"])
        self.assertEqual(result["regressions"], [])

    def test_material_p95_regression_fails(self) -> None:
        baseline = report(complete_metrics(
            {"variational_commit_total_ms": {"count": 20, "p95": 600.0}}
        ))
        candidate = report(complete_metrics(
            {"variational_commit_total_ms": {"count": 20, "p95": 650.1}}
        ))

        result = compare_reports(baseline, candidate)

        self.assertFalse(result["passed"])
        self.assertEqual(result["regressions"], ["variational_commit_total_ms"])

    def test_hot_path_absolute_cap_cannot_be_relaxed_by_grace(self) -> None:
        baseline = report(complete_metrics(
            {"commit_result_to_lighter_send_ms": {"count": 20, "p95": 2.0}}
        ))
        candidate = report(complete_metrics(
            {"commit_result_to_lighter_send_ms": {"count": 20, "p95": 10.1}}
        ))

        result = compare_reports(baseline, candidate, absolute_grace_ms=50.0)

        self.assertFalse(result["passed"])
        self.assertEqual(
            result["comparisons"]["commit_result_to_lighter_send_ms"]["allowed_p95_ms"],
            10.0,
        )

    def test_insufficient_samples_warn_without_claiming_regression(self) -> None:
        baseline = report(complete_metrics(
            {"lighter_queue_wait_ms": {"count": 3, "p95": 1.0}}
        ))
        candidate = report(complete_metrics(
            {"lighter_queue_wait_ms": {"count": 3, "p95": 100.0}}
        ))

        result = compare_reports(baseline, candidate, min_samples=5)

        self.assertFalse(result["passed"])
        self.assertFalse(result["conclusive"])
        self.assertEqual(result["regressions"], [])
        self.assertTrue(
            any("insufficient samples" in warning for warning in result["warnings"])
        )


if __name__ == "__main__":
    unittest.main()
