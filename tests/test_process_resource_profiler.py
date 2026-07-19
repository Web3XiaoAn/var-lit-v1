from __future__ import annotations

import unittest

from tools.profile_process_resources import (
    parse_ps_output,
    process_tree,
    summarize_samples,
)


class ProcessResourceProfilerTests(unittest.TestCase):
    def test_parser_and_tree_include_only_root_descendants(self) -> None:
        rows = parse_ps_output(
            """
              10   1 1000 1.5 root command
              11  10 2000 2.5 child command --flag
              12  11 3000 3.5 grandchild
              20   1 9000 9.5 unrelated
            """
        )
        tree = process_tree(rows, 10)
        self.assertEqual({row.pid for row in tree}, {10, 11, 12})
        self.assertEqual(sum(row.rss_kib for row in tree), 6000)
        self.assertEqual(sum(row.cpu_percent for row in tree), 7.5)

    def test_summary_uses_nearest_rank_and_maxima(self) -> None:
        samples = [
            {"cpu_percent": float(index), "rss_mib": float(index * 10), "processes": index}
            for index in range(1, 21)
        ]
        summary = summarize_samples(samples)
        self.assertEqual(summary["cpu_p50_percent"], 10.0)
        self.assertEqual(summary["cpu_p95_percent"], 19.0)
        self.assertEqual(summary["rss_p95_mib"], 190.0)
        self.assertEqual(summary["processes_max"], 20)


if __name__ == "__main__":
    unittest.main()
