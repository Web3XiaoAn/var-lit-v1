from __future__ import annotations

import sys
import unittest
from unittest.mock import patch

from main import parse_args


class CliTests(unittest.TestCase):
    def test_dashboard_is_enabled_by_default(self) -> None:
        with patch.object(sys, "argv", ["main.py"]):
            args = parse_args()

        self.assertTrue(args.dashboard)
        self.assertTrue(args.auto_hedge)

    def test_no_dashboard_does_not_disable_auto_hedge(self) -> None:
        with patch.object(sys, "argv", ["main.py", "--no-dashboard"]):
            args = parse_args()

        self.assertFalse(args.dashboard)
        self.assertTrue(args.auto_hedge)


if __name__ == "__main__":
    unittest.main()
