from __future__ import annotations

import unittest
import os
from argparse import Namespace
from pathlib import Path

os.environ.setdefault("LIGHTER_ACCOUNT_INDEX", "1")
os.environ.setdefault("LIGHTER_API_KEY_INDEX", "1")
os.environ.setdefault("LIGHTER_PRIVATE_KEY", "0x00")

import main


class RuntimeArtifactIsolationTests(unittest.TestCase):
    def test_unit_runtime_never_uses_live_log_directory(self) -> None:
        live_log_dir = (Path(__file__).resolve().parents[1] / "log").resolve()
        self.assertNotEqual(main.LOG_DIR.resolve(), live_log_dir)

        runtime = main.VariationalToLighterRuntime(
            Namespace(auto_hedge=True, lang="zh")
        )
        try:
            self.assertEqual(runtime.orders_file.parent, main.LOG_DIR.resolve())
            self.assertEqual(main.APP_LOG_FILE.parent.resolve(), main.LOG_DIR.resolve())
        finally:
            for handler in runtime.logger.handlers:
                handler.close()


if __name__ == "__main__":
    unittest.main()
