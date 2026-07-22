from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from tools.sync_research_database import bootstrap
from variational.local_config import resolve_configured_path


class LocalConfigTests(unittest.TestCase):
    def test_environment_selects_external_runtime_config(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-config-") as tmp:
            root = Path(tmp) / "project"
            config = Path(tmp) / "runtime.env"
            root.mkdir()
            config.write_text(
                "RESEARCH_DATABASE_FILE=/var/lib/var-lit-v1/research.sqlite3\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"VARIATIONAL_ENV_FILE": str(config)},
            ):
                path = resolve_configured_path(root, "RESEARCH_DATABASE_FILE")

            self.assertEqual(
                path,
                Path("/var/lib/var-lit-v1/research.sqlite3").resolve(),
            )

    def test_dotenv_relative_path_is_anchored_to_project_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-config-") as tmp:
            root = Path(tmp)
            (root / ".env").write_text(
                "RESEARCH_DATABASE_FILE=../research/strategy.sqlite3\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    resolve_configured_path(root, "RESEARCH_DATABASE_FILE"),
                    (root / "../research/strategy.sqlite3").resolve(),
                )

    def test_explicit_path_does_not_require_dotenv(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-config-") as tmp:
            root = Path(tmp)
            self.assertEqual(
                resolve_configured_path(
                    root,
                    "RESEARCH_DATABASE_FILE",
                    Path("explicit/research.sqlite3"),
                ),
                (root / "explicit/research.sqlite3").resolve(),
            )

    def test_missing_dotenv_or_key_never_falls_back_to_project_local_data(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-config-") as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "configuration is missing"):
                    resolve_configured_path(root, "RESEARCH_DATABASE_FILE")
                (root / ".env").write_text("VARIATIONAL_RUNTIME_DIR=./runtime\n")
                with self.assertRaisesRegex(RuntimeError, "RESEARCH_DATABASE_FILE is missing"):
                    resolve_configured_path(root, "RESEARCH_DATABASE_FILE")

    def test_bootstrap_counts_sources_from_the_configured_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-config-") as tmp:
            root = Path(tmp) / "project"
            runtime = Path(tmp) / "external-runtime"
            root.mkdir()
            runtime.mkdir()
            sources = [(runtime / "one.jsonl", "one", False)]
            synchronizer = MagicMock()
            synchronizer.sync_once.return_value = {
                "inserted": 0,
                "duplicates": 0,
                "malformed": 0,
            }
            with (
                patch(
                    "tools.sync_research_database.default_runtime_sources",
                    return_value=sources,
                ) as source_builder,
                patch(
                    "tools.sync_research_database.ResearchDatabaseSynchronizer",
                    return_value=synchronizer,
                ),
            ):
                report = bootstrap(MagicMock(), root, runtime)
            source_builder.assert_called_once_with(runtime)
            self.assertEqual(report["files"], 1)


if __name__ == "__main__":
    unittest.main()
