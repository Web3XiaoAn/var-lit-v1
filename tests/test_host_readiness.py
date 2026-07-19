from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.check_host_readiness import GIB, build_report, parse_dotenv


class HostReadinessTests(unittest.TestCase):
    def test_dotenv_parser_rejects_duplicate_without_exposing_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / ".env"
            path.write_text(
                "LIGHTER_PRIVATE_KEY=very-secret\nLIGHTER_PRIVATE_KEY=again\n",
                encoding="utf-8",
            )

            values, errors = parse_dotenv(path)

        self.assertEqual(values["LIGHTER_PRIVATE_KEY"], "very-secret")
        self.assertEqual(len(errors), 1)
        self.assertNotIn("very-secret", json.dumps(errors))

    def test_synthetic_server_requires_amd64_memory_swap_and_external_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project = Path(temp_dir)
            for relative in (
                "main.py",
                "requirements.txt",
                "chrome_extension/manifest.json",
                "chrome_extension/background.js",
                "deploy/launch_chrome.sh",
                "deploy/run_runtime.sh",
            ):
                target = project / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("test", encoding="utf-8")
            config = project / ".env"
            config.write_text(
                "LIGHTER_PRIVATE_KEY=secret\n"
                "LIGHTER_API_KEY_INDEX=1\n"
                "LIGHTER_ACCOUNT_INDEX=2\n"
                "VARIATIONAL_RUNTIME_DIR=/var/lib/variational/runtime\n"
                "RESEARCH_DATABASE_FILE=/var/lib/variational/research.sqlite3\n"
                "RESEARCH_DATABASE_ENABLED=false\n",
                encoding="utf-8",
            )
            config.chmod(0o600)

            result = build_report(
                phase="server",
                project_dir=project,
                config_path=config,
                chrome_profile=None,
                system="Linux",
                machine="x86_64",
                cpu_count=2,
                memory_bytes=4 * GIB,
                swap_bytes=2 * GIB,
                disk_free_bytes=20 * GIB,
            )

        # The synthetic host has no real Linux Chrome/Xvfb binaries, so those
        # two checks fail while the resource/config checks remain valid.
        self.assertIn("chrome", result["failed"])
        self.assertIn("xvfb", result["failed"])
        for name in (
            "server_architecture",
            "memory",
            "swap",
            "external_runtime_path",
            "external_research_path",
            "server_research_database",
        ):
            status = next(
                item["status"] for item in result["checks"] if item["name"] == name
            )
            self.assertEqual(status, "pass")


if __name__ == "__main__":
    unittest.main()
