from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from dotenv import dotenv_values

from main import RUNTIME_DOTENV_ALLOWED_KEYS


PROJECT_DIR = Path(__file__).resolve().parents[1]


class DeploymentAssetTests(unittest.TestCase):
    def test_public_release_identity_is_v1(self) -> None:
        manifest = json.loads(
            (PROJECT_DIR / "chrome_extension" / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
        listener = (PROJECT_DIR / "variational" / "listener.py").read_text(
            encoding="utf-8"
        )
        background = (PROJECT_DIR / "chrome_extension" / "background.js").read_text(
            encoding="utf-8"
        )

        self.assertEqual(manifest["name"], "Var-Lit V1 Bridge")
        self.assertEqual(manifest["version"], "1.0.0")
        self.assertIn('COMMAND_EXTENSION_BUILD = "var-lit-v1"', listener)
        self.assertIn('const FORWARDER_BUILD = "var-lit-v1"', background)
        self.assertNotIn("2026-07-19-", listener)
        self.assertNotIn("2026-07-19-", background)

    def test_server_env_is_complete_safe_and_external(self) -> None:
        values = dotenv_values(PROJECT_DIR / "deploy" / "server.env.example")

        self.assertEqual(set(values), set(RUNTIME_DOTENV_ALLOWED_KEYS))
        self.assertEqual(values["STRATEGY_EXECUTION_MODE"], "observe")
        self.assertEqual(values["STRATEGY_ORDER_NOTIONAL_USD"], "500")
        self.assertEqual(values["RESEARCH_DATABASE_ENABLED"], "false")
        self.assertTrue(str(values["VARIATIONAL_RUNTIME_DIR"]).startswith("/var/lib/"))
        self.assertTrue(str(values["RESEARCH_DATABASE_FILE"]).startswith("/var/lib/"))
        self.assertIn("请填写", str(values["LIGHTER_PRIVATE_KEY"]))

    def test_chrome_launcher_preserves_latency_and_sandbox(self) -> None:
        source = (PROJECT_DIR / "deploy" / "launch_chrome.sh").read_text(
            encoding="utf-8"
        )

        for required in (
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--remote-debugging-address=127.0.0.1",
            "--window-size=1920,1080",
        ):
            self.assertIn(required, source)
        for forbidden in (
            "--no-sandbox",
            "--single-process",
            "--disable-dev-shm-usage",
        ):
            self.assertNotIn(forbidden, source)

    def test_chrome_launcher_executes_with_a_fresh_profile_on_macos(self) -> None:
        if os.uname().sysname != "Darwin":
            self.skipTest("macOS launcher integration test")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_chrome = temp_path / "fake-chrome"
            fake_chrome.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\"\n",
                encoding="utf-8",
            )
            fake_chrome.chmod(0o700)
            profile = temp_path / "profile"
            environment = os.environ.copy()
            environment.update(
                {
                    "VARIATIONAL_CHROME_BIN": str(fake_chrome),
                    "VARIATIONAL_CHROME_PROFILE_DIR": str(profile),
                    "VARIATIONAL_CHROME_DEBUG_PORT": "19222",
                }
            )

            result = subprocess.run(
                [str(PROJECT_DIR / "deploy" / "launch_chrome.sh")],
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )

            self.assertTrue(profile.is_dir())
            self.assertEqual(profile.stat().st_mode & 0o777, 0o700)
            self.assertIn(f"--user-data-dir={profile}", result.stdout)
            self.assertIn("--remote-debugging-port=19222", result.stdout)
            self.assertIn("--load-extension=", result.stdout)

    def test_official_chrome_requires_persistent_manual_extension_install(self) -> None:
        if os.uname().sysname != "Darwin":
            self.skipTest("macOS launcher integration test")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            fake_chrome = temp_path / "google-chrome-stable"
            fake_chrome.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\"\n",
                encoding="utf-8",
            )
            fake_chrome.chmod(0o700)
            environment = os.environ.copy()
            environment.update(
                {
                    "VARIATIONAL_CHROME_BIN": str(fake_chrome),
                    "VARIATIONAL_CHROME_PROFILE_DIR": str(temp_path / "profile"),
                }
            )

            result = subprocess.run(
                [str(PROJECT_DIR / "deploy" / "launch_chrome.sh")],
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )

            self.assertNotIn("--load-extension=", result.stdout)
            self.assertIn("one-time manual Load unpacked", result.stderr)

    def test_server_runtime_disables_only_dashboard(self) -> None:
        source = (PROJECT_DIR / "deploy" / "run_runtime.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("--no-dashboard", source)
        self.assertNotIn("--no-hedge", source)

    def test_runtime_restarts_after_chrome_without_restarting_chrome(self) -> None:
        display = (
            PROJECT_DIR
            / "deploy"
            / "systemd"
            / "var-lit-v1-display.service.example"
        ).read_text(encoding="utf-8")
        window_manager = (
            PROJECT_DIR
            / "deploy"
            / "systemd"
            / "var-lit-v1-window-manager.service.example"
        ).read_text(encoding="utf-8")
        chrome = (
            PROJECT_DIR
            / "deploy"
            / "systemd"
            / "var-lit-v1-chrome.service.example"
        ).read_text(encoding="utf-8")
        runtime = (
            PROJECT_DIR
            / "deploy"
            / "systemd"
            / "var-lit-v1-runtime.service.example"
        ).read_text(encoding="utf-8")

        self.assertIn("1920x1080x24", display)
        self.assertIn("User=varlit", window_manager)
        self.assertIn("ExecStart=/usr/bin/openbox --replace", window_manager)
        self.assertIn("Requires=var-lit-v1-display.service", window_manager)
        self.assertIn(
            "Requires=var-lit-v1-display.service var-lit-v1-window-manager.service",
            chrome,
        )
        self.assertIn("Requires=var-lit-v1-chrome.service", runtime)
        self.assertNotIn("PartOf=var-lit-v1-runtime.service", chrome)

    def test_ci_keeps_both_supported_python_versions(self) -> None:
        workflow = (PROJECT_DIR / ".github" / "workflows" / "test.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn('python-version: ["3.11", "3.12"]', workflow)
        self.assertIn("actions/checkout@v7", workflow)
        self.assertIn("actions/setup-python@v6", workflow)
        self.assertIn("actions/setup-node@v7", workflow)
        self.assertIn("python -m unittest discover", workflow)
        self.assertIn("node --test tests/test_extension_templates.js", workflow)


if __name__ == "__main__":
    unittest.main()
