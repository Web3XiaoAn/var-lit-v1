#!/usr/bin/env python3
"""Read-only local/server host readiness audit for Var-Lit V1 deployment."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GIB = 1024**3
SUPPORTED_PYTHON = {(3, 11), (3, 12)}
REQUIRED_PROJECT_FILES = (
    "main.py",
    "requirements.txt",
    "chrome_extension/manifest.json",
    "chrome_extension/background.js",
    "deploy/launch_chrome.sh",
    "deploy/run_runtime.sh",
)
SECRET_KEYS = {
    "LIGHTER_PRIVATE_KEY",
    "LIGHTER_API_KEY_INDEX",
    "LIGHTER_ACCOUNT_INDEX",
}


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str


def check(name: str, passed: bool, detail: str, *, warning: bool = False) -> Check:
    if passed:
        status = "pass"
    elif warning:
        status = "warning"
    else:
        status = "fail"
    return Check(name=name, status=status, detail=detail)


def _read_linux_meminfo(path: Path = Path("/proc/meminfo")) -> tuple[int, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            amount = int(raw.strip().split()[0])
            values[key] = amount * 1024
    except (OSError, ValueError, IndexError):
        return 0, 0
    return values.get("MemTotal", 0), values.get("SwapTotal", 0)


def _darwin_sysctl_int(name: str) -> int:
    try:
        result = subprocess.run(
            ["/usr/sbin/sysctl", "-n", name],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return int(result.stdout.strip())
    except (OSError, ValueError, subprocess.SubprocessError):
        return 0


def memory_totals(system: str) -> tuple[int, int]:
    if system == "Linux":
        return _read_linux_meminfo()
    if system == "Darwin":
        return _darwin_sysctl_int("hw.memsize"), 0
    return 0, 0


def parse_dotenv(path: Path) -> tuple[dict[str, str], list[str]]:
    values: dict[str, str] = {}
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return {}, [str(exc)]
    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            errors.append(f"line {line_number}: missing '='")
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in values:
            errors.append(f"line {line_number}: empty or duplicate key")
            continue
        values[key] = value.strip()
    return values, errors


def _path_is_outside_project(raw_path: str, project_dir: Path) -> bool:
    if not raw_path:
        return False
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = project_dir / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(project_dir.resolve())
    except ValueError:
        return True
    return False


def _find_chrome(system: str) -> str | None:
    candidates: Iterable[str]
    if system == "Darwin":
        candidates = (
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        )
    else:
        candidates = (
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
        )
    return next((candidate for candidate in candidates if Path(candidate).is_file()), None)


def build_report(
    *,
    phase: str,
    project_dir: Path,
    config_path: Path,
    chrome_profile: Path | None,
    system: str | None = None,
    machine: str | None = None,
    cpu_count: int | None = None,
    memory_bytes: int | None = None,
    swap_bytes: int | None = None,
    disk_free_bytes: int | None = None,
) -> dict[str, object]:
    system = system or platform.system()
    machine = machine or platform.machine()
    cpu_count = cpu_count if cpu_count is not None else (os.cpu_count() or 0)
    measured_memory, measured_swap = memory_totals(system)
    memory_bytes = measured_memory if memory_bytes is None else memory_bytes
    swap_bytes = measured_swap if swap_bytes is None else swap_bytes
    if disk_free_bytes is None:
        try:
            disk_free_bytes = shutil.disk_usage(project_dir).free
        except OSError:
            disk_free_bytes = 0

    checks: list[Check] = []
    checks.append(
        check(
            "operating_system",
            system in {"Darwin", "Linux"} and (phase != "server" or system == "Linux"),
            f"{system} {machine}",
        )
    )
    if phase == "server":
        checks.append(
            check(
                "server_architecture",
                machine in {"x86_64", "amd64"},
                f"machine={machine}; Google Chrome Linux deployment requires amd64",
            )
        )
    checks.append(
        check(
            "python_version",
            sys.version_info[:2] in SUPPORTED_PYTHON,
            platform.python_version(),
        )
    )
    checks.append(check("cpu", cpu_count >= 2, f"logical_cpus={cpu_count}"))
    memory_gib = memory_bytes / GIB if memory_bytes else 0.0
    checks.append(
        check(
            "memory",
            phase != "server" or memory_gib >= 3.5,
            (
                f"total_gib={memory_gib:.2f}; server minimum is nominal 4 GiB"
                if phase == "server"
                else f"total_gib={memory_gib:.2f}"
            ),
        )
    )
    swap_gib = swap_bytes / GIB if swap_bytes else 0.0
    if phase == "server":
        checks.append(
            check(
                "swap",
                memory_gib >= 7.0 or swap_gib >= 1.5,
                f"swap_gib={swap_gib:.2f}; a 4 GiB server requires at least 2 GiB swap",
            )
        )
    free_gib = disk_free_bytes / GIB if disk_free_bytes else 0.0
    checks.append(
        check(
            "disk_free",
            free_gib >= 10.0,
            f"free_gib={free_gib:.2f}; at least 10 GiB must remain free",
        )
    )

    missing_files = [
        relative
        for relative in REQUIRED_PROJECT_FILES
        if not (project_dir / relative).is_file()
    ]
    checks.append(
        check(
            "project_files",
            not missing_files,
            "all required files present" if not missing_files else f"missing={missing_files}",
        )
    )
    chrome_path = _find_chrome(system)
    checks.append(
        check(
            "chrome",
            chrome_path is not None,
            chrome_path or "Google Chrome/Chromium executable not found",
        )
    )
    if phase == "server":
        xvfb = shutil.which("Xvfb")
        checks.append(check("xvfb", xvfb is not None, xvfb or "Xvfb not found"))

    env_values, env_errors = parse_dotenv(config_path)
    config_exists = config_path.is_file()
    checks.append(
        check(
            "dotenv_parse",
            config_exists and not env_errors,
            "valid" if config_exists and not env_errors else f"errors={env_errors}",
        )
    )
    if config_exists:
        mode = config_path.stat().st_mode & 0o777
        checks.append(
            check(
                "dotenv_permissions",
                phase != "server" or mode == 0o600,
                f"mode={mode:03o}; server secret file must be 600",
                warning=phase != "server",
            )
        )
        placeholders = [
            key
            for key in SECRET_KEYS
            if not env_values.get(key) or "请填写" in env_values.get(key, "")
        ]
        checks.append(
            check(
                "credentials_present",
                not placeholders,
                "all credential fields populated"
                if not placeholders
                else f"placeholder_keys={sorted(placeholders)}",
            )
        )
        runtime_path = env_values.get("VARIATIONAL_RUNTIME_DIR", "")
        research_path = env_values.get("RESEARCH_DATABASE_FILE", "")
        checks.append(
            check(
                "external_runtime_path",
                _path_is_outside_project(runtime_path, project_dir),
                "runtime path is outside project"
                if _path_is_outside_project(runtime_path, project_dir)
                else "runtime path must be outside project",
            )
        )
        checks.append(
            check(
                "external_research_path",
                _path_is_outside_project(research_path, project_dir),
                "research path is outside project"
                if _path_is_outside_project(research_path, project_dir)
                else "research path must be outside project",
            )
        )
        if phase == "server":
            checks.append(
                check(
                    "server_research_database",
                    env_values.get("RESEARCH_DATABASE_ENABLED", "").lower() == "false",
                    "disabled" if env_values.get("RESEARCH_DATABASE_ENABLED", "").lower() == "false" else "must be false",
                )
            )

    if chrome_profile is not None:
        profile_exists = chrome_profile.is_dir()
        checks.append(
            check(
                "chrome_profile",
                profile_exists,
                str(chrome_profile),
            )
        )

    failed = [item.name for item in checks if item.status == "fail"]
    warnings = [item.name for item in checks if item.status == "warning"]
    return {
        "schema": "variational-host-readiness-v1",
        "phase": phase,
        "ready": not failed,
        "failed": failed,
        "warnings": warnings,
        "checks": [asdict(item) for item in checks],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("local", "server"), default="local")
    parser.add_argument("--project-dir", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--config", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--chrome-profile", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        phase=args.phase,
        project_dir=args.project_dir.expanduser().resolve(),
        config_path=args.config.expanduser().resolve(),
        chrome_profile=(
            args.chrome_profile.expanduser().resolve()
            if args.chrome_profile is not None
            else None
        ),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
