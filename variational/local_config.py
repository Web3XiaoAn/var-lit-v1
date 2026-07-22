"""Read local artifact paths without importing or mutating the live runtime."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import dotenv_values


def resolve_configured_path(
    project_root: Path,
    env_key: str,
    explicit: Path | None = None,
    *,
    dotenv_path: Path | None = None,
) -> Path:
    """Resolve an explicit path or the authoritative value in ``.env``.

    Research utilities must never silently fall back to a project-local
    database or log directory: doing so can create a second, empty research
    store while the live runtime writes elsewhere.
    """

    root = project_root.expanduser().resolve()
    selected = explicit
    if selected is None:
        source = dotenv_path or Path(
            os.environ.get("VARIATIONAL_ENV_FILE", root / ".env")
        )
        if not source.is_file():
            raise RuntimeError(
                f"Required local configuration is missing: {source}. "
                f"Pass an explicit path or create the project .env file."
            )
        values = dotenv_values(dotenv_path=source, interpolate=False)
        raw = str(values.get(env_key) or "").strip()
        if not raw:
            raise RuntimeError(
                f"{env_key} is missing from {source}; pass an explicit path instead"
            )
        selected = Path(raw)
    path = selected.expanduser()
    return path.resolve() if path.is_absolute() else (root / path).resolve()


__all__ = ["resolve_configured_path"]
