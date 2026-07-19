"""Bootstrap test artifact isolation for non-package unittest discovery."""

from __future__ import annotations

import atexit
import os
import tempfile


_runtime_artifacts: tempfile.TemporaryDirectory[str] | None = None
if "VARIATIONAL_RUNTIME_DIR" not in os.environ:
    _runtime_artifacts = tempfile.TemporaryDirectory(
        prefix="variational-test-runtime-"
    )
    os.environ["VARIATIONAL_RUNTIME_DIR"] = _runtime_artifacts.name
    atexit.register(_runtime_artifacts.cleanup)
