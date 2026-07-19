"""Test package bootstrap that keeps runtime artifacts out of the live log directory."""

from __future__ import annotations

import atexit
import os
import tempfile


_runtime_artifacts = tempfile.TemporaryDirectory(prefix="variational-test-runtime-")
os.environ.setdefault("VARIATIONAL_RUNTIME_DIR", _runtime_artifacts.name)
atexit.register(_runtime_artifacts.cleanup)
