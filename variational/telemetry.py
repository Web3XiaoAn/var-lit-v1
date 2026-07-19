from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any


class AsyncJsonlWriter:
    """Best-effort JSONL writer that never waits in the caller's hot path."""

    def __init__(
        self,
        path: Path,
        *,
        max_queue_size: int = 2048,
        rolling_timestamp_field: str | None = None,
        rolling_keep_ms: int | None = None,
        rolling_compaction_interval_ms: int | None = None,
        max_file_bytes: int | None = None,
        backup_count: int = 0,
    ) -> None:
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        rolling_values = (
            rolling_timestamp_field,
            rolling_keep_ms,
            rolling_compaction_interval_ms,
        )
        if any(value is not None for value in rolling_values) and not all(
            value is not None for value in rolling_values
        ):
            raise ValueError("rolling JSONL settings must be provided together")
        if rolling_timestamp_field is not None and not rolling_timestamp_field:
            raise ValueError("rolling_timestamp_field must not be empty")
        if rolling_keep_ms is not None and rolling_keep_ms <= 0:
            raise ValueError("rolling_keep_ms must be positive")
        if rolling_compaction_interval_ms is not None and rolling_compaction_interval_ms <= 0:
            raise ValueError("rolling_compaction_interval_ms must be positive")
        if max_file_bytes is not None and max_file_bytes <= 0:
            raise ValueError("max_file_bytes must be positive")
        if backup_count < 0:
            raise ValueError("backup_count must not be negative")
        if max_file_bytes is not None and backup_count == 0:
            raise ValueError("backup_count must be positive when rotation is enabled")
        if max_file_bytes is None and backup_count != 0:
            raise ValueError("max_file_bytes is required when backup_count is set")
        self.path = path
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max_queue_size)
        self._worker: asyncio.Task[None] | None = None
        self._closing = False
        self._rolling_timestamp_field = rolling_timestamp_field
        self._rolling_keep_ms = rolling_keep_ms
        self._rolling_compaction_interval_ms = rolling_compaction_interval_ms
        self._next_compaction_timestamp_ms: int | None = None
        self._max_file_bytes = max_file_bytes
        self._backup_count = backup_count
        self.dropped_events = 0
        self.write_failures = 0
        self.compactions = 0

    def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain(), name=f"jsonl-writer:{self.path.name}")

    def emit(self, row: dict[str, Any]) -> bool:
        """Queue an event without awaiting disk I/O.

        Returning False means the bounded queue was full or the writer is closing.
        Telemetry loss is intentionally isolated from trading behavior.
        """
        if self._closing:
            return False
        try:
            self._queue.put_nowait(dict(row))
            return True
        except asyncio.QueueFull:
            self.dropped_events += 1
            return False

    async def close(self) -> None:
        self._closing = True
        worker = self._worker
        if worker is None:
            return
        try:
            await asyncio.wait_for(self._queue.join(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    async def _drain(self) -> None:
        while True:
            row = await self._queue.get()
            try:
                line = json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
                await asyncio.to_thread(self._append_line, self.path, line)
                await self._compact_if_due(row)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.write_failures += 1
            finally:
                self._queue.task_done()

    def _append_line(self, path: Path, line: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded_size = len(line.encode("utf-8"))
        if (
            self._max_file_bytes is not None
            and path.is_file()
            and path.stat().st_size + encoded_size > self._max_file_bytes
        ):
            self._rotate_files(path)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    def _rotate_files(self, path: Path) -> None:
        oldest = path.with_name(f"{path.name}.{self._backup_count}")
        try:
            oldest.unlink()
        except FileNotFoundError:
            pass
        for index in range(self._backup_count - 1, 0, -1):
            source = path.with_name(f"{path.name}.{index}")
            if source.exists():
                os.replace(source, path.with_name(f"{path.name}.{index + 1}"))
        if path.exists():
            os.replace(path, path.with_name(f"{path.name}.1"))

    async def _compact_if_due(self, row: dict[str, Any]) -> None:
        timestamp_field = self._rolling_timestamp_field
        keep_ms = self._rolling_keep_ms
        interval_ms = self._rolling_compaction_interval_ms
        if timestamp_field is None or keep_ms is None or interval_ms is None:
            return
        timestamp_ms = row.get(timestamp_field)
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
            return
        next_compaction_ms = self._next_compaction_timestamp_ms
        if next_compaction_ms is not None and timestamp_ms < next_compaction_ms:
            return
        await asyncio.to_thread(
            self.compact_jsonl_window,
            self.path,
            timestamp_field=timestamp_field,
            cutoff_ms=timestamp_ms - keep_ms,
        )
        self.compactions += 1
        self._next_compaction_timestamp_ms = timestamp_ms + interval_ms

    @staticmethod
    def compact_jsonl_window(
        path: Path,
        *,
        timestamp_field: str,
        cutoff_ms: int,
    ) -> int:
        """Atomically retain valid JSON rows inside the requested time window."""

        if not path.is_file():
            return 0
        retained: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(row, dict):
                        continue
                    timestamp_ms = row.get(timestamp_field)
                    if (
                        isinstance(timestamp_ms, bool)
                        or not isinstance(timestamp_ms, int)
                        or timestamp_ms < cutoff_ms
                    ):
                        continue
                    retained.append(row)
        except OSError:
            return 0

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.rolling.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                for row in retained:
                    handle.write(
                        json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
                    )
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return len(retained)
