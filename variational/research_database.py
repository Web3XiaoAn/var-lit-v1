from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Sequence


RESEARCH_DATABASE_SCHEMA = "variational-research-sqlite-v1"
DEFAULT_MAX_DATABASE_BYTES = 900 * 1024 * 1024
DEFAULT_SYNC_INTERVAL_SECONDS = 1.0
INSERT_BATCH_SIZE = 1_000
RETENTION_CHECK_INTERVAL_SECONDS = 60.0
RETENTION_DELETE_BATCH_SIZE = 10_000
RUNTIME_LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S,%f"
AUTOMATIC_BAD_EXECUTION_BPS = Decimal("2.0")


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _parse_iso_timestamp_ms(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = int(value)
        if numeric > 10_000_000_000:
            return numeric
        if numeric > 0:
            return numeric * 1_000
        return None
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000)


def event_timestamp_ms(payload: dict[str, Any]) -> int:
    for field in (
        "sample_timestamp_ms",
        "frameCapturedAtMs",
        "createdAtMs",
        "utc_time",
        "logged_at",
        "timestamp",
        "created_at",
        "opened_at",
        "closed_at",
        "saved_at",
    ):
        parsed = _parse_iso_timestamp_ms(payload.get(field))
        if parsed is not None:
            return parsed
    return int(time.time() * 1_000)


def _event_key(stream: str, payload_json: str) -> str:
    digest = hashlib.sha256()
    digest.update(stream.encode("utf-8"))
    digest.update(b"\0")
    digest.update(payload_json.encode("utf-8"))
    return digest.hexdigest()


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _leg_pnl(payload: dict[str, Any]) -> Decimal | None:
    side = str(payload.get("side") or "").strip().lower()
    qty = _decimal(payload.get("qty"))
    lighter_qty = _decimal(payload.get("lighter_filled_qty"))
    var_price = _decimal(payload.get("variational_filled_price"))
    lighter_price = _decimal(payload.get("lighter_filled_price"))
    if (
        side not in {"buy", "sell"}
        or qty is None
        or var_price is None
        or lighter_price is None
    ):
        return None
    matched_qty = min(qty, lighter_qty or qty)
    if side == "buy":
        return (lighter_price - var_price) * matched_qty
    return (var_price - lighter_price) * matched_qty


def _actual_lighter_fill(payload: dict[str, Any]) -> tuple[Decimal, Decimal]:
    qty = _decimal(payload.get("qty")) or Decimal("0")
    price = _decimal(payload.get("lighter_filled_price"))
    if price is None:
        return Decimal("0"), Decimal("0")
    filled_qty = min(qty, _decimal(payload.get("lighter_filled_qty")) or qty)
    filled_quote = _decimal(payload.get("lighter_filled_quote"))
    return filled_qty, filled_quote if filled_quote is not None else price * filled_qty


def _actual_round_pnl(
    open_payload: dict[str, Any],
    close_payload: dict[str, Any],
) -> Decimal | None:
    side = str(open_payload.get("side") or "").strip().lower()
    qty = _decimal(open_payload.get("qty"))
    open_var = _decimal(open_payload.get("variational_filled_price"))
    close_var = _decimal(close_payload.get("variational_filled_price"))
    if (
        side not in {"buy", "sell"}
        or qty is None
        or open_var is None
        or close_var is None
    ):
        return None
    open_lighter_qty, open_lighter_quote = _actual_lighter_fill(open_payload)
    close_lighter_qty, close_lighter_quote = _actual_lighter_fill(close_payload)
    if open_lighter_qty != close_lighter_qty:
        return None
    if side == "buy":
        return (
            (close_var - open_var) * qty
            + open_lighter_quote
            - close_lighter_quote
        )
    return (
        (open_var - close_var) * qty
        + close_lighter_quote
        - open_lighter_quote
    )


def _has_var_fill(payload: dict[str, Any]) -> bool:
    return bool(
        str(payload.get("side") or "").strip().lower() in {"buy", "sell"}
        and _decimal(payload.get("qty")) is not None
        and _decimal(payload.get("variational_filled_price")) is not None
    )


def _execution_loss(payload: dict[str, Any]) -> Decimal | None:
    """Return loss from the latest final fills, falling back to stored data."""

    firm_guard_pnl = _decimal(payload.get("firm_guard_pnl"))
    actual_pnl = _leg_pnl(payload)
    if firm_guard_pnl is not None and actual_pnl is not None:
        return firm_guard_pnl - actual_pnl
    return _decimal(payload.get("execution_loss_usd"))


def _fill_completed_at_ms(
    payload: dict[str, Any],
    fallback_ms: int,
) -> int:
    fill_times = [
        parsed
        for parsed in (
            _parse_iso_timestamp_ms(payload.get("variational_filled_at")),
            _parse_iso_timestamp_ms(payload.get("lighter_filled_at")),
        )
        if parsed is not None
    ]
    return max(fill_times, default=fallback_ms)


def _round_key(open_trade_key: str, close_trade_key: str) -> str:
    return hashlib.sha256(
        f"{open_trade_key}\0{close_trade_key}".encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class ResearchEvent:
    stream: str
    payload: dict[str, Any]
    source: str
    pinned: bool = False


@dataclass(frozen=True)
class SyncSource:
    path: Path
    stream: str
    parser: str = "jsonl"
    pinned: bool = False


@dataclass
class TailState:
    device: int
    inode: int
    offset: int


class ResearchDatabase:
    """Deduplicated SQLite store for research and replay data.

    Runtime recovery remains in the bounded live log directory. This database is
    observational only and is never read by the order path.
    """

    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = DEFAULT_MAX_DATABASE_BYTES,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.path = path.expanduser().resolve()
        self.max_bytes = max_bytes
        self._last_retention_check: float | None = None

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA cache_size=-8192")
        connection.execute("PRAGMA journal_size_limit=16777216")
        connection.execute("PRAGMA temp_store=MEMORY")
        self._initialize(connection)
        return connection

    def _initialize(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS research_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream TEXT NOT NULL,
                event_time_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                pinned INTEGER NOT NULL DEFAULT 0,
                event_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                ingested_at_ms INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS research_events_stream_time
            ON research_events(stream, event_time_ms)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS research_events_retention
            ON research_events(pinned, event_time_ms)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS research_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS research_rounds (
                round_key TEXT PRIMARY KEY,
                opened_at_ms INTEGER NOT NULL,
                closed_at_ms INTEGER NOT NULL,
                open_trade_key TEXT NOT NULL,
                close_trade_key TEXT NOT NULL,
                strategy TEXT,
                asset TEXT,
                direction TEXT NOT NULL,
                quantity TEXT,
                open_notional_usd TEXT,
                open_leg_pnl_usd TEXT,
                close_leg_pnl_usd TEXT,
                round_pnl_usd TEXT,
                open_execution_loss_usd TEXT,
                close_execution_loss_usd TEXT,
                total_execution_loss_usd TEXT,
                open_execution_loss_bps TEXT,
                total_execution_loss_bps TEXT,
                open_context_json TEXT,
                close_context_json TEXT,
                payload_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS research_rounds_closed_at
            ON research_rounds(closed_at_ms)
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS round_quality_labels (
                round_key TEXT NOT NULL,
                label TEXT NOT NULL,
                source TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                note TEXT,
                metrics_json TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(round_key, label, source),
                FOREIGN KEY(round_key) REFERENCES research_rounds(round_key)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_labels (
                trade_key TEXT NOT NULL,
                phase TEXT NOT NULL,
                label TEXT NOT NULL,
                source TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                note TEXT,
                metrics_json TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(trade_key, label, source)
            )
            """
        )
        connection.execute("DROP VIEW IF EXISTS research_round_quality")
        connection.execute(
            """
            CREATE VIEW research_round_quality AS
            SELECT
                rounds.*,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM round_quality_labels labels
                        WHERE labels.round_key = rounds.round_key
                          AND labels.source = 'manual_user'
                          AND labels.label = 'bad_execution'
                    ) THEN 'bad_execution'
                    WHEN EXISTS (
                        SELECT 1
                        FROM trade_quality_labels labels
                        WHERE labels.trade_key IN (
                            rounds.open_trade_key,
                            rounds.close_trade_key
                        )
                          AND labels.source = 'manual_user'
                          AND labels.label = 'bad_execution'
                    ) THEN 'bad_execution'
                    WHEN EXISTS (
                        SELECT 1
                        FROM round_quality_labels labels
                        WHERE labels.round_key = rounds.round_key
                          AND labels.source = 'automatic'
                          AND labels.label = 'suspected_bad_execution'
                    ) THEN 'suspected_bad_execution'
                    WHEN EXISTS (
                        SELECT 1
                        FROM trade_quality_labels labels
                        WHERE labels.trade_key IN (
                            rounds.open_trade_key,
                            rounds.close_trade_key
                        )
                          AND labels.source = 'automatic'
                          AND labels.label = 'suspected_bad_execution'
                    ) THEN 'suspected_bad_execution'
                    WHEN EXISTS (
                        SELECT 1
                        FROM round_quality_labels labels
                        WHERE labels.round_key = rounds.round_key
                          AND labels.source = 'manual_user'
                          AND labels.label = 'acceptable_execution'
                    ) THEN 'acceptable_execution'
                    ELSE 'unflagged'
                END AS effective_quality
            FROM research_rounds rounds
            """
        )
        connection.execute(
            """
            INSERT INTO research_metadata(key, value)
            VALUES('schema', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (RESEARCH_DATABASE_SCHEMA,),
        )
        connection.execute(
            """
            INSERT INTO research_metadata(key, value)
            VALUES('max_database_bytes', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(self.max_bytes),),
        )
        connection.commit()

    def insert_events(self, events: Iterable[ResearchEvent]) -> tuple[int, int]:
        prepared: list[tuple[Any, ...]] = []
        now_ms = int(time.time() * 1_000)
        for event in events:
            payload_json = _canonical_json(event.payload)
            prepared.append(
                (
                    event.stream,
                    event_timestamp_ms(event.payload),
                    event.source,
                    1 if event.pinned else 0,
                    _event_key(event.stream, payload_json),
                    payload_json,
                    now_ms,
                )
            )
        if not prepared:
            return 0, 0
        with self._connect() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO research_events(
                    stream,
                    event_time_ms,
                    source,
                    pinned,
                    event_key,
                    payload_json,
                    ingested_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                prepared,
            )
            inserted = connection.total_changes - before
            connection.commit()
            self._enforce_retention_if_due(connection)
        return inserted, len(prepared) - inserted

    def ingest_jsonl(
        self,
        path: Path,
        *,
        stream: str,
        source: str | None = None,
        pinned: bool = False,
    ) -> tuple[int, int, int]:
        inserted = 0
        duplicates = 0
        malformed = 0
        batch: list[ResearchEvent] = []
        opener: Callable[..., Any] = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(payload, dict):
                    malformed += 1
                    continue
                batch.append(
                    ResearchEvent(
                        stream=stream,
                        payload=payload,
                        source=source or path.as_posix(),
                        pinned=pinned,
                    )
                )
                if len(batch) >= INSERT_BATCH_SIZE:
                    added, repeated = self.insert_events(batch)
                    inserted += added
                    duplicates += repeated
                    batch.clear()
        if batch:
            added, repeated = self.insert_events(batch)
            inserted += added
            duplicates += repeated
        return inserted, duplicates, malformed

    def ingest_json_document(
        self,
        path: Path,
        *,
        stream: str,
        source: str | None = None,
        pinned: bool = False,
    ) -> tuple[int, int]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        wrapped = payload if isinstance(payload, dict) else {"value": payload}
        return self.insert_events(
            [
                ResearchEvent(
                    stream=stream,
                    payload=wrapped,
                    source=source or path.as_posix(),
                    pinned=pinned,
                )
            ]
        )

    def ingest_runtime_log(
        self,
        path: Path,
        *,
        source: str | None = None,
        pinned: bool = False,
    ) -> tuple[int, int]:
        events: list[ResearchEvent] = []
        inserted = 0
        duplicates = 0
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                payload = runtime_log_payload(line.rstrip("\n"))
                events.append(
                    ResearchEvent(
                        stream="runtime_log",
                        payload=payload,
                        source=source or path.as_posix(),
                        pinned=pinned,
                    )
                )
                if len(events) >= INSERT_BATCH_SIZE:
                    added, repeated = self.insert_events(events)
                    inserted += added
                    duplicates += repeated
                    events.clear()
        if events:
            added, repeated = self.insert_events(events)
            inserted += added
            duplicates += repeated
        return inserted, duplicates

    def byte_size(self) -> int:
        return sum(
            candidate.stat().st_size
            for candidate in (
                self.path,
                self.path.with_name(f"{self.path.name}-wal"),
                self.path.with_name(f"{self.path.name}-shm"),
            )
            if candidate.is_file()
        )

    def stats(self) -> dict[str, Any]:
        with self._connect() as connection:
            streams = {
                str(stream): int(count)
                for stream, count in connection.execute(
                    """
                    SELECT stream, COUNT(*)
                    FROM research_events
                    GROUP BY stream
                    ORDER BY stream
                    """
                )
            }
            total = int(
                connection.execute(
                    "SELECT COUNT(*) FROM research_events"
                ).fetchone()[0]
            )
            pinned = int(
                connection.execute(
                    "SELECT COUNT(*) FROM research_events WHERE pinned = 1"
                ).fetchone()[0]
            )
            oldest, newest = connection.execute(
                "SELECT MIN(event_time_ms), MAX(event_time_ms) FROM research_events"
            ).fetchone()
            rounds = int(
                connection.execute(
                    "SELECT COUNT(*) FROM research_rounds"
                ).fetchone()[0]
            )
            bad_rounds = int(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM research_round_quality
                    WHERE effective_quality IN (
                        'bad_execution',
                        'suspected_bad_execution'
                    )
                    """
                ).fetchone()[0]
            )
        return {
            "schema": RESEARCH_DATABASE_SCHEMA,
            "path": self.path.as_posix(),
            "bytes": self.byte_size(),
            "max_bytes": self.max_bytes,
            "events": total,
            "pinned_events": pinned,
            "oldest_event_time_ms": oldest,
            "newest_event_time_ms": newest,
            "streams": streams,
            "rounds": rounds,
            "flagged_bad_execution_rounds": bad_rounds,
        }

    def refresh_derived_rounds(self) -> dict[str, int]:
        """Pair final open/close fills and update compact research round rows."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT event_time_ms, id, payload_json
                FROM research_events
                WHERE stream = 'order_metric'
                  AND json_extract(payload_json, '$.strategy_phase')
                      IN ('open', 'close', 'emergency_close')
                  AND json_extract(
                          payload_json,
                          '$.variational_filled_price'
                      ) IS NOT NULL
                ORDER BY event_time_ms, id
                """
            ).fetchall()
            final_fills: dict[str, tuple[int, int, dict[str, Any]]] = {}
            for event_time_ms, _event_id, payload_json in rows:
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue
                trade_key = str(payload.get("trade_key") or "").strip()
                if not trade_key:
                    continue
                # A Lighter fill can be logged before the authoritative Var
                # fill/recovery arrives. Keep following every lifecycle state
                # and retain the latest snapshot with a final Var fill. A
                # protective recovery may intentionally have no Lighter fill;
                # a later lifecycle correction replaces the provisional row.
                if not _has_var_fill(payload):
                    continue
                source_event_ms = int(event_time_ms)
                completed_at_ms = _fill_completed_at_ms(
                    payload,
                    source_event_ms,
                )
                final_fills[trade_key] = (
                    source_event_ms,
                    completed_at_ms,
                    payload,
                )

            ordered_fills = sorted(
                (
                    (completed_at_ms, payload)
                    for _source_event_ms, completed_at_ms, payload
                    in final_fills.values()
                ),
                key=lambda row: (row[0], str(row[1].get("trade_key") or "")),
            )
            pending_opens: dict[str, tuple[int, dict[str, Any]]] = {}
            derived: list[dict[str, Any]] = []
            for timestamp_ms, payload in ordered_fills:
                self._update_automatic_trade_label(
                    connection,
                    payload,
                    now_ms=int(time.time() * 1_000),
                )
                phase = str(payload.get("strategy_phase") or "").lower()
                asset = str(payload.get("asset") or "UNKNOWN").upper()
                if phase == "open":
                    pending_opens[asset] = (timestamp_ms, payload)
                    continue
                open_record = pending_opens.get(asset)
                if open_record is None:
                    continue
                open_timestamp_ms, open_payload = open_record
                if (
                    str(open_payload.get("side") or "").lower()
                    == str(payload.get("side") or "").lower()
                    or _decimal(open_payload.get("qty"))
                    != _decimal(payload.get("qty"))
                ):
                    continue
                derived.append(
                    self._normalized_round_payload(
                        open_timestamp_ms,
                        open_payload,
                        timestamp_ms,
                        payload,
                    )
                )
                pending_opens.pop(asset, None)

            now_ms = int(time.time() * 1_000)
            before = connection.total_changes
            for payload in derived:
                connection.execute(
                    """
                    INSERT INTO research_rounds(
                        round_key,
                        opened_at_ms,
                        closed_at_ms,
                        open_trade_key,
                        close_trade_key,
                        strategy,
                        asset,
                        direction,
                        quantity,
                        open_notional_usd,
                        open_leg_pnl_usd,
                        close_leg_pnl_usd,
                        round_pnl_usd,
                        open_execution_loss_usd,
                        close_execution_loss_usd,
                        total_execution_loss_usd,
                        open_execution_loss_bps,
                        total_execution_loss_bps,
                        open_context_json,
                        close_context_json,
                        payload_json,
                        updated_at_ms
                    ) VALUES (
                        :round_key,
                        :opened_at_ms,
                        :closed_at_ms,
                        :open_trade_key,
                        :close_trade_key,
                        :strategy,
                        :asset,
                        :direction,
                        :quantity,
                        :open_notional_usd,
                        :open_leg_pnl_usd,
                        :close_leg_pnl_usd,
                        :round_pnl_usd,
                        :open_execution_loss_usd,
                        :close_execution_loss_usd,
                        :total_execution_loss_usd,
                        :open_execution_loss_bps,
                        :total_execution_loss_bps,
                        :open_context_json,
                        :close_context_json,
                        :payload_json,
                        :updated_at_ms
                    )
                    ON CONFLICT(round_key) DO UPDATE SET
                        opened_at_ms=excluded.opened_at_ms,
                        closed_at_ms=excluded.closed_at_ms,
                        open_trade_key=excluded.open_trade_key,
                        close_trade_key=excluded.close_trade_key,
                        strategy=excluded.strategy,
                        asset=excluded.asset,
                        direction=excluded.direction,
                        quantity=excluded.quantity,
                        open_notional_usd=excluded.open_notional_usd,
                        open_leg_pnl_usd=excluded.open_leg_pnl_usd,
                        close_leg_pnl_usd=excluded.close_leg_pnl_usd,
                        round_pnl_usd=excluded.round_pnl_usd,
                        open_execution_loss_usd=excluded.open_execution_loss_usd,
                        close_execution_loss_usd=excluded.close_execution_loss_usd,
                        total_execution_loss_usd=excluded.total_execution_loss_usd,
                        open_execution_loss_bps=excluded.open_execution_loss_bps,
                        total_execution_loss_bps=excluded.total_execution_loss_bps,
                        open_context_json=excluded.open_context_json,
                        close_context_json=excluded.close_context_json,
                        payload_json=excluded.payload_json,
                        updated_at_ms=excluded.updated_at_ms
                    """,
                    {**payload, "updated_at_ms": now_ms},
                )
                self._update_automatic_round_label(
                    connection,
                    payload,
                    now_ms=now_ms,
                )
            connection.commit()
            return {
                "rounds_seen": len(derived),
                "database_changes": connection.total_changes - before,
            }

    @staticmethod
    def _normalized_round_payload(
        open_timestamp_ms: int,
        open_payload: dict[str, Any],
        close_timestamp_ms: int,
        close_payload: dict[str, Any],
    ) -> dict[str, Any]:
        open_trade_key = str(open_payload.get("trade_key"))
        close_trade_key = str(close_payload.get("trade_key"))
        open_side = str(open_payload.get("side") or "").lower()
        qty = _decimal(open_payload.get("qty"))
        open_var_price = _decimal(open_payload.get("variational_filled_price"))
        open_notional = (
            qty * open_var_price
            if qty is not None and open_var_price is not None
            else None
        )
        open_pnl = _leg_pnl(open_payload)
        round_pnl = _actual_round_pnl(open_payload, close_payload)
        open_lighter_qty, _open_lighter_quote = _actual_lighter_fill(open_payload)
        close_lighter_qty, _close_lighter_quote = _actual_lighter_fill(close_payload)
        recovery = bool(
            str(close_payload.get("strategy_phase") or "").lower()
            == "emergency_close"
            or open_lighter_qty != qty
            or close_lighter_qty != qty
        )
        if round_pnl is not None:
            firm_guard_pnl = _decimal(open_payload.get("firm_guard_pnl"))
            open_pnl = (
                firm_guard_pnl
                if recovery and open_lighter_qty != qty and firm_guard_pnl is not None
                else (open_pnl or Decimal("0"))
            )
            close_pnl = round_pnl - open_pnl
        else:
            close_pnl = _leg_pnl(close_payload)
        open_loss = _execution_loss(open_payload) or Decimal("0")
        close_loss = _execution_loss(close_payload) or Decimal("0")
        open_loss_bps = (
            open_loss / open_notional * Decimal("10000")
            if open_notional is not None and open_notional > 0
            else None
        )
        total_loss = open_loss + close_loss
        total_loss_bps = (
            total_loss / open_notional * Decimal("10000")
            if open_notional is not None and open_notional > 0
            else None
        )
        open_context = open_payload.get("adaptive_strategy_context")
        close_context = close_payload.get("adaptive_strategy_context")
        payload = {
            "schema": "research-normalized-round-v2",
            "round_key": _round_key(open_trade_key, close_trade_key),
            "opened_at_ms": open_timestamp_ms,
            "closed_at_ms": close_timestamp_ms,
            "open_trade_key": open_trade_key,
            "close_trade_key": close_trade_key,
            "strategy": (
                open_payload.get("strategy_tag")
                or open_payload.get("strategy_policy")
                or "unknown"
            ),
            "asset": open_payload.get("asset"),
            "direction": (
                "long_var_short_lighter"
                if open_side == "buy"
                else "short_var_long_lighter"
            ),
            "round_class": "protective_recovery" if recovery else "strategy",
            "quantity": _decimal_text(qty),
            "open_notional_usd": _decimal_text(open_notional),
            "open_leg_pnl_usd": _decimal_text(open_pnl),
            "close_leg_pnl_usd": _decimal_text(close_pnl),
            "round_pnl_usd": _decimal_text(round_pnl),
            "open_execution_loss_usd": _decimal_text(open_loss),
            "close_execution_loss_usd": _decimal_text(close_loss),
            "total_execution_loss_usd": _decimal_text(total_loss),
            "open_execution_loss_bps": _decimal_text(open_loss_bps),
            "total_execution_loss_bps": _decimal_text(total_loss_bps),
            "open_context_json": (
                _canonical_json(open_context)
                if isinstance(open_context, dict)
                else None
            ),
            "close_context_json": (
                _canonical_json(close_context)
                if isinstance(close_context, dict)
                else None
            ),
        }
        payload["payload_json"] = _canonical_json(
            {
                key: value
                for key, value in payload.items()
                if key not in {"open_context_json", "close_context_json", "payload_json"}
            }
            | {
                "open_context": open_context,
                "close_context": close_context,
            }
        )
        return payload

    @staticmethod
    def _update_automatic_round_label(
        connection: sqlite3.Connection,
        payload: dict[str, Any],
        *,
        now_ms: int,
    ) -> None:
        total_loss_bps = _decimal(payload.get("total_execution_loss_bps"))
        round_key = str(payload["round_key"])
        if (
            total_loss_bps is not None
            and total_loss_bps >= AUTOMATIC_BAD_EXECUTION_BPS
        ):
            metrics = _canonical_json(
                {
                    "total_execution_loss_bps": _decimal_text(total_loss_bps),
                    "threshold_bps": _decimal_text(
                        AUTOMATIC_BAD_EXECUTION_BPS
                    ),
                    "total_execution_loss_usd": payload.get(
                        "total_execution_loss_usd"
                    ),
                    "open_notional_usd": payload.get("open_notional_usd"),
                }
            )
            connection.execute(
                """
                INSERT INTO round_quality_labels(
                    round_key,
                    label,
                    source,
                    reason_code,
                    note,
                    metrics_json,
                    created_at_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(round_key, label, source) DO UPDATE SET
                    reason_code=excluded.reason_code,
                    note=excluded.note,
                    metrics_json=excluded.metrics_json,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    round_key,
                    "suspected_bad_execution",
                    "automatic",
                    "round_execution_loss_bps_gte_2",
                    "自动筛查：整轮执行损耗达到或超过2bps",
                    metrics,
                    now_ms,
                    now_ms,
                ),
            )
        else:
            connection.execute(
                """
                DELETE FROM round_quality_labels
                WHERE round_key = ?
                  AND label = 'suspected_bad_execution'
                  AND source = 'automatic'
                """,
                (round_key,),
            )

    def label_round(
        self,
        *,
        open_trade_key: str,
        close_trade_key: str,
        label: str,
        reason_code: str,
        note: str,
        source: str = "manual_user",
    ) -> str:
        round_key = _round_key(open_trade_key, close_trade_key)
        now_ms = int(time.time() * 1_000)
        with self._connect() as connection:
            exists = connection.execute(
                "SELECT 1 FROM research_rounds WHERE round_key = ?",
                (round_key,),
            ).fetchone()
            if exists is None:
                raise KeyError(
                    "round not found for trade keys: "
                    f"{open_trade_key} / {close_trade_key}"
                )
            connection.execute(
                """
                INSERT INTO round_quality_labels(
                    round_key,
                    label,
                    source,
                    reason_code,
                    note,
                    metrics_json,
                    created_at_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(round_key, label, source) DO UPDATE SET
                    reason_code=excluded.reason_code,
                    note=excluded.note,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    round_key,
                    label,
                    source,
                    reason_code,
                    note,
                    now_ms,
                    now_ms,
                ),
            )
            connection.commit()
        return round_key

    def label_trade(
        self,
        *,
        trade_key: str,
        phase: str,
        label: str,
        reason_code: str,
        note: str,
        source: str = "manual_user",
    ) -> str:
        normalized_phase = phase.strip().lower()
        if normalized_phase not in {"open", "close"}:
            raise ValueError("phase must be open or close")
        now_ms = int(time.time() * 1_000)
        with self._connect() as connection:
            exists = connection.execute(
                """
                SELECT 1
                FROM research_events
                WHERE stream = 'order_metric'
                  AND json_extract(payload_json, '$.event') = 'lighter_fill'
                  AND json_extract(payload_json, '$.trade_key') = ?
                  AND json_extract(payload_json, '$.strategy_phase') = ?
                LIMIT 1
                """,
                (trade_key, normalized_phase),
            ).fetchone()
            if exists is None:
                raise KeyError(f"completed fill not found for trade key: {trade_key}")
            connection.execute(
                """
                INSERT INTO trade_quality_labels(
                    trade_key,
                    phase,
                    label,
                    source,
                    reason_code,
                    note,
                    metrics_json,
                    created_at_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(trade_key, label, source) DO UPDATE SET
                    phase=excluded.phase,
                    reason_code=excluded.reason_code,
                    note=excluded.note,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    trade_key,
                    normalized_phase,
                    label,
                    source,
                    reason_code,
                    note,
                    now_ms,
                    now_ms,
                ),
            )
            connection.commit()
        return trade_key

    @staticmethod
    def _update_automatic_trade_label(
        connection: sqlite3.Connection,
        payload: dict[str, Any],
        *,
        now_ms: int,
    ) -> None:
        trade_key = str(payload.get("trade_key") or "").strip()
        phase = str(payload.get("strategy_phase") or "").strip().lower()
        qty = _decimal(payload.get("qty"))
        var_price = _decimal(payload.get("variational_filled_price"))
        execution_loss = _execution_loss(payload)
        notional = (
            qty * var_price
            if qty is not None and var_price is not None
            else None
        )
        execution_loss_bps = (
            execution_loss / notional * Decimal("10000")
            if execution_loss is not None
            and notional is not None
            and notional > 0
            else None
        )
        if not trade_key or phase not in {"open", "close"}:
            return
        if (
            execution_loss_bps is not None
            and execution_loss_bps >= AUTOMATIC_BAD_EXECUTION_BPS
        ):
            connection.execute(
                """
                INSERT INTO trade_quality_labels(
                    trade_key,
                    phase,
                    label,
                    source,
                    reason_code,
                    note,
                    metrics_json,
                    created_at_ms,
                    updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key, label, source) DO UPDATE SET
                    phase=excluded.phase,
                    reason_code=excluded.reason_code,
                    note=excluded.note,
                    metrics_json=excluded.metrics_json,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    trade_key,
                    phase,
                    "suspected_bad_execution",
                    "automatic",
                    "leg_execution_loss_bps_gte_2",
                    "自动筛查：单腿执行损耗达到或超过2bps",
                    _canonical_json(
                        {
                            "execution_loss_bps": _decimal_text(
                                execution_loss_bps
                            ),
                            "threshold_bps": _decimal_text(
                                AUTOMATIC_BAD_EXECUTION_BPS
                            ),
                            "execution_loss_usd": _decimal_text(execution_loss),
                            "notional_usd": _decimal_text(notional),
                        }
                    ),
                    now_ms,
                    now_ms,
                ),
            )
        else:
            connection.execute(
                """
                DELETE FROM trade_quality_labels
                WHERE trade_key = ?
                  AND label = 'suspected_bad_execution'
                  AND source = 'automatic'
                """,
                (trade_key,),
            )

    def _enforce_retention_if_due(self, connection: sqlite3.Connection) -> None:
        now = time.monotonic()
        if (
            self._last_retention_check is not None
            and now - self._last_retention_check
            < RETENTION_CHECK_INTERVAL_SECONDS
        ):
            return
        self._last_retention_check = now
        if self.byte_size() <= self.max_bytes:
            return
        target_bytes = int(self.max_bytes * 0.90)
        while self.byte_size() > target_bytes:
            current_bytes = self.byte_size()
            total_payload_bytes = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(LENGTH(payload_json)), 0)
                    FROM research_events
                    """
                ).fetchone()[0]
            )
            if total_payload_bytes <= 0:
                break
            storage_ratio = max(1.0, current_bytes / total_payload_bytes)
            payload_target = int(
                ((current_bytes - target_bytes) / storage_ratio) * 1.20
            )
            deleted_payload = 0
            deleted_rows = 0
            while deleted_payload < payload_target:
                selected = connection.execute(
                    """
                    SELECT id, LENGTH(payload_json)
                    FROM research_events
                    WHERE pinned = 0
                    ORDER BY
                        CASE stream
                            WHEN 'execution_trace' THEN 0
                            WHEN 'runtime_log' THEN 1
                            WHEN 'strategy_market_sample' THEN 2
                            WHEN 'runtime_state' THEN 3
                            WHEN 'execution_sample' THEN 4
                            WHEN 'order_metric' THEN 5
                            ELSE 2
                        END,
                        event_time_ms,
                        id
                    LIMIT ?
                    """,
                    (RETENTION_DELETE_BATCH_SIZE,),
                ).fetchall()
                if not selected:
                    break
                connection.executemany(
                    "DELETE FROM research_events WHERE id = ?",
                    ((int(row_id),) for row_id, _length in selected),
                )
                deleted_payload += sum(int(length or 0) for _row_id, length in selected)
                deleted_rows += len(selected)
            connection.commit()
            if deleted_rows == 0:
                break
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.commit()
            connection.execute("VACUUM")
            connection.commit()


def runtime_log_payload(line: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"line": line}
    timestamp_text = line.split(" | ", 1)[0].strip()
    try:
        local_time = datetime.strptime(
            timestamp_text,
            RUNTIME_LOG_TIMESTAMP_FORMAT,
        ).astimezone()
    except ValueError:
        return payload
    payload["timestamp"] = local_time.isoformat()
    return payload


def parse_line(parser: str, raw_line: bytes) -> dict[str, Any] | None:
    text = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
    if not text:
        return None
    if parser == "runtime_log":
        return runtime_log_payload(text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


class ResearchDatabaseSynchronizer:
    """Incrementally follows bounded runtime files into the SQLite corpus."""

    def __init__(
        self,
        database: ResearchDatabase,
        sources: Sequence[SyncSource],
    ) -> None:
        self.database = database
        self.sources = tuple(sources)
        self._tails: dict[str, TailState] = {}
        self._document_hashes: dict[str, str] = {}
        self.sync_failures = 0
        self.malformed_lines = 0
        self.inserted_events = 0
        self.duplicate_events = 0
        self._derived_initialized = False

    def sync_once(self) -> dict[str, int]:
        inserted_before = self.inserted_events
        duplicates_before = self.duplicate_events
        malformed_before = self.malformed_lines
        order_metrics_changed = False
        for source in self._expanded_sources():
            try:
                inserted_before_source = self.inserted_events
                if source.parser == "json_document":
                    self._sync_document(source)
                else:
                    self._sync_append_file(source)
                if (
                    source.stream == "order_metric"
                    and self.inserted_events > inserted_before_source
                ):
                    order_metrics_changed = True
            except OSError:
                self.sync_failures += 1
            except (json.JSONDecodeError, sqlite3.Error):
                self.sync_failures += 1
        if order_metrics_changed or not self._derived_initialized:
            self.database.refresh_derived_rounds()
            self._derived_initialized = True
        return {
            "inserted": self.inserted_events - inserted_before,
            "duplicates": self.duplicate_events - duplicates_before,
            "malformed": self.malformed_lines - malformed_before,
            "failures": self.sync_failures,
        }

    def _expanded_sources(self) -> Iterator[SyncSource]:
        for source in self.sources:
            if source.parser == "json_document":
                if source.path.is_file():
                    yield source
                continue
            candidates = [source.path]
            candidates.extend(
                sorted(
                    source.path.parent.glob(f"{source.path.name}.*"),
                    key=lambda path: path.name,
                    reverse=True,
                )
            )
            for candidate in candidates:
                if candidate.is_file():
                    yield SyncSource(
                        candidate,
                        stream=source.stream,
                        parser=source.parser,
                        pinned=source.pinned,
                    )

    def _sync_document(self, source: SyncSource) -> None:
        raw = source.path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        state_key = source.path.resolve().as_posix()
        if self._document_hashes.get(state_key) == digest:
            return
        payload = json.loads(raw.decode("utf-8"))
        wrapped = payload if isinstance(payload, dict) else {"value": payload}
        added, repeated = self.database.insert_events(
            [
                ResearchEvent(
                    stream=source.stream,
                    payload=wrapped,
                    source=source.path.as_posix(),
                    pinned=source.pinned,
                )
            ]
        )
        self.inserted_events += added
        self.duplicate_events += repeated
        self._document_hashes[state_key] = digest

    def _sync_append_file(self, source: SyncSource) -> None:
        stat_result = source.path.stat()
        state_key = source.path.resolve().as_posix()
        state = self._tails.get(state_key)
        if (
            state is None
            or state.device != stat_result.st_dev
            or state.inode != stat_result.st_ino
            or stat_result.st_size < state.offset
        ):
            state = TailState(
                device=stat_result.st_dev,
                inode=stat_result.st_ino,
                offset=0,
            )
            self._tails[state_key] = state
        events: list[ResearchEvent] = []
        with source.path.open("rb") as handle:
            handle.seek(state.offset)
            while True:
                line_start = handle.tell()
                raw_line = handle.readline()
                if not raw_line:
                    break
                if not raw_line.endswith((b"\n", b"\r")):
                    # Do not consume a row that is still being appended. The
                    # next poll rereads it once the terminating newline exists.
                    handle.seek(line_start)
                    break
                payload = parse_line(source.parser, raw_line)
                if payload is None:
                    self.malformed_lines += 1
                    continue
                events.append(
                    ResearchEvent(
                        stream=source.stream,
                        payload=payload,
                        source=source.path.as_posix(),
                        pinned=source.pinned,
                    )
                )
                if len(events) >= INSERT_BATCH_SIZE:
                    added, repeated = self.database.insert_events(events)
                    self.inserted_events += added
                    self.duplicate_events += repeated
                    events.clear()
            state.offset = handle.tell()
        if events:
            added, repeated = self.database.insert_events(events)
            self.inserted_events += added
            self.duplicate_events += repeated


def default_runtime_sources(runtime_dir: Path) -> tuple[SyncSource, ...]:
    return (
        SyncSource(
            runtime_dir / "strategy_market_samples.jsonl",
            stream="strategy_market_sample",
        ),
        SyncSource(
            runtime_dir / "order_metrics.jsonl",
            stream="order_metric",
        ),
        SyncSource(
            runtime_dir / "execution_trace.jsonl",
            stream="execution_trace",
        ),
        SyncSource(
            runtime_dir / "runtime.log",
            stream="runtime_log",
            parser="runtime_log",
        ),
        SyncSource(
            runtime_dir / "runtime_state.json",
            stream="runtime_state",
            parser="json_document",
        ),
        SyncSource(
            runtime_dir / "execution_samples.json",
            stream="execution_sample",
            parser="json_document",
        ),
        SyncSource(
            runtime_dir / "current_strategy_sample_session.json",
            stream="sampling_session",
            parser="json_document",
        ),
    )
