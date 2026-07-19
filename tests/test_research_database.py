from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from variational.research_database import (
    ResearchDatabase,
    ResearchDatabaseSynchronizer,
    ResearchEvent,
    SyncSource,
)


class ResearchDatabaseTests(unittest.TestCase):
    def test_insert_is_deduplicated_and_queryable(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-db-") as tmp:
            database = ResearchDatabase(Path(tmp) / "research.sqlite3")
            event = ResearchEvent(
                stream="strategy_market_sample",
                payload={"sample_timestamp_ms": 1234, "rate": "0.001"},
                source="test",
            )

            self.assertEqual(database.insert_events([event]), (1, 0))
            self.assertEqual(database.insert_events([event]), (0, 1))

            stats = database.stats()
            self.assertEqual(stats["events"], 1)
            self.assertEqual(
                stats["streams"],
                {"strategy_market_sample": 1},
            )

    def test_synchronizer_follows_append_and_recovers_after_replace(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-sync-") as tmp:
            root = Path(tmp)
            source = root / "strategy_market_samples.jsonl"
            source.write_text(
                json.dumps({"sample_timestamp_ms": 1, "value": "first"}) + "\n",
                encoding="utf-8",
            )
            database = ResearchDatabase(root / "research.sqlite3")
            synchronizer = ResearchDatabaseSynchronizer(
                database,
                [
                    SyncSource(
                        source,
                        stream="strategy_market_sample",
                    )
                ],
            )

            self.assertEqual(synchronizer.sync_once()["inserted"], 1)
            with source.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({"sample_timestamp_ms": 2, "value": "second"})
                    + "\n"
                )
            self.assertEqual(synchronizer.sync_once()["inserted"], 1)
            with source.open("ab") as handle:
                handle.write(
                    json.dumps(
                        {"sample_timestamp_ms": 4, "value": "partial"}
                    ).encode("utf-8")
                )
            self.assertEqual(synchronizer.sync_once()["inserted"], 0)
            with source.open("ab") as handle:
                handle.write(b"\n")
            self.assertEqual(synchronizer.sync_once()["inserted"], 1)

            replacement = source.with_name(".replacement.tmp")
            replacement.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {"sample_timestamp_ms": 2, "value": "second"}
                        ),
                        json.dumps(
                            {"sample_timestamp_ms": 3, "value": "third"}
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            replacement.replace(source)

            result = synchronizer.sync_once()
            self.assertEqual(result["inserted"], 1)
            self.assertEqual(result["duplicates"], 1)
            self.assertEqual(database.stats()["events"], 4)

    def test_restart_replay_keeps_existing_data_and_adds_only_new_rows(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-restart-") as tmp:
            root = Path(tmp)
            source = root / "strategy_market_samples.jsonl"
            source.write_text(
                json.dumps({"sample_timestamp_ms": 1, "value": "before"})
                + "\n",
                encoding="utf-8",
            )
            database = ResearchDatabase(root / "research.sqlite3")
            first = ResearchDatabaseSynchronizer(
                database,
                [SyncSource(source, stream="strategy_market_sample")],
            )
            self.assertEqual(first.sync_once()["inserted"], 1)

            with source.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({"sample_timestamp_ms": 2, "value": "after"})
                    + "\n"
                )
            restarted = ResearchDatabaseSynchronizer(
                database,
                [SyncSource(source, stream="strategy_market_sample")],
            )
            result = restarted.sync_once()

            self.assertEqual(result["inserted"], 1)
            self.assertEqual(result["duplicates"], 1)
            self.assertEqual(database.stats()["events"], 2)

    def test_pinned_history_survives_retention(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-retention-") as tmp:
            database = ResearchDatabase(
                Path(tmp) / "research.sqlite3",
                max_bytes=32 * 1024,
            )
            pinned = ResearchEvent(
                stream="strategy_market_sample",
                payload={
                    "sample_timestamp_ms": 1,
                    "value": "historical",
                },
                source="historical",
                pinned=True,
            )
            live = [
                ResearchEvent(
                    stream="execution_trace",
                    payload={
                        "logged_at": f"2026-01-01T00:00:{index:02d}+00:00",
                        "value": "x" * 2_000,
                        "index": index,
                    },
                    source="live",
                )
                for index in range(40)
            ]

            with patch(
                "variational.research_database.time.monotonic",
                return_value=1.0,
            ):
                database.insert_events([pinned, *live])

            stats = database.stats()
            self.assertEqual(stats["pinned_events"], 1)
            self.assertEqual(stats["events"], 1)
            self.assertEqual(
                stats["streams"],
                {"strategy_market_sample": 1},
            )

    def test_completed_round_is_derived_and_bad_entry_is_flagged(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-round-") as tmp:
            root = Path(tmp)
            metrics = root / "order_metrics.jsonl"
            rows = [
                {
                    "event": "lighter_fill",
                    "logged_at": "2026-01-01T00:00:00+00:00",
                    "strategy_phase": "open",
                    "side": "buy",
                    "qty": "2",
                    "lighter_filled_qty": "2",
                    "trade_key": "open-1",
                    "asset": "BTC",
                    "strategy_tag": "test",
                    "variational_filled_price": "100",
                    "lighter_filled_price": "100.10",
                    "execution_loss_usd": "0.05",
                },
                {
                    "event": "lighter_fill",
                    "logged_at": "2026-01-01T00:01:00+00:00",
                    "strategy_phase": "close",
                    "side": "sell",
                    "qty": "2",
                    "lighter_filled_qty": "2",
                    "trade_key": "close-1",
                    "asset": "BTC",
                    "strategy_tag": "test",
                    "variational_filled_price": "100.08",
                    "lighter_filled_price": "100",
                    "execution_loss_usd": "0.01",
                },
            ]
            metrics.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )
            database = ResearchDatabase(root / "research.sqlite3")
            synchronizer = ResearchDatabaseSynchronizer(
                database,
                [SyncSource(metrics, stream="order_metric")],
            )

            synchronizer.sync_once()

            with database._connect() as connection:
                round_row = connection.execute(
                    """
                    SELECT
                        round_pnl_usd,
                        open_execution_loss_bps,
                        total_execution_loss_bps,
                        effective_quality
                    FROM research_round_quality
                    """
                ).fetchone()
            self.assertIsNotNone(round_row)
            self.assertEqual(round_row[0], "0.36")
            self.assertEqual(round_row[1], "2.50000")
            self.assertEqual(round_row[2], "3.0000")
            self.assertEqual(round_row[3], "suspected_bad_execution")

            database.label_round(
                open_trade_key="open-1",
                close_trade_key="close-1",
                label="bad_execution",
                reason_code="user_confirmed",
                note="人工确认",
            )
            with database._connect() as connection:
                quality = connection.execute(
                    "SELECT effective_quality FROM research_round_quality"
                ).fetchone()[0]
            self.assertEqual(quality, "bad_execution")

            database.label_trade(
                trade_key="open-1",
                phase="open",
                label="bad_execution",
                reason_code="user_confirmed",
                note="人工确认开仓腿",
            )
            with database._connect() as connection:
                trade_label = connection.execute(
                    """
                    SELECT label, source
                    FROM trade_quality_labels
                    WHERE trade_key = 'open-1'
                      AND source = 'manual_user'
                    """
                ).fetchone()
            self.assertEqual(trade_label, ("bad_execution", "manual_user"))

    def test_late_var_fill_replaces_provisional_close_loss(self) -> None:
        with tempfile.TemporaryDirectory(prefix="research-round-late-fill-") as tmp:
            root = Path(tmp)
            metrics = root / "order_metrics.jsonl"
            initial_rows = [
                {
                    "event": "lighter_fill",
                    "logged_at": "2026-01-01T00:00:00+00:00",
                    "strategy_phase": "open",
                    "side": "buy",
                    "qty": "2",
                    "lighter_filled_qty": "2",
                    "trade_key": "open-late",
                    "asset": "BTC",
                    "strategy_tag": "test",
                    "variational_filled_price": "100",
                    "variational_filled_at": "2026-01-01T00:00:00+00:00",
                    "lighter_filled_price": "100.10",
                    "lighter_filled_at": "2026-01-01T00:00:00.100+00:00",
                    "firm_guard_pnl": "0.25",
                    "execution_loss_usd": "0.05",
                },
                {
                    "event": "lighter_fill",
                    "logged_at": "2026-01-01T00:01:00+00:00",
                    "strategy_phase": "close",
                    "side": "sell",
                    "qty": "2",
                    "lighter_filled_qty": "2",
                    "trade_key": "close-late",
                    "asset": "BTC",
                    "strategy_tag": "test",
                    "variational_filled_price": "100.08",
                    "variational_filled_at": "2026-01-01T00:01:00+00:00",
                    "variational_fill_source": "http_commit",
                    "lighter_filled_price": "100",
                    "lighter_filled_at": "2026-01-01T00:01:00.100+00:00",
                    "firm_guard_pnl": "0.20",
                    "execution_loss_usd": None,
                },
            ]
            metrics.write_text(
                "".join(json.dumps(row) + "\n" for row in initial_rows),
                encoding="utf-8",
            )
            database = ResearchDatabase(root / "research.sqlite3")
            synchronizer = ResearchDatabaseSynchronizer(
                database,
                [SyncSource(metrics, stream="order_metric")],
            )

            synchronizer.sync_once()
            with database._connect() as connection:
                provisional = connection.execute(
                    """
                    SELECT close_leg_pnl_usd, close_execution_loss_usd
                    FROM research_rounds
                    """
                ).fetchone()
            self.assertEqual(provisional, ("0.16", "0.04"))

            corrected_close = {
                **initial_rows[1],
                "event": "variational_fill",
                "logged_at": "2026-01-01T00:01:01+00:00",
                "variational_filled_price": "100.04",
                "variational_filled_at": "2026-01-01T00:01:00.050+00:00",
                "variational_fill_source": "event",
                # Deliberately stale: the research row must recompute from the
                # corrected final prices instead of copying this value.
                "execution_loss_usd": "0.04",
            }
            with metrics.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(corrected_close) + "\n")

            synchronizer.sync_once()
            with database._connect() as connection:
                corrected = connection.execute(
                    """
                    SELECT
                        close_leg_pnl_usd,
                        round_pnl_usd,
                        close_execution_loss_usd,
                        total_execution_loss_usd,
                        effective_quality
                    FROM research_round_quality
                    """
                ).fetchone()
                close_label = connection.execute(
                    """
                    SELECT label
                    FROM trade_quality_labels
                    WHERE trade_key = 'close-late'
                      AND source = 'automatic'
                    """
                ).fetchone()
            self.assertEqual(
                corrected,
                ("0.08", "0.28", "0.12", "0.17", "suspected_bad_execution"),
            )
            self.assertEqual(close_label, ("suspected_bad_execution",))


if __name__ == "__main__":
    unittest.main()
