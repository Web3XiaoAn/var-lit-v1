#!/usr/bin/env python3
"""Run a no-order local probe for the Variational Chrome extension.

The probe binds the three localhost forwarder ports, accepts market telemetry,
and completes only the fixed extension registration handshake. It never emits
a PLACE_ORDER command and therefore cannot submit or simulate an order.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import websockets


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from variational.listener import (  # noqa: E402
    COMMAND_EXTENSION_BUILD,
    COMMAND_PROTOCOL_VERSION,
)


@dataclass(slots=True)
class ProbeState:
    started_monotonic: float = field(default_factory=time.monotonic)
    connections: dict[str, int] = field(
        default_factory=lambda: {"websocket": 0, "rest": 0, "command": 0}
    )
    messages: dict[str, int] = field(
        default_factory=lambda: {"websocket": 0, "rest": 0, "command": 0}
    )
    bytes_received: dict[str, int] = field(
        default_factory=lambda: {"websocket": 0, "rest": 0, "command": 0}
    )
    command_registered: bool = False
    command_build: str | None = None
    command_protocol: str | None = None
    last_error: str | None = None

    def record(self, channel: str, raw: str | bytes) -> None:
        self.messages[channel] += 1
        self.bytes_received[channel] += len(raw)

    def report(self) -> dict[str, Any]:
        elapsed = max(0.001, time.monotonic() - self.started_monotonic)
        return {
            "schema": "variational-extension-probe-v1",
            "duration_seconds": round(elapsed, 3),
            "expected_build": COMMAND_EXTENSION_BUILD,
            "expected_protocol": COMMAND_PROTOCOL_VERSION,
            "connections": dict(self.connections),
            "messages": dict(self.messages),
            "bytes_received": dict(self.bytes_received),
            "messages_per_second": {
                channel: round(count / elapsed, 3)
                for channel, count in self.messages.items()
            },
            "command_registered": self.command_registered,
            "command_build": self.command_build,
            "command_protocol": self.command_protocol,
            "last_error": self.last_error,
            "ready": (
                self.connections["websocket"] > 0
                and self.connections["rest"] > 0
                and self.command_registered
            ),
            "orders_sent": 0,
        }


class ExtensionProbe:
    def __init__(self) -> None:
        self.state = ProbeState()

    async def handle_stream(self, connection: Any, channel: str) -> None:
        self.state.connections[channel] += 1
        try:
            async for raw in connection:
                self.state.record(channel, raw)
        except websockets.ConnectionClosed:
            return

    async def handle_command(self, connection: Any) -> None:
        self.state.connections["command"] += 1
        try:
            raw = await asyncio.wait_for(connection.recv(), timeout=5.0)
            self.state.record("command", raw)
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                self.state.last_error = "command registration was not valid JSON"
                await connection.close(code=1008, reason="invalid registration")
                return
            self.state.command_build = str(payload.get("build") or "")
            self.state.command_protocol = str(payload.get("protocolVersion") or "")
            valid = (
                payload.get("type") == "REGISTER"
                and payload.get("role") == "extension"
                and self.state.command_build == COMMAND_EXTENSION_BUILD
                and self.state.command_protocol == COMMAND_PROTOCOL_VERSION
            )
            if not valid:
                self.state.last_error = "extension build or command protocol mismatch"
                await connection.send(
                    json.dumps(
                        {
                            "type": "REGISTER_ACK",
                            "ok": False,
                            "role": "extension",
                            "protocolVersion": COMMAND_PROTOCOL_VERSION,
                            "build": COMMAND_EXTENSION_BUILD,
                        }
                    )
                )
                await connection.close(code=1008, reason="build mismatch")
                return
            self.state.command_registered = True
            await connection.send(
                json.dumps(
                    {
                        "type": "REGISTER_ACK",
                        "ok": True,
                        "role": "extension",
                        "protocolVersion": COMMAND_PROTOCOL_VERSION,
                        "build": COMMAND_EXTENSION_BUILD,
                    }
                )
            )
            async for unexpected in connection:
                # The probe never sends commands. Count any extension message
                # for diagnostics without interpreting it as an order result.
                self.state.record("command", unexpected)
        except asyncio.TimeoutError:
            self.state.last_error = "extension registration timed out"
        except websockets.ConnectionClosed:
            return


async def run_probe(
    *,
    host: str,
    websocket_port: int,
    rest_port: int,
    command_port: int,
    duration_seconds: float,
) -> dict[str, Any]:
    probe = ExtensionProbe()
    try:
        async with (
            websockets.serve(
                lambda connection: probe.handle_stream(connection, "websocket"),
                host,
                websocket_port,
            ),
            websockets.serve(
                lambda connection: probe.handle_stream(connection, "rest"),
                host,
                rest_port,
            ),
            websockets.serve(probe.handle_command, host, command_port),
        ):
            await asyncio.sleep(duration_seconds)
    except OSError as exc:
        raise RuntimeError(
            "forwarder port is already in use; stop main.py before running the probe"
        ) from exc
    return probe.state.report()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--websocket-port", type=int, default=8766)
    parser.add_argument("--rest-port", type=int, default=8767)
    parser.add_argument("--command-port", type=int, default=8768)
    parser.add_argument("--duration", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.duration <= 0:
        print("duration must be positive", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(
            run_probe(
                host=args.host,
                websocket_port=args.websocket_port,
                rest_port=args.rest_port,
                command_port=args.command_port,
                duration_seconds=args.duration,
            )
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
