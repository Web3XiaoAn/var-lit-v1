from __future__ import annotations

import json
import unittest

import websockets

from tools.probe_extension import ExtensionProbe
from variational.listener import COMMAND_EXTENSION_BUILD, COMMAND_PROTOCOL_VERSION


class ExtensionProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_probe_accepts_only_exact_registration(self) -> None:
        probe = ExtensionProbe()
        async with websockets.serve(probe.handle_command, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://127.0.0.1:{port}") as connection:
                await connection.send(
                    json.dumps(
                        {
                            "type": "REGISTER",
                            "role": "extension",
                            "protocolVersion": COMMAND_PROTOCOL_VERSION,
                            "build": COMMAND_EXTENSION_BUILD,
                        }
                    )
                )
                response = json.loads(await connection.recv())
                self.assertTrue(response["ok"])
        report = probe.state.report()
        self.assertTrue(report["command_registered"])
        self.assertEqual(report["orders_sent"], 0)

    async def test_stream_probe_counts_telemetry_without_responding(self) -> None:
        probe = ExtensionProbe()
        handler = lambda connection: probe.handle_stream(connection, "websocket")
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://127.0.0.1:{port}") as connection:
                await connection.send('{"kind":"ws_frame"}')
        self.assertEqual(probe.state.messages["websocket"], 1)
        self.assertGreater(probe.state.bytes_received["websocket"], 0)


if __name__ == "__main__":
    unittest.main()
