import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from variational.listener import (
    COMMAND_EXTENSION_BUILD,
    COMMAND_PROTOCOL_VERSION,
    CommandBroker,
    run_receiver_server,
    valid_chrome_extension_origin,
)

def registration_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "REGISTER",
        "role": "extension",
        "protocolVersion": COMMAND_PROTOCOL_VERSION,
        "build": COMMAND_EXTENSION_BUILD,
    }
    payload.update(overrides)
    return payload


class FakeWebSocket:
    def __init__(self, messages: list[str] | None = None) -> None:
        self.messages = list(messages or [])
        self.sent: list[dict[str, object]] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    def __aiter__(self):
        async def messages():
            for message in self.messages:
                yield message

        return messages()


class FailingWebSocket(FakeWebSocket):
    async def send(self, raw: str) -> None:
        raise ConnectionError("closed")


class ListenerSecurityTests(unittest.IsolatedAsyncioTestCase):
    def test_chrome_extension_origin_is_exact(self) -> None:
        extension_id = "abcdefghijklmnopabcdefghijklmnop"
        self.assertTrue(valid_chrome_extension_origin(f"chrome-extension://{extension_id}"))
        for origin in (
            "",
            "null",
            f"http://{extension_id}",
            f"chrome-extension://{extension_id}/",
            f"chrome-extension://{extension_id}?spoof=1",
            f"chrome-extension://{extension_id}.example",
            "chrome-extension://abcdefghijklmnopabcdefghijklmn0p",
            "chrome-extension://ABCDEFGHIJKLMNOPABCDEFGHIJKLMNOP",
        ):
            with self.subTest(origin=origin):
                self.assertFalse(valid_chrome_extension_origin(origin))

    async def test_registration_requires_exact_role_protocol_and_build(self) -> None:
        invalid_payloads = (
            registration_payload(role="controller"),
            registration_payload(protocolVersion=""),
            registration_payload(protocolVersion="old-command-protocol"),
            registration_payload(build=""),
            registration_payload(build="old-extension-build"),
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                broker = CommandBroker(quiet=True)
                websocket = FakeWebSocket()
                await broker.on_connect(websocket)
                await broker.handle_raw_message(websocket, json.dumps(payload))
                self.assertFalse(await broker.extension_connected())
                self.assertEqual(websocket.sent[-1]["type"], "REGISTER_ACK")
                self.assertIs(websocket.sent[-1]["ok"], False)

        broker = CommandBroker(quiet=True)
        extension = FakeWebSocket()
        duplicate = FakeWebSocket()
        await broker.on_connect(extension)
        await broker.on_connect(duplicate)
        await broker.handle_raw_message(extension, json.dumps(registration_payload()))
        await broker.handle_raw_message(duplicate, json.dumps(registration_payload()))

        self.assertTrue(await broker.extension_connected())
        self.assertIs(extension.sent[-1]["ok"], True)
        self.assertEqual(extension.sent[-1]["role"], "extension")
        self.assertEqual(extension.sent[-1]["protocolVersion"], COMMAND_PROTOCOL_VERSION)
        self.assertEqual(extension.sent[-1]["build"], COMMAND_EXTENSION_BUILD)
        self.assertIs(duplicate.sent[-1]["ok"], False)
        self.assertIn("already registered", str(duplicate.sent[-1]["error"]))

    async def test_registration_needs_no_manual_secret(self) -> None:
        for payload in (
            registration_payload(),
            registration_payload(authToken="ignored-legacy-value"),
        ):
            with self.subTest(payload=payload):
                broker = CommandBroker(quiet=True)
                websocket = FakeWebSocket()
                await broker.on_connect(websocket)
                await broker.handle_raw_message(websocket, json.dumps(payload))
                self.assertTrue(await broker.extension_connected())
                self.assertIs(websocket.sent[-1]["ok"], True)

    async def test_websocket_place_order_is_never_forwarded(self) -> None:
        broker = CommandBroker(quiet=True)
        extension = FakeWebSocket()
        await broker.on_connect(extension)
        await broker.handle_raw_message(extension, json.dumps(registration_payload()))
        extension.sent.clear()

        await broker.handle_raw_message(
            extension,
            json.dumps(
                {
                    "type": "PLACE_ORDER",
                    "requestId": "external-order",
                    "side": "BUY",
                    "amount": "200",
                    "fetchStage": "commit",
                }
            ),
        )

        self.assertEqual(len(extension.sent), 1)
        self.assertEqual(extension.sent[0]["type"], "ORDER_RESULT")
        self.assertIs(extension.sent[0]["ok"], False)
        self.assertIn("cannot submit orders", str(extension.sent[0]["error"]))

    async def test_only_registered_extension_can_complete_internal_order(self) -> None:
        broker = CommandBroker(quiet=True)
        extension = FakeWebSocket()
        attacker = FakeWebSocket()
        await broker.on_connect(extension)
        await broker.on_connect(attacker)
        await broker.handle_raw_message(extension, json.dumps(registration_payload()))
        extension.sent.clear()

        pending = asyncio.create_task(
            broker.request_place_order(
                side="BUY",
                amount="200",
                timeout_ms=1000,
                fetch_stage="quote",
            )
        )
        await asyncio.sleep(0)
        place_order = extension.sent[-1]
        request_id = str(place_order["requestId"])

        await broker.handle_raw_message(
            attacker,
            json.dumps(
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": True,
                }
            ),
        )
        self.assertFalse(pending.done())
        self.assertEqual(attacker.sent[-1]["type"], "ERROR")

        await broker.handle_raw_message(
            extension,
            json.dumps(
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": True,
                }
            ),
        )
        result = await pending
        self.assertIs(result["ok"], True)

    async def test_internal_order_rejects_missing_unknown_and_full_stage(self) -> None:
        broker = CommandBroker(quiet=True)
        for stage in (None, "unknown", "full"):
            with self.subTest(stage=stage):
                result = await broker.request_place_order(
                    side="BUY",
                    amount="200",
                    fetch_stage=stage,
                )
                self.assertIs(result["ok"], False)
                self.assertIn("quote or commit", str(result["error"]))

    async def test_failed_send_returns_immediately_and_cleans_pending_request(self) -> None:
        broker = CommandBroker(quiet=True)
        extension = FailingWebSocket()
        broker._extension = extension
        broker._roles[extension] = "extension"

        result = await broker.request_place_order(
            side="BUY",
            amount="200",
            timeout_ms=60_000,
            fetch_stage="commit",
        )

        self.assertIs(result["ok"], False)
        self.assertIn("disconnected", str(result["error"]).lower())
        self.assertEqual(broker._pending_futures, {})

    async def test_cancelled_request_cleans_pending_request(self) -> None:
        broker = CommandBroker(quiet=True)
        extension = FakeWebSocket()
        broker._extension = extension
        broker._roles[extension] = "extension"

        pending = asyncio.create_task(
            broker.request_place_order(
                side="BUY",
                amount="200",
                timeout_ms=60_000,
                fetch_stage="commit",
            )
        )
        await asyncio.sleep(0)
        self.assertEqual(len(broker._pending_futures), 1)

        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.assertEqual(broker._pending_futures, {})

    async def test_receiver_with_command_bridge_disabled_only_sends_to_sink(self) -> None:
        broker = CommandBroker(quiet=True)
        broker.handle_raw_message = AsyncMock()  # type: ignore[method-assign]
        sink = AsyncMock()
        websocket = FakeWebSocket(
            [
                json.dumps(registration_payload()),
                json.dumps(
                    {
                        "type": "PLACE_ORDER",
                        "requestId": "must-be-sampled",
                        "side": "BUY",
                        "amount": "999999",
                        "fetchStage": "commit",
                    }
                ),
            ]
        )
        captured: dict[str, object] = {}

        async def fake_serve(handler, host, port, **kwargs):
            captured["handler"] = handler
            captured["host"] = host
            captured["port"] = port
            return object()

        with patch("variational.listener.websockets.serve", side_effect=fake_serve):
            await run_receiver_server(
                "ws",
                "127.0.0.1",
                8766,
                sink,
                command_broker=broker,
                command_bridge=False,
            )

        handler = captured["handler"]
        await handler(websocket)  # type: ignore[operator]

        broker.handle_raw_message.assert_not_awaited()  # type: ignore[attr-defined]
        self.assertEqual(sink.handle.await_count, 2)
        self.assertEqual(sink.handle.await_args_list[0].args[0], "ws")
        self.assertEqual(sink.handle.await_args_list[1].args[0], "ws")


if __name__ == "__main__":
    unittest.main()
