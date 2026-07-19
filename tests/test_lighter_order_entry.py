import asyncio
import json
import unittest

from variational.lighter_order_entry import (
    LighterOrderEntry,
    LighterOrderEntryUnavailable,
    LighterOrderEntryUnknown,
)


class FakeWebSocket:
    def __init__(self, responses):
        self.responses = asyncio.Queue()
        for response in responses:
            self.responses.put_nowait(response)
        self.sent = []
        self.closed = False

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    async def recv(self):
        return await self.responses.get()

    async def close(self):
        self.closed = True


class LighterOrderEntryTests(unittest.TestCase):
    def test_single_writer_serializes_and_matches_each_response(self):
        async def run_case():
            ws = FakeWebSocket(
                [
                    json.dumps({"type": "connected"}),
                    json.dumps({"type": "jsonapi/sendtx", "data": {"id": "one", "code": 0}}),
                    json.dumps({"type": "jsonapi/sendtx", "data": {"id": "two", "code": 200}}),
                ]
            )

            async def connect(*_args, **_kwargs):
                return ws

            entry = LighterOrderEntry("wss://test", connect=connect)
            first = asyncio.create_task(
                entry.submit(tx_type=1, tx_info='{"a":1}', tx_hash="hash-1", request_id="one")
            )
            second = asyncio.create_task(
                entry.submit(tx_type=1, tx_info='{"a":2}', tx_hash="hash-2", request_id="two")
            )
            one, two = await asyncio.gather(first, second)
            self.assertEqual(one.code, 0)
            self.assertEqual(two.code, 200)
            self.assertGreaterEqual(one.queue_wait_ns, 0)
            self.assertGreaterEqual(one.round_trip_ns, 0)
            self.assertGreater(one.send_monotonic_ns, 0)
            self.assertGreaterEqual(
                one.response_monotonic_ns, one.send_monotonic_ns
            )
            self.assertEqual([item["data"]["id"] for item in ws.sent], ["one", "two"])
            await asyncio.wait_for(entry._queue.join(), timeout=0.1)
            await entry.close()

        asyncio.run(run_case())

    def test_start_prewarms_dedicated_connection_before_submit(self):
        async def run_case():
            ws = FakeWebSocket([])
            connect_calls = 0

            async def connect(*_args, **_kwargs):
                nonlocal connect_calls
                connect_calls += 1
                return ws

            entry = LighterOrderEntry("wss://test", connect=connect)
            await entry.start()
            self.assertTrue(entry.is_ready)
            self.assertEqual(connect_calls, 1)
            await entry.close()
            self.assertTrue(ws.closed)

        asyncio.run(run_case())

    def test_background_maintenance_reconnects_after_startup_failure(self):
        async def run_case():
            ws = FakeWebSocket([])
            accepting_connections = False

            async def connect(*_args, **_kwargs):
                if not accepting_connections:
                    raise OSError("offline")
                return ws

            entry = LighterOrderEntry(
                "wss://test", connect=connect, reconnect_delay=0.001
            )
            with self.assertRaises(LighterOrderEntryUnavailable):
                await entry.start()
            accepting_connections = True
            for _ in range(50):
                if entry.is_ready:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(entry.is_ready)
            await entry.close()

        asyncio.run(run_case())

    def test_background_maintenance_replaces_library_closed_socket(self):
        async def run_case():
            first = FakeWebSocket([])
            second = FakeWebSocket([])
            sockets = [first, second]

            async def connect(*_args, **_kwargs):
                return sockets.pop(0)

            entry = LighterOrderEntry(
                "wss://test", connect=connect, reconnect_delay=0.001
            )
            await entry.start()
            self.assertTrue(entry.is_ready)

            # Simulate websockets' own ping task closing the protocol without
            # going through LighterOrderEntry._close_socket().
            first.closed = True
            self.assertFalse(entry.is_ready)
            for _ in range(50):
                if entry.is_ready and entry._ws is second:
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(entry.is_ready)
            self.assertIs(entry._ws, second)
            await entry.close()

        asyncio.run(run_case())

    def test_connect_failure_is_safe_rest_fallback_condition(self):
        async def run_case():
            async def connect(*_args, **_kwargs):
                raise OSError("offline")

            entry = LighterOrderEntry("wss://test", connect=connect)
            with self.assertRaises(LighterOrderEntryUnavailable):
                await entry.submit(tx_type=1, tx_info="{}", tx_hash="hash", request_id="one")
            await entry.close()

        asyncio.run(run_case())

    def test_response_timeout_is_unknown_not_a_safe_retry(self):
        async def run_case():
            ws = FakeWebSocket([])

            async def connect(*_args, **_kwargs):
                return ws

            entry = LighterOrderEntry(
                "wss://test", connect=connect, response_timeout=0.01
            )
            with self.assertRaises(LighterOrderEntryUnknown):
                await entry.submit(tx_type=1, tx_info="{}", tx_hash="hash", request_id="one")
            self.assertTrue(ws.closed)
            await entry.close()

        asyncio.run(run_case())

    def test_close_resolves_submission_already_waiting_for_response(self):
        async def run_case():
            ws = FakeWebSocket([])

            async def connect(*_args, **_kwargs):
                return ws

            entry = LighterOrderEntry("wss://test", connect=connect)
            pending = asyncio.create_task(
                entry.submit(
                    tx_type=1,
                    tx_info="{}",
                    tx_hash="hash",
                    request_id="in-flight",
                )
            )
            for _ in range(50):
                if ws.sent:
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(len(ws.sent), 1)

            await entry.close()
            with self.assertRaises(LighterOrderEntryUnknown):
                await asyncio.wait_for(pending, timeout=0.1)
            await asyncio.wait_for(entry._queue.join(), timeout=0.1)

        asyncio.run(run_case())

    def test_mismatched_sendtx_id_is_ignored_until_matching_receipt_arrives(self):
        async def run_case():
            ws = FakeWebSocket(
                [
                    json.dumps(
                        {
                            "type": "jsonapi/sendtx",
                            "data": {"id": "another-order", "code": 500},
                        }
                    ),
                    json.dumps(
                        {
                            "type": "jsonapi/sendtx",
                            "data": {"id": "expected-order", "code": 0},
                        }
                    ),
                ]
            )

            async def connect(*_args, **_kwargs):
                return ws

            entry = LighterOrderEntry("wss://test", connect=connect)
            receipt = await entry.submit(
                tx_type=1,
                tx_info="{}",
                tx_hash="hash",
                request_id="expected-order",
            )
            self.assertEqual(receipt.code, 0)
            self.assertEqual(receipt.raw["data"]["id"], "expected-order")
            await entry.close()

        asyncio.run(run_case())
