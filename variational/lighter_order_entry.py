"""Single-writer WebSocket transaction entry for Lighter.

The market-data and private-account streams deliberately do not share this
connection.  A transaction submission is not a fill confirmation: callers
must continue to reconcile through the private stream/REST before treating an
order as filled.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Callable

import websockets


class LighterOrderEntryUnavailable(RuntimeError):
    """No frame was sent, so a caller may safely use its REST fallback."""


class LighterOrderEntryUnknown(RuntimeError):
    """A frame may have reached the exchange; reconcile before any retry."""


@dataclass(frozen=True, slots=True)
class LighterOrderEntryReceipt:
    code: int | str | None
    message: str | None
    tx_hash: str | None
    raw: dict[str, Any]
    queue_wait_ns: int
    round_trip_ns: int
    send_monotonic_ns: int
    response_monotonic_ns: int


@dataclass(slots=True)
class _QueuedTransaction:
    tx_type: int | str
    tx_info: str
    tx_hash: str
    request_id: str
    future: asyncio.Future[LighterOrderEntryReceipt]
    enqueued_monotonic_ns: int
    dispatched: bool = False


class LighterOrderEntry:
    """Serialize signed transactions over one persistent WebSocket.

    Only one command is in flight.  That gives the signer one nonce owner and
    makes response-to-command association deterministic even when the server
    also emits pings or a connection greeting.
    """

    def __init__(
        self,
        url: str,
        *,
        ping_interval: float = 30.0,
        ping_timeout: float = 30.0,
        response_timeout: float = 5.0,
        max_queue_size: int = 64,
        reconnect_delay: float = 0.25,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self.url = url
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.response_timeout = response_timeout
        self.reconnect_delay = reconnect_delay
        self._connect = connect or websockets.connect
        self._queue: asyncio.Queue[_QueuedTransaction | None] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._worker: asyncio.Task[None] | None = None
        self._maintenance: asyncio.Task[None] | None = None
        self._ws: Any | None = None
        self._connect_lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._closed = False

    @property
    def is_ready(self) -> bool:
        return (
            self._ready.is_set()
            and self._ws is not None
            and not self._socket_looks_closed(self._ws)
        )

    @staticmethod
    def _socket_looks_closed(ws: Any) -> bool:
        """Recognize a connection closed by the library's ping task.

        websockets may mark the protocol closed without invoking this wrapper.
        Treating a non-None object as ready would let the Var Commit proceed
        before the Lighter hedge discovers the dead transport.
        """

        if bool(getattr(ws, "closed", False)):
            return True
        state = getattr(ws, "state", None)
        state_name = str(getattr(state, "name", state) or "").upper()
        return state_name in {"CLOSING", "CLOSED"}

    async def start(self) -> None:
        """Prewarm the dedicated connection without placing an order."""
        if self._closed:
            raise LighterOrderEntryUnavailable("Lighter order-entry WebSocket is closed")
        if self._maintenance is None or self._maintenance.done():
            self._maintenance = asyncio.create_task(
                self._maintain_connection(), name="lighter-order-entry-maintenance"
            )
        await self._ensure_connected()

    async def submit(
        self,
        *,
        tx_type: int | str,
        tx_info: str,
        tx_hash: str,
        request_id: str,
    ) -> LighterOrderEntryReceipt:
        if self._closed:
            raise LighterOrderEntryUnavailable("Lighter order-entry WebSocket is closed")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[LighterOrderEntryReceipt] = loop.create_future()
        try:
            self._queue.put_nowait(
                _QueuedTransaction(
                    tx_type,
                    tx_info,
                    tx_hash,
                    request_id,
                    future,
                    time.monotonic_ns(),
                )
            )
        except asyncio.QueueFull as exc:
            raise LighterOrderEntryUnavailable("Lighter order-entry queue is full") from exc
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run(), name="lighter-order-entry")
        return await future

    async def close(self) -> None:
        self._closed = True
        if self._worker is not None and not self._worker.done():
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
        if self._maintenance is not None and not self._maintenance.done():
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
        await self._close_socket()
        while True:
            try:
                queued = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if queued is not None and not queued.future.done():
                    queued.future.set_exception(
                        LighterOrderEntryUnavailable(
                            "Lighter order-entry WebSocket is closed"
                        )
                    )
            finally:
                self._queue.task_done()

    async def _run(self) -> None:
        try:
            while not self._closed:
                command = await self._queue.get()
                try:
                    if command is None:
                        return
                    if command.future.cancelled():
                        continue
                    try:
                        receipt = await self._submit_one(command)
                    except asyncio.CancelledError:
                        if not command.future.done():
                            error_type = (
                                LighterOrderEntryUnknown
                                if command.dispatched
                                else LighterOrderEntryUnavailable
                            )
                            command.future.set_exception(
                                error_type(
                                    "Lighter order-entry WebSocket closed during submission"
                                )
                            )
                        raise
                    except Exception as exc:
                        if not command.future.done():
                            command.future.set_exception(exc)
                    else:
                        if not command.future.done():
                            command.future.set_result(receipt)
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            await self._close_socket()

    async def _ensure_connected(self) -> Any:
        async with self._connect_lock:
            if self._ws is not None and not self._socket_looks_closed(self._ws):
                return self._ws
            self._ws = None
            self._ready.clear()
            try:
                self._ws = await self._connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                )
            except Exception as exc:
                self._ready.clear()
                raise LighterOrderEntryUnavailable(
                    f"Lighter order-entry connect failed: {exc}"
                ) from exc
            self._ready.set()
            return self._ws

    async def _maintain_connection(self) -> None:
        while not self._closed:
            if not self.is_ready:
                try:
                    await self._ensure_connected()
                except LighterOrderEntryUnavailable:
                    await asyncio.sleep(self.reconnect_delay)
                    continue
            await asyncio.sleep(self.reconnect_delay)

    async def _submit_one(self, command: _QueuedTransaction) -> LighterOrderEntryReceipt:
        ws = await self._ensure_connected()
        frame = {
            "type": "jsonapi/sendtx",
            "data": {
                "id": command.request_id,
                "tx_type": command.tx_type,
                "tx_info": json.loads(command.tx_info),
            },
        }
        send_started_ns = time.monotonic_ns()
        # Cancellation or connection loss after this point has no delivery
        # guarantee.  Mark it before awaiting send so shutdown cannot classify
        # an in-flight frame as safe for REST fallback.
        command.dispatched = True
        try:
            await ws.send(json.dumps(frame, separators=(",", ":")))
        except Exception as exc:
            # A failed WebSocket write has no delivery guarantee.
            await self._close_socket()
            raise LighterOrderEntryUnknown(
                f"Lighter order-entry send outcome is unknown: {exc}"
            ) from exc

        deadline = asyncio.get_running_loop().time() + self.response_timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                await self._close_socket()
                raise LighterOrderEntryUnknown("Lighter order-entry response timed out")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except Exception as exc:
                await self._close_socket()
                raise LighterOrderEntryUnknown(
                    f"Lighter order-entry receive outcome is unknown: {exc}"
                ) from exc
            response_received_ns = time.monotonic_ns()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                data = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            if data.get("type") == "ping":
                await ws.send(json.dumps({"type": "pong"}))
                continue
            receipt = self._receipt_for(
                command,
                data,
                queue_wait_ns=send_started_ns - command.enqueued_monotonic_ns,
                round_trip_ns=response_received_ns - send_started_ns,
                send_monotonic_ns=send_started_ns,
                response_monotonic_ns=response_received_ns,
            )
            if receipt is not None:
                return receipt

    @staticmethod
    def _receipt_for(
        command: _QueuedTransaction,
        data: dict[str, Any],
        *,
        queue_wait_ns: int,
        round_trip_ns: int,
        send_monotonic_ns: int,
        response_monotonic_ns: int,
    ) -> LighterOrderEntryReceipt | None:
        payload = data.get("data") if isinstance(data.get("data"), dict) else data
        response_id = payload.get("id") or data.get("id")
        response_type = str(data.get("type") or "")
        # Lighter's WebSocket examples associate sendTx responses with data.id.
        # Accept a bare sendTx reply too: only the single writer has a request
        # in flight, so it cannot be confused with another order.
        if response_id is not None and str(response_id) != command.request_id:
            return None
        if response_id is None and "sendtx" not in response_type.lower():
            return None
        code = payload.get("code", data.get("code"))
        message = payload.get("message", data.get("message"))
        tx_hash = payload.get("tx_hash", payload.get("txHash", command.tx_hash))
        return LighterOrderEntryReceipt(
            code,
            message,
            tx_hash,
            data,
            queue_wait_ns,
            round_trip_ns,
            send_monotonic_ns,
            response_monotonic_ns,
        )

    async def _close_socket(self) -> None:
        async with self._connect_lock:
            ws, self._ws = self._ws, None
            self._ready.clear()
        if ws is None:
            return
        close = getattr(ws, "close", None)
        if callable(close):
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass
