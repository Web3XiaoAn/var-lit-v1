from __future__ import annotations

import asyncio
import contextlib
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from aiohttp import WSMsgType, web


SnapshotFactory = Callable[[], Awaitable[dict[str, Any]]]
ActionPreparer = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]
ActionExecutor = Callable[
    [str, dict[str, Any], dict[str, Any]], Awaitable[dict[str, Any]]
]


@dataclass(slots=True)
class PreparedAction:
    action: str
    payload: dict[str, Any]
    preview: dict[str, Any]
    expires_monotonic: float


@dataclass(slots=True)
class CompletedAction:
    response: dict[str, Any]
    expires_monotonic: float


class OperationsDashboardServer:
    """Loopback-only operations UI with two-step, idempotent commands.

    The browser reaches this listener through an explicit SSH local-forward.
    A process-local CSRF secret prevents unrelated browser origins from
    submitting commands to the forwarded port, while prepare/commit binds a
    destructive action to one short-lived runtime-state preview.
    """

    def __init__(
        self,
        *,
        snapshot_factory: SnapshotFactory,
        action_preparer: ActionPreparer,
        action_executor: ActionExecutor,
        asset_dir: Path,
        host: str = "127.0.0.1",
        port: int = 8780,
        refresh_seconds: float = 0.2,
    ) -> None:
        if host != "127.0.0.1":
            raise ValueError("Operations dashboard must listen on 127.0.0.1")
        if not 1 <= port <= 65535:
            raise ValueError("Operations dashboard port is invalid")
        if not 0.1 <= refresh_seconds <= 5.0:
            raise ValueError("Operations dashboard refresh must be 0.1-5.0s")
        self.snapshot_factory = snapshot_factory
        self.action_preparer = action_preparer
        self.action_executor = action_executor
        self.asset_dir = asset_dir.resolve()
        self.host = host
        self.port = port
        self.refresh_seconds = refresh_seconds
        self._csrf_token = secrets.token_urlsafe(32)
        self._prepared: dict[str, PreparedAction] = {}
        self._completed: dict[str, CompletedAction] = {}
        self._action_lock = asyncio.Lock()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    @property
    def address(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def start(self) -> None:
        if self._runner is not None:
            return
        app = web.Application(client_max_size=16 * 1024)
        app.middlewares.append(self._security_middleware)
        app.router.add_get("/", self._index)
        app.router.add_get("/health", self._health)
        app.router.add_get("/api/state", self._state)
        app.router.add_get("/api/stream", self._stream)
        app.router.add_post("/api/actions/prepare", self._prepare_action)
        app.router.add_post("/api/actions/commit", self._commit_action)
        runner = web.AppRunner(
            app,
            access_log=None,
            handle_signals=False,
            shutdown_timeout=2.0,
        )
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        try:
            await site.start()
        except Exception:
            await runner.cleanup()
            raise
        self._runner = runner
        self._site = site

    async def stop(self) -> None:
        runner = self._runner
        self._runner = None
        self._site = None
        self._prepared.clear()
        self._completed.clear()
        if runner is not None:
            await runner.cleanup()

    @web.middleware
    async def _security_middleware(
        self,
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        if not self._loopback_host(request.host):
            raise web.HTTPForbidden(text="loopback host required")
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            origin = request.headers.get("Origin", "")
            if not self._loopback_origin(origin):
                raise web.HTTPForbidden(text="loopback origin required")
            if request.headers.get("X-Var-Lit-CSRF") != self._csrf_token:
                raise web.HTTPForbidden(text="invalid dashboard session")
            if request.content_type != "application/json":
                raise web.HTTPUnsupportedMediaType(text="JSON required")
        response = await handler(request)
        if not response.prepared:
            self._set_security_headers(response)
        return response

    @staticmethod
    def _set_security_headers(response: web.StreamResponse) -> None:
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"

    @staticmethod
    def _loopback_host(value: str) -> bool:
        host = value.rsplit("@", 1)[-1]
        if host.startswith("["):
            host = host.split("]", 1)[0] + "]"
        else:
            host = host.split(":", 1)[0]
        return host.lower() in {"127.0.0.1", "localhost", "[::1]"}

    @classmethod
    def _loopback_origin(cls, value: str) -> bool:
        try:
            parsed = urlparse(value)
        except ValueError:
            return False
        return parsed.scheme in {"http", "https"} and cls._loopback_host(
            parsed.netloc
        )

    async def _index(self, _request: web.Request) -> web.Response:
        source = (self.asset_dir / "index.html").read_text(encoding="utf-8")
        nonce = secrets.token_urlsafe(18)
        source = source.replace("__VAR_LIT_CSRF__", self._csrf_token)
        source = source.replace("__VAR_LIT_CSP_NONCE__", nonce)
        response = web.Response(text=source, content_type="text/html")
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; "
            f"style-src 'nonce-{nonce}'; script-src 'nonce-{nonce}'; "
            "connect-src 'self' ws://127.0.0.1:* ws://localhost:*; "
            "img-src 'self' data:; base-uri 'none'; form-action 'self'; "
            "frame-ancestors 'none'"
        )
        return response

    async def _health(self, _request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "service": "var-lit-v1-operations-dashboard",
                "listener": f"{self.host}:{self.port}",
            }
        )

    async def _state(self, _request: web.Request) -> web.Response:
        return web.json_response(await self.snapshot_factory())

    async def _stream(self, request: web.Request) -> web.WebSocketResponse:
        if request.query.get("csrf") != self._csrf_token:
            raise web.HTTPForbidden(text="invalid dashboard session")
        origin = request.headers.get("Origin", "")
        if not self._loopback_origin(origin):
            raise web.HTTPForbidden(text="loopback origin required")
        ws = web.WebSocketResponse(
            heartbeat=20.0,
            receive_timeout=None,
            max_msg_size=4 * 1024,
            compress=False,
        )
        self._set_security_headers(ws)
        await ws.prepare(request)
        try:
            while not ws.closed:
                snapshot = await self.snapshot_factory()
                await ws.send_json(snapshot)
                try:
                    message = await asyncio.wait_for(
                        ws.receive(), timeout=self.refresh_seconds
                    )
                except asyncio.TimeoutError:
                    continue
                if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    break
        except (ConnectionResetError, RuntimeError):
            pass
        finally:
            with contextlib.suppress(Exception):
                await ws.close()
        return ws

    async def _json_body(self, request: web.Request) -> dict[str, Any]:
        try:
            payload = await request.json()
        except Exception as exc:
            raise web.HTTPBadRequest(text="invalid JSON") from exc
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        return payload

    def _prune_confirmations(self) -> None:
        now = time.monotonic()
        self._prepared = {
            key: value
            for key, value in self._prepared.items()
            if value.expires_monotonic > now
        }
        self._completed = {
            key: value
            for key, value in self._completed.items()
            if value.expires_monotonic > now
        }

    async def _prepare_action(self, request: web.Request) -> web.Response:
        body = await self._json_body(request)
        action = str(body.get("action") or "").strip()
        payload = body.get("payload") or {}
        if not action or not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="action and payload are required")
        self._prune_confirmations()
        preview = await self.action_preparer(action, payload)
        allowed = preview.get("allowed") is True
        response: dict[str, Any] = {"ok": allowed, "preview": preview}
        if allowed:
            confirmation_id = secrets.token_urlsafe(24)
            # Give an operator enough time to read the authoritative facts on
            # a remote screen without leaving the command valid indefinitely.
            expires_seconds = 60
            self._prepared[confirmation_id] = PreparedAction(
                action=action,
                payload=dict(payload),
                preview=dict(preview),
                expires_monotonic=time.monotonic() + expires_seconds,
            )
            response.update(
                {
                    "confirmationId": confirmation_id,
                    "expiresSeconds": expires_seconds,
                }
            )
        return web.json_response(response, status=200 if allowed else 409)

    async def _commit_action(self, request: web.Request) -> web.Response:
        body = await self._json_body(request)
        confirmation_id = str(body.get("confirmationId") or "").strip()
        if not confirmation_id:
            raise web.HTTPBadRequest(text="confirmationId is required")
        self._prune_confirmations()
        async with self._action_lock:
            completed = self._completed.get(confirmation_id)
            if completed is not None:
                return web.json_response(completed.response)
            prepared = self._prepared.pop(confirmation_id, None)
            if prepared is None:
                return web.json_response(
                    {"ok": False, "error": "确认已过期，请重新检查当前状态"},
                    status=409,
                )
            try:
                result = await self.action_executor(
                    prepared.action,
                    prepared.payload,
                    prepared.preview,
                )
            except Exception as exc:
                result = {"ok": False, "error": str(exc)}
            response = dict(result)
            response.setdefault("ok", False)
            self._completed[confirmation_id] = CompletedAction(
                response=response,
                expires_monotonic=time.monotonic() + 60,
            )
        return web.json_response(response, status=200 if response["ok"] else 409)
