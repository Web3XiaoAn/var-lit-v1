from __future__ import annotations

import asyncio
import re
import socket
import unittest
from pathlib import Path

from aiohttp import ClientSession

from variational.operations_dashboard import OperationsDashboardServer


PROJECT_DIR = Path(__file__).resolve().parents[1]


def unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


class OperationsDashboardServerTests(unittest.TestCase):
    def test_loopback_only_listener_and_idempotent_two_step_action(self) -> None:
        async def run_case() -> None:
            executions: list[tuple[str, dict, dict]] = []

            async def snapshot() -> dict:
                return {"sequence": 1, "health": {"runtimeActive": True}}

            async def prepare(action: str, payload: dict) -> dict:
                return {
                    "allowed": action == "pause_open",
                    "guard": "guard-1",
                    "message": "preview",
                    "facts": {"payload": payload.get("value", "-")},
                }

            async def execute(action: str, payload: dict, preview: dict) -> dict:
                executions.append((action, payload, preview))
                return {"ok": True, "message": "done"}

            port = unused_loopback_port()
            server = OperationsDashboardServer(
                snapshot_factory=snapshot,
                action_preparer=prepare,
                action_executor=execute,
                asset_dir=PROJECT_DIR / "dashboard",
                port=port,
            )
            await server.start()
            try:
                base = f"http://127.0.0.1:{port}"
                async with ClientSession() as client:
                    index = await client.get(base + "/")
                    self.assertEqual(index.status, 200)
                    source = await index.text()
                    csrf_match = re.search(
                        r'<meta name="var-lit-csrf" content="([^"]+)">',
                        source,
                    )
                    self.assertIsNotNone(csrf_match)
                    assert csrf_match is not None
                    csrf = csrf_match.group(1)
                    self.assertNotIn("__VAR_LIT_CSRF__", source)
                    self.assertIn("frame-ancestors 'none'", index.headers["Content-Security-Policy"])

                    denied = await client.post(
                        base + "/api/actions/prepare",
                        json={"action": "pause_open", "payload": {}},
                        headers={"Origin": base},
                    )
                    self.assertEqual(denied.status, 403)

                    headers = {
                        "Origin": base,
                        "X-Var-Lit-CSRF": csrf,
                    }
                    prepared_response = await client.post(
                        base + "/api/actions/prepare",
                        json={"action": "pause_open", "payload": {"value": "x"}},
                        headers=headers,
                    )
                    self.assertEqual(prepared_response.status, 200)
                    prepared = await prepared_response.json()
                    confirmation_id = prepared["confirmationId"]

                    first = await client.post(
                        base + "/api/actions/commit",
                        json={"confirmationId": confirmation_id},
                        headers=headers,
                    )
                    second = await client.post(
                        base + "/api/actions/commit",
                        json={"confirmationId": confirmation_id},
                        headers=headers,
                    )
                    self.assertEqual(first.status, 200)
                    self.assertEqual(second.status, 200)
                    self.assertEqual(len(executions), 1)

                    ws = await client.ws_connect(
                        f"ws://127.0.0.1:{port}/api/stream?csrf={csrf}",
                        headers={"Origin": base},
                    )
                    message = await ws.receive_json(timeout=1)
                    self.assertEqual(message["sequence"], 1)
                    await ws.close()
            finally:
                await server.stop()

        asyncio.run(run_case())

    def test_rejects_non_loopback_configuration(self) -> None:
        async def callback(*_args):
            return {}

        with self.assertRaisesRegex(ValueError, "127.0.0.1"):
            OperationsDashboardServer(
                snapshot_factory=callback,
                action_preparer=callback,
                action_executor=callback,
                asset_dir=PROJECT_DIR / "dashboard",
                host="0.0.0.0",
            )

    def test_dashboard_has_no_external_assets_or_popup_controls(self) -> None:
        source = (PROJECT_DIR / "dashboard" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("https://", source)
        self.assertNotIn("window.open", source)
        self.assertIn("action-detail", source)
        self.assertIn("classList.contains('open')", source)
        self.assertIn(".direction.long", source)
        self.assertIn(".direction.short", source)
        self.assertIn("round.directionKey === 'long_var'", source)
        self.assertIn("Var-Lit V5 自动化监控台", source)
        self.assertIn("V5 原型 · 双边持仓", source)
        self.assertIn("V5 运行 · 双边持仓", source)
        self.assertIn("lastSnapshot?.environment === 'demo'", source)
        self.assertIn("真实 Runtime ·", source)
        self.assertIn("做多 Var 开仓硬门槛", source)
        self.assertIn("做空 Var 开仓硬门槛", source)
        self.assertIn("常规平仓阈值开始时间", source)
        self.assertIn("实时基差", source)
        self.assertIn("metrics.currentBasis", source)
        self.assertIn("metrics.openThresholds", source)
        self.assertIn("预估开仓 PnL", source)
        self.assertIn("开仓门槛", source)
        self.assertIn("三窗中位数", source)
        self.assertIn("实时 + 统计 · 面板只读", source)
        self.assertIn("metrics.basisMedians?.[window]", source)
        self.assertIn("当前持仓收益", source)
        self.assertIn("开仓收益", source)
        self.assertIn("此时平仓磨损", source)
        self.assertIn("平仓预留", source)
        self.assertIn("positionPnl.closeEstimate", source)
        self.assertIn("positionPnl.closeReserve", source)
        self.assertIn("检查并保存", source)
        self.assertIn("不读取或写入真实 .env", source)
        self.assertIn(
            ".sort((a, b) => Number(b.number) - Number(a.number))",
            source,
        )
        self.assertNotIn("displaySnapshot", source)
        self.assertNotIn("demo-scenario", source)
        self.assertIn("definition.key !== 'pause_open'", source)
        self.assertIn("if (controlsLocked) closeAllActions()", source)
        self.assertIn("revision !== prepareRevision", source)


if __name__ == "__main__":
    unittest.main()
