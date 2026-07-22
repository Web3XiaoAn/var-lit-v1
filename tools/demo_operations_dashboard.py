#!/usr/bin/env python3
"""Run the operations dashboard with simulated data and zero exchange access."""

from __future__ import annotations

import argparse
import asyncio
import copy
import signal
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from variational.operations_dashboard import OperationsDashboardServer


class DemoRuntime:
    """Small in-memory stand-in used only for browser and interaction review."""

    def __init__(self) -> None:
        self.sequence = 53000
        self.open_paused = False
        self.positions = {"var": "0.00782", "lighter": "-0.00782"}
        self.config = {
            "executionMode": "live",
            "orderNotionalUsd": "500",
            "maxNormalRoundWearUsd": "-0.020",
            "buyThresholdMinPct": "0.0576",
            "sellThresholdMinPct": "-0.0696",
            "maxQuoteAgeMs": 200,
            "earlyExitMinutes": "30",
        }
        self.rounds = self._rounds()

    @staticmethod
    def _rounds() -> list[dict[str, Any]]:
        values = [
            (1, "short_var", "-0.004100", "-0.002700"),
            (2, "long_var", "+0.003500", "+0.005600"),
            (3, "short_var", "-0.010200", "-0.008500"),
            (4, "long_var", "+0.001900", "+0.002500"),
            (5, "short_var", "-0.011100", "+0.007500"),
            (6, "long_var", "+0.004800", "+0.006700"),
            (7, "short_var", "-0.013000", "-0.009100"),
            (8, "long_var", "+0.002600", "+0.003400"),
            (9, "short_var", "+0.001200", "+0.002900"),
            (10, "long_var", "+0.006000", "+0.008200"),
        ]
        rows: list[dict[str, Any]] = []
        base_closed_at_ms = 1_774_300_000_000
        for number, direction_key, open_wear, close_wear in values:
            round_wear = float(open_wear) + float(close_wear)
            held_seconds = 300 + number * 137
            closed_at_ms = base_closed_at_ms + number * 3_600_000
            rows.append(
                {
                    "number": number,
                    "directionKey": direction_key,
                    "direction": (
                        "多 Var / 空 Lighter"
                        if direction_key == "long_var"
                        else "空 Var / 多 Lighter"
                    ),
                    "openWear": open_wear,
                    "closeWear": close_wear,
                    "roundWear": f"{round_wear:.6f}",
                    "openedAtMs": closed_at_ms - held_seconds * 1000,
                    "closedAtMs": closed_at_ms,
                    "heldSeconds": held_seconds,
                    "withinLimit": round_wear >= -0.02,
                }
            )
        return rows

    async def snapshot(self) -> dict[str, Any]:
        self.sequence += 1
        total_open = sum(float(row["openWear"]) for row in self.rounds)
        total_close = sum(float(row["closeWear"]) for row in self.rounds)
        total = total_open + total_close
        positive = sum(float(row["roundWear"]) >= 0 for row in self.rounds)
        batch_holding_seconds = sum(row["heldSeconds"] for row in self.rounds)
        matched = abs(float(self.positions["var"]) + float(self.positions["lighter"])) < 1e-9
        has_position = abs(float(self.positions["var"])) > 1e-9
        return {
            "schema": "var-lit-v1-operations-state-v1",
            "environment": "demo",
            "sequence": self.sequence,
            "generatedAt": "demo",
            "dataAgeMs": 85,
            "health": {
                "runtimeActive": True,
                "headline": "演示环境 · 不连接交易所、不发送订单",
                "level": "warning",
                "risk": "策略运行稳定（模拟）",
                "actionBusy": False,
            },
            "connections": {
                "command": True,
                "privateStream": True,
                "lighterOrderEntry": True,
                "varAgeMs": 82,
                "lighterAgeMs": 31,
            },
            "positions": {
                **self.positions,
                "activeOrders": 0,
                "capturedAt": "demo-live-snapshot",
                "matched": matched,
                "reconcile": "正常" if matched else "仓位不一致",
                "direction": "多 Var / 空 Lighter" if has_position else None,
                "heldSeconds": 1122 if has_position else None,
                "idleSeconds": None if has_position else 42,
            },
            "strategy": {
                "mode": self.config["executionMode"],
                "status": "新开仓暂停" if self.open_paused else "运行中",
                "openPaused": self.open_paused,
                "automationPaused": False,
                "pauseReason": None,
                "automationReady": True,
            },
            "config": copy.deepcopy(self.config),
            "metrics": {
                "currentRoundEstimate": "-0.0287" if has_position else None,
                "currentRoundNote": (
                    "当前可执行平仓估值；最终轮次仍以双边成交价结算"
                    if has_position
                    else "当前无持仓"
                ),
                "totalOpenWear": f"{total_open:.6f}",
                "totalCloseWear": f"{total_close:.6f}",
                "totalWear": f"{total:.6f}",
                "averageWear": f"{total / len(self.rounds):.6f}",
                "batchHoldingSeconds": batch_holding_seconds,
                "averageHoldingSeconds": batch_holding_seconds // len(self.rounds),
                "todayHoldingSeconds": batch_holding_seconds + (1122 if has_position else 0),
                "todayTradingVolumeUsd": "4218.72",
                "todayWear": f"{total:.6f}",
                "todayAverageWear": f"{total / len(self.rounds):.6f}",
                "todayCompletedRounds": len(self.rounds),
                "holdingDay": "2026-07-21",
                "holdingTimezone": "Asia/Shanghai",
                "positiveRounds": positive,
                "negativeRounds": len(self.rounds) - positive,
                "currentBasis": {
                    "fresh": True,
                    "referenceLongVar": "0.0006123",
                    "referenceShortVar": "-0.0007058",
                    "referenceNotionalUsd": "500",
                    "estimatedOpenLongUsd": "0.30245",
                    "estimatedOpenShortUsd": "-0.35485",
                },
                "openThresholds": {
                    "fresh": True,
                    "longVar": "0.0006379",
                    "shortVar": "-0.0006940",
                },
                "basisMedians": {
                    "5m": {
                        "longVar": "0.0004821",
                        "shortVar": "0.0005356",
                        "ready": True,
                        "sampleCount": 301,
                    },
                    "30m": {
                        "longVar": "0.0004568",
                        "shortVar": "0.0005082",
                        "ready": True,
                        "sampleCount": 1801,
                    },
                    "1h": {
                        "longVar": "0.0004315",
                        "shortVar": "0.0004974",
                        "ready": True,
                        "sampleCount": 3601,
                    },
                },
                "currentPositionPnl": {
                    "active": has_position,
                    "open": "0.0060" if has_position else None,
                    "closeEstimate": "-0.0240" if has_position else None,
                    "closeReserve": "-0.0107" if has_position else None,
                },
            },
            "recentRounds": copy.deepcopy(self.rounds),
        }

    async def prepare(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        facts = {
            "Var 权威仓位": f"{self.positions['var']} BTC",
            "Lighter 权威仓位": f"{self.positions['lighter']} BTC",
            "活动委托": "0",
            "环境": "纯本地模拟，不会发送交易请求",
        }
        if action == "stage_config":
            return {
                "allowed": True,
                "message": "演示中只更新内存显示；不会读取或写入真实 .env。",
                "facts": {**facts, "单边金额": f"{payload.get('orderNotionalUsd', '-')} U"},
            }
        if action in {"close_var_residual", "close_lighter_residual"}:
            own_key = "var" if action == "close_var_residual" else "lighter"
            own_name = "Var" if own_key == "var" else "Lighter"
            if abs(float(self.positions[own_key])) <= 1e-9:
                return {
                    "allowed": False,
                    "reason": f"{own_name} 当前没有残仓。",
                    "facts": facts,
                }
            other = "Lighter" if action == "close_var_residual" else "Var"
            other_key = "lighter" if other == "Lighter" else "var"
            if abs(float(self.positions[other_key])) > 1e-9:
                return {
                    "allowed": False,
                    "reason": f"{other} 并非空仓；真实运行时会拒绝这个单边操作。",
                    "facts": facts,
                }
        if action == "force_round_close" and abs(float(self.positions["var"])) < 1e-9:
            return {"allowed": False, "reason": "当前没有双边持仓", "facts": facts}
        messages = {
            "pause_open": "切换是否允许产生新的策略开仓；平仓和对账继续运行。",
            "force_round_close": "模拟忽略收益阈值并将双边权威仓位归零。",
            "close_var_residual": "模拟仅平 Var 残仓，不创建 Lighter 对冲。",
            "close_lighter_residual": "模拟提交 Lighter reduce-only 平仓。",
            "refresh_var": "模拟刷新 Var 页面并等待行情恢复。",
            "reconcile": "模拟重新读取双方权威仓位。",
        }
        if action not in messages:
            return {"allowed": False, "reason": "未知演示操作", "facts": facts}
        return {"allowed": True, "message": messages[action], "facts": facts}

    async def execute(
        self,
        action: str,
        payload: dict[str, Any],
        _preview: dict[str, Any],
    ) -> dict[str, Any]:
        if action == "pause_open":
            self.open_paused = not self.open_paused
            return {"ok": True, "message": "演示状态已切换。"}
        if action == "stage_config":
            self.config.update(
                {
                    "executionMode": str(payload.get("executionMode") or "observe"),
                    "orderNotionalUsd": str(payload.get("orderNotionalUsd") or ""),
                    "maxNormalRoundWearUsd": str(payload.get("maxNormalRoundWearUsd") or ""),
                    "buyThresholdMinPct": str(payload.get("buyThresholdMinPct") or ""),
                    "sellThresholdMinPct": str(payload.get("sellThresholdMinPct") or ""),
                    "maxQuoteAgeMs": int(payload.get("maxQuoteAgeMs") or 0),
                    "earlyExitMinutes": str(payload.get("earlyExitMinutes") or ""),
                }
            )
            return {"ok": True, "message": "演示参数已更新；没有写入任何文件。"}
        if action == "force_round_close":
            self.positions = {"var": "0", "lighter": "0"}
            return {"ok": True, "message": "演示双边仓位已归零。"}
        if action == "close_var_residual":
            self.positions["var"] = "0"
            return {"ok": True, "message": "演示 Var 残仓已归零。"}
        if action == "close_lighter_residual":
            self.positions["lighter"] = "0"
            return {"ok": True, "message": "演示 Lighter 残仓已归零。"}
        if action == "refresh_var":
            return {"ok": True, "message": "演示 Var 行情已恢复。"}
        if action == "reconcile":
            return {"ok": True, "message": "演示权威账户已重新对账。"}
        return {"ok": False, "error": "未知演示操作"}


async def run(port: int) -> None:
    demo = DemoRuntime()
    server = OperationsDashboardServer(
        snapshot_factory=demo.snapshot,
        action_preparer=demo.prepare,
        action_executor=demo.execute,
        asset_dir=PROJECT_ROOT / "dashboard",
        port=port,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    await server.start()
    print("DEMO ONLY: no exchange clients, credentials, or order channels are loaded.")
    print(f"Open {server.address}")
    try:
        await stop.wait()
    finally:
        await server.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8780)
    args = parser.parse_args()
    asyncio.run(run(args.port))


if __name__ == "__main__":
    main()
