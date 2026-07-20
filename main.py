from __future__ import annotations

import argparse
import asyncio
import copy
import contextlib
import enum
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
import signal
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

import requests
import websockets
from dotenv import dotenv_values
from lighter.signer_client import SignerClient
from lighter.api.account_api import AccountApi
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from variational.listener import (
    HEARTBEAT_STALE_SECONDS,
    CommandBroker,
    EventSink,
    VariationalMonitor,
    run_command_server,
    run_receiver_server,
)
from variational.lighter_order_entry import (
    LighterOrderEntry,
    LighterOrderEntryUnavailable,
    LighterOrderEntryUnknown,
)
from variational.operations_dashboard import OperationsDashboardServer
from variational.telemetry import AsyncJsonlWriter
from variational.research_database import (
    DEFAULT_MAX_DATABASE_BYTES,
    DEFAULT_SYNC_INTERVAL_SECONDS,
    ResearchDatabase,
    ResearchDatabaseSynchronizer,
    default_runtime_sources,
)
from adaptive_strategy import (
    Action as StrategyAction,
    CLOSE_RESERVE_MULTIPLIER,
    CloseCandidate,
    Decision as StrategyDecision,
    DirectionalRates,
    EpochActivator,
    MarketFrame,
    OpenCandidate,
    OpportunitySample,
    ParameterEpoch,
    PositionContext,
    RollingWindowStore,
    Side as StrategySide,
    SourceClock,
    StrategyEngine,
    build_parameter_candidate,
    load_model_config,
    open_candidate_from_payload,
    open_candidate_to_payload,
    opportunity_balance_threshold,
)
from execution_reserve import (
    EXECUTION_SAMPLE_LIMIT_PER_BUCKET,
    ExecutionLossSample,
    read_execution_samples as read_execution_sample_records,
    write_execution_samples as write_execution_sample_records,
)

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
FORWARDER_COMMAND_PORT = 8768
OPERATIONS_DASHBOARD_HOST = "127.0.0.1"
DEFAULT_OPERATIONS_DASHBOARD_PORT = 8780
OPERATIONS_DASHBOARD_ASSET_DIR = Path(__file__).resolve().parent / "dashboard"
DOTENV_FILE = Path(__file__).resolve().parent / ".env"
LOG_DIR = Path(os.environ.get("VARIATIONAL_RUNTIME_DIR", "./log"))
OUTPUT_DIR = LOG_DIR
APP_LOG_FILE = LOG_DIR / "runtime.log"
RUNTIME_STATE_FILE = LOG_DIR / "runtime_state.json"
EXECUTION_SAMPLES_FILE = LOG_DIR / "execution_samples.json"
TRACE_FILE_NAME = "execution_trace.jsonl"
STRATEGY_MARKET_SAMPLES_FILE_NAME = "strategy_market_samples.jsonl"
STRATEGY_SAMPLE_SESSION_FILE_NAME = "current_strategy_sample_session.json"
STRATEGY_MARKET_SAMPLE_VERSION = "adaptive-market-sample-v1"
ADAPTIVE_MODEL_FILE = Path(__file__).resolve().parent / "adaptive_strategy" / "models" / "adaptive-median-v5.json"
RESEARCH_DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "research_data"
    / "strategy_research.sqlite3"
)
TRACE_QUEUE_SIZE = 2048
OPEN_DECISION_TRACE_HEARTBEAT_MS = 10_000
TRACE_MAX_FILE_BYTES = 16 * 1024 * 1024
ORDER_LOG_MAX_FILE_BYTES = 8 * 1024 * 1024
RUNTIME_BUILD = "var-lit-v1"
# Keep the report schema cohort stable across execution-policy-only builds so
# restart does not discard the existing live execution-loss samples.
EXECUTION_SAMPLE_VERSION = "2026-07-15-adaptive-median-v4-live2"
SETTLED_EXECUTION_HISTORY_LIMIT = 100
READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05
EVENT_SIGNAL_FALLBACK_SECONDS = 1.0
STRATEGY_SAMPLE_SECONDS = 1.0
STRATEGY_MIN_SAMPLE_INTERVAL_MS = 750
STRATEGY_MAX_SAMPLE_GAP_MS = 60_000
STRATEGY_HISTORY_RESUME_MAX_GAP_MS = 5 * 60 * 1_000
STRATEGY_STATISTICS_WINDOW_MS = 60 * 60 * 1_000
STRATEGY_CACHE_KEEP_MS = STRATEGY_STATISTICS_WINDOW_MS + STRATEGY_MAX_SAMPLE_GAP_MS
STRATEGY_CACHE_COMPACTION_INTERVAL_MS = 9 * 60 * 1_000
STRATEGY_CACHE_STARTUP_MAX_MS = 70 * 60 * 1_000
DEFAULT_HEDGE_SLIPPAGE_BPS = Decimal("2.0")
MAX_HEDGE_SLIPPAGE_BPS = Decimal("10")
DASHBOARD_REFRESH_SECONDS = 0.2
DASHBOARD_ORDERS = 8
ASSET_SWITCH_CONFIRM_TICKS = 3
DEFAULT_REFERENCE_NOTIONAL_USD = Decimal("500")
DEFAULT_ORDER_NOTIONAL_USD = Decimal("200")
DEFAULT_BUY_DYNAMIC_THRESHOLD_MIN_PCT = Decimal("0.05")
DEFAULT_SELL_DYNAMIC_THRESHOLD_MIN_PCT = Decimal("-0.073")
PERCENT_DIVISOR = Decimal("100")
DEFAULT_PROVISIONAL_RESERVE_BPS_PER_LEG = Decimal("0.50")
DEFAULT_MAX_NORMAL_ROUND_WEAR_BPS = Decimal("1.0")
DEFAULT_PARAMETER_REFRESH_SECONDS = 60
DEFAULT_PARAMETER_CONFIRMATIONS = 1
DEFAULT_EARLY_EXIT_SECONDS = 30 * 60
DEFAULT_MAX_HOLD_SECONDS = 2 * 60 * 60
DEFAULT_MAX_QUOTE_AGE_MS = 600
DEFAULT_ROUND_COOLDOWN_SECONDS = 30
DEFAULT_VAR_ORDER_RESULT_TIMEOUT_MS = 5000
ADAPTIVE_MODEL_VERSION = "adaptive-median-v5"
MIGRATABLE_FLAT_V4_MODEL_HASHES = frozenset(
    {
        # v4 audit1: the signal gate still included MAD, balance and learned
        # execution headroom. Only flat, unpaused telemetry may migrate.
        "6fb063111009d1553d153d28d0b6fa1e408fa8fcd0b34074b29eb40ac668922b",
    }
)
MANUAL_STRATEGY_TAG = "manual"
STRATEGY_EXECUTION_MODES = frozenset({"observe", "live"})
DEFAULT_STRATEGY_EXECUTION_MODE = "observe"
CANARY_SESSION_OBSERVING = "OBSERVING"
CANARY_SESSION_ARMED = "ARMED"
CANARY_SESSION_REVIEW_REQUIRED = "REVIEW_REQUIRED"
CANARY_SESSION_HALTED = "HALTED"
VAR_BASE_QTY_TICK = Decimal("0.000001")
V5_OPEN_RATE_RANGE_BPS = Decimal("3.0")
V5_CLOSE_RATE_RANGE_BPS = Decimal("4.0")
V5_RATE_RANGE_WINDOW_MS = 5_000
V5_CLOSE_RANGE_MAX_DEFERRAL_MS = 2_000
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30
LIGHTER_ORDER_ENTRY_RESPONSE_TIMEOUT_SECONDS = 5.0
LIGHTER_ORDER_ENTRY_QUEUE_SIZE = 64
AUTO_VAR_ORDER_COOLDOWN_SECONDS = 8.0
CLOSE_ZERO_WEAR_STABILITY_MS = 2_000
CLOSE_ZERO_WEAR_ACCUMULATION_WINDOW_MS = 10_000
CLOSE_ZERO_WEAR_MAX_SAMPLE_GAP_MS = 1_000
AUTO_VAR_FILL_TIMEOUT_SECONDS = 20.0
LIGHTER_FILL_TIMEOUT_SECONDS = 20.0
LIGHTER_ERROR_CONFIRM_SECONDS = 5.0
LIGHTER_ORDER_REST_POLL_SECONDS = 2.0
LIGHTER_ORDER_REST_FALLBACK_POLL_SECONDS = 0.5
LIGHTER_INACTIVE_ORDER_PAGE_LIMIT = 100
LIGHTER_INACTIVE_ORDER_MAX_PAGES = 20
LIGHTER_TRADE_RECONCILE_PAGE_LIMIT = 100
LIGHTER_TRADE_RECONCILE_MAX_PAGES = 5
DEFAULT_RECONCILE_INTERVAL_SECONDS = 5.0
DEFAULT_LIGHTER_HEDGE_MAX_ATTEMPTS = 3
FIRM_NOTIONAL_TOLERANCE_USD = Decimal("1.00")
RECONCILE_MISMATCH_CONFIRM_SECONDS = 5.0
VAR_POSITION_TOLERANCE = Decimal("0.000001")
MAX_FORWARDED_EVENT_AGE_SECONDS = 10.0
VAR_PORTFOLIO_RECOVERY_DELAY_SECONDS = 1.0
VAR_PORTFOLIO_RECOVERY_MAX_AGE_SECONDS = 2.0
RECOVERED_FILL_DEDUP_SECONDS = 15.0
VAR_COMMIT_CONFIRM_TIMEOUT_SECONDS = 3.0
LIGHTER_CLIENT_ORDER_INDEX_MAX = (1 << 48) - 1
LIGHTER_CLIENT_ORDER_COLLISION_LIMIT = 128

VAR_INTENT_QUOTING = "QUOTING"
VAR_INTENT_PREPARED = "PREPARED"
VAR_INTENT_COMMITTING = "VAR_COMMITTING"
VAR_INTENT_COMMIT_AMBIGUOUS = "VAR_COMMIT_AMBIGUOUS"
VAR_INTENT_COMMITTED = "VAR_COMMITTED"

RUNTIME_DOTENV_REQUIRED_KEYS = frozenset(
    {
        "LIGHTER_PRIVATE_KEY",
        "LIGHTER_API_KEY_INDEX",
        "LIGHTER_ACCOUNT_INDEX",
        "STRATEGY_EXECUTION_MODE",
        "STRATEGY_ORDER_NOTIONAL_USD",
        "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT",
        "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT",
    }
)
RUNTIME_DOTENV_OPTIONAL_KEYS = frozenset(
    {
        "VARIATIONAL_RUNTIME_DIR",
        "OPERATIONS_DASHBOARD_ENABLED",
        "OPERATIONS_DASHBOARD_PORT",
        "RESEARCH_DATABASE_ENABLED",
        "RESEARCH_DATABASE_FILE",
        "RESEARCH_DATABASE_MAX_MIB",
        "RESEARCH_DATABASE_SYNC_SECONDS",
        "STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS",
        "STRATEGY_MAX_QUOTE_AGE_MS",
        "STRATEGY_EARLY_EXIT_MINUTES",
    }
)
RUNTIME_DOTENV_ALLOWED_KEYS = (
    RUNTIME_DOTENV_REQUIRED_KEYS | RUNTIME_DOTENV_OPTIONAL_KEYS
)

EXECUTION_STATE_VAR_COMMITTED = "VAR_COMMITTED"
EXECUTION_STATE_HEDGE_SUBMITTING = "HEDGE_SUBMITTING"
EXECUTION_STATE_HEDGE_SUBMITTED = "HEDGE_SUBMITTED"
EXECUTION_STATE_HEDGE_PARTIAL = "HEDGE_PARTIAL"
EXECUTION_STATE_HEDGED = "HEDGED"
EXECUTION_STATE_HEDGE_ERROR = "HEDGE_ERROR"
EXECUTION_STATE_RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class LighterOrderReconcileOutcome(enum.Enum):
    FOUND = "FOUND"
    CONFIRMED_ABSENT = "CONFIRMED_ABSENT"
    UNKNOWN = "UNKNOWN"


class VarPortfolioRecoveryOutcome(enum.Enum):
    FILLED = "FILLED"
    CONFIRMED_NOT_FILLED = "CONFIRMED_NOT_FILLED"
    UNKNOWN = "UNKNOWN"


class AccountReconcileOutcome(enum.Enum):
    FRESH_MATCH = "FRESH_MATCH"
    FRESH_MISMATCH = "FRESH_MISMATCH"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


class VarEventOrigin(enum.Enum):
    AUTO_INTENT = "AUTO_INTENT"
    MANUAL_LIVE = "MANUAL_LIVE"
    PORTFOLIO_RECOVERY = "PORTFOLIO_RECOVERY"
    REPLAY = "REPLAY"
    UNKNOWN = "UNKNOWN"


@dataclass(slots=True)
class StrategyConfig:
    execution_mode: str = DEFAULT_STRATEGY_EXECUTION_MODE
    reference_notional_usd: Decimal = DEFAULT_REFERENCE_NOTIONAL_USD
    order_notional_usd: Decimal = DEFAULT_ORDER_NOTIONAL_USD
    buy_dynamic_threshold_min_pct: Decimal = (
        DEFAULT_BUY_DYNAMIC_THRESHOLD_MIN_PCT
    )
    sell_dynamic_threshold_min_pct: Decimal = (
        DEFAULT_SELL_DYNAMIC_THRESHOLD_MIN_PCT
    )
    provisional_reserve_bps_per_leg: Decimal = DEFAULT_PROVISIONAL_RESERVE_BPS_PER_LEG
    max_normal_round_wear_bps: Decimal = DEFAULT_MAX_NORMAL_ROUND_WEAR_BPS
    parameter_refresh_seconds: int = DEFAULT_PARAMETER_REFRESH_SECONDS
    parameter_confirmations: int = DEFAULT_PARAMETER_CONFIRMATIONS
    early_exit_seconds: int = DEFAULT_EARLY_EXIT_SECONDS
    max_hold_seconds: int = DEFAULT_MAX_HOLD_SECONDS
    max_quote_age_ms: int = DEFAULT_MAX_QUOTE_AGE_MS
    dashboard_refresh_seconds: float = DASHBOARD_REFRESH_SECONDS
    sampling_enabled: bool = True
    round_cooldown_seconds: int = DEFAULT_ROUND_COOLDOWN_SECONDS
    var_order_result_timeout_ms: int = DEFAULT_VAR_ORDER_RESULT_TIMEOUT_MS
    hedge_slippage_bps: Decimal = DEFAULT_HEDGE_SLIPPAGE_BPS
    lighter_hedge_max_attempts: int = DEFAULT_LIGHTER_HEDGE_MAX_ATTEMPTS
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS


@dataclass(slots=True)
class VarOrderIntent:
    phase: str
    side: str
    amount: Decimal
    sent_monotonic: float
    market: str
    request_id: str | None = None
    order_id: str | None = None
    provisional_trade_key: str | None = None
    commit_accepted_monotonic: float | None = None
    state: str = VAR_INTENT_QUOTING
    trace_id: str | None = None
    firm_quote_id: str | None = None
    firm_price: Decimal | None = None
    firm_qty: Decimal | None = None
    firm_target_notional_usd: Decimal | None = None
    firm_guard_pnl: Decimal | None = None
    firm_required_pnl: Decimal | None = None
    execution_reserve_usd: Decimal | None = None
    lighter_vwap: Decimal | None = None
    lighter_quote_age_ms: int | None = None
    lighter_client_order_index: int | None = None
    lighter_client_order_collision: int = 0
    sent_at_iso: str | None = None
    prepared_at_iso: str | None = None
    adaptive_strategy_context: dict[str, Any] | None = None
    strategy_tag: str = MANUAL_STRATEGY_TAG
    commit_rfq_id: str | None = None
    confirmed_trade_key: str | None = None


@dataclass(slots=True)
class AccountSnapshot:
    var_position: Decimal
    lighter_position: Decimal
    lighter_active_orders: int
    captured_at: str


@dataclass(slots=True, frozen=True)
class VariationalPortfolioMetadata:
    has_snapshot: bool
    request_id: str | None
    published_at: str | None
    captured_at: str | None
    position_updated_at: str | None
    fingerprint: str | None
    content_revision: int
    age_seconds: float | None


@dataclass(slots=True, frozen=True)
class FirmQuoteDecision:
    allowed: bool
    expected_pnl: Decimal
    required_pnl: Decimal
    lighter_vwap: Decimal


@dataclass(slots=True, frozen=True)
class LighterHedgeDispatchSnapshot:
    """One in-memory, fixed-point pre-send view of the Lighter book."""

    market_generation: int
    market_index: int
    base_amount: int
    price_i: int
    marginal_price_i: int
    economic_limit_price_i: int | None
    order_book_nonce: int | None
    quote_age_ms: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_trace_id() -> str:
    return uuid.uuid4().hex


def trace_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, dict):
        return {str(key): trace_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [trace_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def deterministic_lighter_client_order_index(
    *,
    account_index: int,
    market: str,
    firm_quote_id: str,
    phase: str,
    side: str,
    attempt: int = 0,
    collision: int = 0,
) -> int:
    """Return a stable uint48 Lighter client order index without using wall-clock time."""
    quote_id = firm_quote_id.strip()
    if not quote_id:
        raise ValueError("firm_quote_id is required for a deterministic client order index")
    material = "\x1f".join(
        (
            str(account_index),
            market.strip().upper(),
            quote_id,
            phase.strip().lower(),
            side.strip().upper(),
            str(attempt),
            str(collision),
        )
    ).encode("utf-8")
    value = int.from_bytes(hashlib.sha256(material).digest()[:8], "big") & LIGHTER_CLIENT_ORDER_INDEX_MAX
    return value or 1


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() else None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def normalize_var_base_qty(qty: Decimal | None) -> Decimal | None:
    if qty is None or qty <= 0:
        return None
    normalized = qty.quantize(VAR_BASE_QTY_TICK, rounding=ROUND_DOWN)
    return normalized if normalized > 0 else None


def lighter_hedge_base_amount(
    var_qty: Decimal | None,
    base_amount_multiplier: int | Decimal | None,
) -> int | None:
    """Return the one authoritative Lighter base amount for a Var quantity.

    Lighter accepts an integer base amount.  Keeping this conversion in one
    helper prevents fill, retry, reconciliation, and close checks from using
    incompatible tolerances around the same unavoidable fractional remainder.
    """

    if var_qty is None or not var_qty.is_finite() or var_qty <= 0:
        return None
    if isinstance(base_amount_multiplier, bool):
        return None
    try:
        multiplier = int(base_amount_multiplier or 0)
        multiplier_decimal = Decimal(str(base_amount_multiplier))
    except (TypeError, ValueError, OverflowError, ArithmeticError):
        return None
    if multiplier <= 0 or Decimal(multiplier) != multiplier_decimal:
        return None
    base_amount = int(
        (var_qty * Decimal(multiplier)).to_integral_value(rounding=ROUND_DOWN)
    )
    return base_amount if base_amount > 0 else None


def lighter_base_qty_tick(
    base_amount_multiplier: int | Decimal | None,
) -> Decimal | None:
    """Return Lighter's exact base-quantity tick, or ``None`` when invalid."""

    if isinstance(base_amount_multiplier, bool):
        return None
    try:
        multiplier = int(base_amount_multiplier or 0)
        multiplier_decimal = Decimal(str(base_amount_multiplier))
    except (TypeError, ValueError, OverflowError, ArithmeticError):
        return None
    if multiplier <= 0 or Decimal(multiplier) != multiplier_decimal:
        return None
    return Decimal("1") / Decimal(multiplier)


def lighter_hedge_target_qty(
    var_qty: Decimal | None,
    base_amount_multiplier: int | Decimal | None,
) -> Decimal | None:
    """Return the exact Lighter quantity represented by its integer amount."""

    base_amount = lighter_hedge_base_amount(var_qty, base_amount_multiplier)
    if base_amount is None:
        return None
    return Decimal(base_amount) / Decimal(int(base_amount_multiplier))


def calculate_lighter_vwap(
    order_book: dict[str, dict[Decimal, Decimal]],
    lighter_side: str,
    qty: Decimal,
) -> Decimal | None:
    result = calculate_lighter_execution(order_book, lighter_side, qty)
    return result[0] if result is not None else None


def calculate_lighter_execution(
    order_book: dict[str, dict[Decimal, Decimal]],
    lighter_side: str,
    qty: Decimal,
) -> tuple[Decimal, Decimal] | None:
    """Return full-depth VWAP and the marginal price for an executable size."""
    if qty <= 0:
        return None
    side = lighter_side.strip().upper()
    if side == "SELL":
        levels = sorted(order_book.get("bids", {}).items(), reverse=True)
    elif side == "BUY":
        levels = sorted(order_book.get("asks", {}).items())
    else:
        return None

    remaining = qty
    quote_total = Decimal("0")
    for price, available in levels:
        if price <= 0 or available <= 0:
            continue
        take = min(remaining, available)
        quote_total += take * price
        remaining -= take
        if remaining <= 0:
            return quote_total / qty, price
    return None


def calculate_lighter_execution_ticks(
    order_book: dict[str, dict[int, int]],
    lighter_side: str,
    base_amount: int,
    *,
    price_multiplier: int,
    base_amount_multiplier: int,
) -> tuple[Decimal, Decimal] | None:
    """Execute against integer price/size ticks without float arithmetic."""
    if base_amount <= 0 or price_multiplier <= 0 or base_amount_multiplier <= 0:
        return None
    execution = calculate_lighter_execution_tick_values(
        order_book,
        lighter_side,
        base_amount,
    )
    if execution is None:
        return None
    quote_ticks, marginal_price_i = execution
    return (
        Decimal(quote_ticks) / Decimal(base_amount * price_multiplier),
        Decimal(marginal_price_i) / Decimal(price_multiplier),
    )


def calculate_lighter_execution_tick_values(
    order_book: dict[str, dict[int, int]],
    lighter_side: str,
    base_amount: int,
) -> tuple[int, int] | None:
    """Return quote ticks and marginal price tick for a complete integer fill."""
    if base_amount <= 0:
        return None
    side = lighter_side.strip().upper()
    if side == "SELL":
        levels = sorted(order_book.get("bids", {}).items(), reverse=True)
    elif side == "BUY":
        levels = sorted(order_book.get("asks", {}).items())
    else:
        return None

    remaining = base_amount
    quote_ticks = 0
    for price_tick, available_base in levels:
        if price_tick <= 0 or available_base <= 0:
            continue
        take = min(remaining, available_base)
        quote_ticks += take * price_tick
        remaining -= take
        if remaining <= 0:
            return quote_ticks, price_tick
    return None


def decimal_ratio(value: Decimal) -> tuple[int, int]:
    """Represent a Decimal exactly as an integer numerator/denominator."""
    sign, digits, exponent = value.as_tuple()
    numerator = int("".join(str(digit) for digit in digits) or "0")
    if sign:
        numerator = -numerator
    if exponent >= 0:
        return numerator * (10**exponent), 1
    return numerator, 10 ** (-exponent)


def ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


def lighter_ioc_limit_price_tick(
    *,
    best_price_i: int,
    lighter_side: str,
    slippage_bps: Decimal,
) -> int | None:
    """Derive the IOC limit entirely in integer price ticks."""
    if best_price_i <= 0 or slippage_bps < 0:
        return None
    bps_numerator, bps_denominator = decimal_ratio(slippage_bps)
    denominator = 10_000 * bps_denominator
    side = lighter_side.strip().upper()
    if side == "BUY":
        return ceil_div(best_price_i * (denominator + bps_numerator), denominator)
    if side == "SELL":
        return (best_price_i * (denominator - bps_numerator)) // denominator
    return None


def lighter_reduce_only_market_price_tick(
    *,
    anchor_price_i: int,
    lighter_side: str,
) -> int | None:
    """Return a practically unbounded market price for a committed close.

    Lighter's market-order transaction still requires a positive integer price.
    After the Variational close has filled, that field must no longer act as a
    local economic/slippage veto: leaving the Lighter leg open is the larger
    risk.  A BUY may sweep up to twice the anchor; a SELL uses the minimum
    positive tick.  The exchange's own reduce-only semantics remain authoritative.
    """

    if anchor_price_i <= 0:
        return None
    side = lighter_side.strip().upper()
    if side == "BUY":
        return anchor_price_i * 2
    if side == "SELL":
        return 1
    return None


def lighter_economic_limit_price(
    *,
    var_side: str,
    firm_price: Decimal,
    firm_qty: Decimal,
    required_pnl: Decimal,
) -> Decimal | None:
    """Return the worst Lighter price that still preserves Firm Guard economics."""

    if firm_price <= 0 or firm_qty <= 0:
        return None
    side = var_side.strip().upper()
    if side == "BUY":
        limit = firm_price + required_pnl / firm_qty
    elif side == "SELL":
        limit = firm_price - required_pnl / firm_qty
    else:
        return None
    return limit if limit.is_finite() and limit > 0 else None


def evaluate_firm_quote_guard(
    *,
    var_side: str,
    firm_price: Decimal,
    firm_qty: Decimal,
    lighter_vwap: Decimal,
    minimum_pnl: Decimal,
    execution_reserve: Decimal,
) -> FirmQuoteDecision:
    side = var_side.strip().upper()
    if side == "BUY":
        expected_pnl = (lighter_vwap - firm_price) * firm_qty
    elif side == "SELL":
        expected_pnl = (firm_price - lighter_vwap) * firm_qty
    else:
        raise ValueError(f"Unsupported Var side: {var_side}")
    required_pnl = minimum_pnl + max(Decimal("0"), execution_reserve)
    return FirmQuoteDecision(
        allowed=expected_pnl >= required_pnl,
        expected_pnl=expected_pnl,
        required_pnl=required_pnl,
        lighter_vwap=lighter_vwap,
    )


def resolve_variational_ticker(ticker: str) -> str:
    return VARIATIONAL_TICKER_OVERRIDES.get(ticker.upper(), ticker.upper())


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def load_runtime_env(dotenv_path: Path = DOTENV_FILE) -> None:
    """Load one authoritative .env and reject ambiguous duplicate settings."""

    if not dotenv_path.is_file():
        raise RuntimeError(f"Required .env file is missing: {dotenv_path}")
    seen: set[str] = set()
    duplicates: set[str] = set()
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, _value = line.partition("=")
        key = key.strip()
        if not separator or not key:
            continue
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    if duplicates:
        raise RuntimeError(
            "Duplicate .env keys are not allowed: " + ", ".join(sorted(duplicates))
        )
    missing = sorted(RUNTIME_DOTENV_REQUIRED_KEYS - seen)
    if missing:
        raise RuntimeError(
            "Required .env keys are missing: " + ", ".join(missing)
        )
    unexpected = sorted(seen - RUNTIME_DOTENV_ALLOWED_KEYS)
    if unexpected:
        raise RuntimeError(
            "Unsupported .env keys must be removed: " + ", ".join(unexpected)
        )
    values = dotenv_values(dotenv_path=dotenv_path, interpolate=False)
    empty = sorted(
        key
        for key in RUNTIME_DOTENV_REQUIRED_KEYS
        if not str(values.get(key) or "").strip()
    )
    if empty:
        raise RuntimeError(
            "Required .env values are empty: " + ", ".join(empty)
        )
    # This personal runtime treats the local file as the sole source of truth.
    # Remove stale shell settings—including the former private-key alias—then
    # install only the explicitly supported local values.
    for key in tuple(os.environ):
        if (
            key.startswith(("STRATEGY_", "LIGHTER_", "RESEARCH_DATABASE_", "AUTO_VAR_"))
            or key in {"API_KEY_PRIVATE_KEY", "VARIATIONAL_RUNTIME_DIR"}
        ):
            os.environ.pop(key, None)
    for key in RUNTIME_DOTENV_REQUIRED_KEYS:
        os.environ[key] = str(values[key])
    for key in RUNTIME_DOTENV_OPTIONAL_KEYS:
        value = str(values.get(key) or "").strip()
        if value:
            os.environ[key] = value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


def optional_env_bool(name: str) -> bool | None:
    value = os.getenv(name, "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def configure_runtime_paths() -> None:
    """Resolve local runtime artifacts after the authoritative .env is loaded."""

    global LOG_DIR, OUTPUT_DIR, APP_LOG_FILE
    global RUNTIME_STATE_FILE, EXECUTION_SAMPLES_FILE

    runtime_dir = Path(optional_env("VARIATIONAL_RUNTIME_DIR") or "./log")
    LOG_DIR = runtime_dir.expanduser()
    OUTPUT_DIR = LOG_DIR
    APP_LOG_FILE = LOG_DIR / "runtime.log"
    RUNTIME_STATE_FILE = LOG_DIR / "runtime_state.json"
    EXECUTION_SAMPLES_FILE = LOG_DIR / "execution_samples.json"


def _payload_decimal(
    payload: dict[str, Any],
    key: str,
    current: Decimal,
    *,
    positive: bool = False,
    maximum: Decimal | None = None,
) -> Decimal:
    value = to_decimal(payload.get(key))
    if value is None or not value.is_finite():
        return current
    if positive and value <= 0:
        return current
    if maximum is not None and value > maximum:
        return current
    return value


def _payload_dynamic_threshold_min_pct(
    payload: dict[str, Any],
    key: str,
    current: Decimal,
    *,
    side: StrategySide,
) -> Decimal:
    value = to_decimal(payload.get(key))
    if value is None or not value.is_finite():
        return current
    if side is StrategySide.BUY:
        return value if Decimal("0") < value <= Decimal("1") else current
    return value if Decimal("-1") <= value < Decimal("0") else current


def _payload_int(
    payload: dict[str, Any],
    key: str,
    current: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = payload.get(key)
    try:
        value = int(str(raw).strip())
    except Exception:
        return current
    if value < minimum or value > maximum:
        return current
    return value


def _payload_minutes_to_seconds(
    payload: dict[str, Any],
    key: str,
    current: int,
    *,
    minimum_seconds: int,
    maximum_seconds: int,
) -> int:
    value = to_decimal(payload.get(key))
    if value is None:
        return current
    seconds = int(value * Decimal("60"))
    if seconds < minimum_seconds or seconds > maximum_seconds:
        return current
    return seconds


def _payload_seconds(
    payload: dict[str, Any],
    key: str,
    current: int,
    *,
    minimum_seconds: int,
    maximum_seconds: int,
) -> int:
    raw = payload.get(key)
    try:
        seconds = int(str(raw).strip())
    except Exception:
        return current
    if seconds < minimum_seconds or seconds > maximum_seconds:
        return current
    return seconds


def _payload_bool(payload: dict[str, Any], key: str, current: bool) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    return current


def _payload_execution_mode(payload: dict[str, Any], current: str) -> str:
    value = str(payload.get("executionMode") or "").strip().lower()
    return value if value in STRATEGY_EXECUTION_MODES else current


def strategy_config_from_payload(payload: dict[str, Any], current: StrategyConfig | None = None) -> StrategyConfig:
    base = current or StrategyConfig()
    dashboard_refresh_ms = _payload_int(
        payload,
        "dashboardRefreshMs",
        int(round(base.dashboard_refresh_seconds * 1000)),
        minimum=100,
        maximum=5000,
    )
    config = StrategyConfig(
        execution_mode=_payload_execution_mode(payload, base.execution_mode),
        reference_notional_usd=_payload_decimal(
            payload,
            "referenceNotionalUsd",
            base.reference_notional_usd,
            positive=True,
        ),
        order_notional_usd=_payload_decimal(
            payload,
            "orderNotionalUsd",
            base.order_notional_usd,
            positive=True,
        ),
        buy_dynamic_threshold_min_pct=_payload_dynamic_threshold_min_pct(
            payload,
            "buyDynamicThresholdMinPct",
            base.buy_dynamic_threshold_min_pct,
            side=StrategySide.BUY,
        ),
        sell_dynamic_threshold_min_pct=_payload_dynamic_threshold_min_pct(
            payload,
            "sellDynamicThresholdMinPct",
            base.sell_dynamic_threshold_min_pct,
            side=StrategySide.SELL,
        ),
        provisional_reserve_bps_per_leg=_payload_decimal(
            payload,
            "provisionalReserveBpsPerLeg",
            base.provisional_reserve_bps_per_leg,
            positive=True,
            maximum=Decimal("10"),
        ),
        max_normal_round_wear_bps=_payload_decimal(
            payload,
            "maxNormalRoundWearBps",
            base.max_normal_round_wear_bps,
            positive=True,
            maximum=Decimal("100"),
        ),
        parameter_refresh_seconds=_payload_minutes_to_seconds(
            payload,
            "parameterRefreshMinutes",
            base.parameter_refresh_seconds,
            minimum_seconds=60,
            maximum_seconds=60 * 60,
        ),
        parameter_confirmations=_payload_int(
            payload,
            "parameterConfirmations",
            base.parameter_confirmations,
            minimum=1,
            maximum=1,
        ),
        early_exit_seconds=_payload_minutes_to_seconds(
            payload,
            "earlyExitMinutes",
            base.early_exit_seconds,
            minimum_seconds=60,
            maximum_seconds=2 * 60 * 60,
        ),
        max_hold_seconds=_payload_minutes_to_seconds(
            payload,
            "maxHoldMinutes",
            base.max_hold_seconds,
            minimum_seconds=30 * 60,
            maximum_seconds=7 * 24 * 60 * 60,
        ),
        max_quote_age_ms=_payload_int(
            payload,
            "maxQuoteAgeMs",
            base.max_quote_age_ms,
            minimum=100,
            maximum=10000,
        ),
        dashboard_refresh_seconds=dashboard_refresh_ms / 1000,
        sampling_enabled=_payload_bool(
            payload,
            "samplingEnabled",
            base.sampling_enabled,
        ),
        round_cooldown_seconds=_payload_seconds(
            payload,
            "roundCooldownSeconds",
            base.round_cooldown_seconds,
            minimum_seconds=0,
            maximum_seconds=60 * 60,
        ),
        var_order_result_timeout_ms=_payload_int(
            payload,
            "varOrderResultTimeoutMs",
            base.var_order_result_timeout_ms,
            minimum=3000,
            maximum=60000,
        ),
        hedge_slippage_bps=_payload_decimal(
            payload,
            "hedgeSlippageBps",
            base.hedge_slippage_bps,
            positive=True,
            maximum=MAX_HEDGE_SLIPPAGE_BPS,
        ),
        lighter_hedge_max_attempts=_payload_int(
            payload,
            "lighterHedgeMaxAttempts",
            base.lighter_hedge_max_attempts,
            minimum=1,
            maximum=5,
        ),
        reconcile_interval_seconds=(
            _payload_int(
                payload,
                "reconcileIntervalMs",
                int(round(base.reconcile_interval_seconds * 1000)),
                minimum=1000,
                maximum=60000,
            )
            / 1000
        ),
    )
    return config


REMOVED_STRATEGY_ENV_KEYS = frozenset(
    {
        "STRATEGY_MEDIAN_MODE",
        "STRATEGY_SIGNAL_NOTIONAL_USD",
        "STRATEGY_OPEN_LONG_MIN_PROFIT_USD",
        "STRATEGY_OPEN_SHORT_MIN_PROFIT_USD",
        "STRATEGY_OPEN_CUSHION_THRESHOLD_PCT",
        "STRATEGY_MIN_HOLD_MINUTES",
        "STRATEGY_CLOSE_MIN_PROFIT_USD",
        "STRATEGY_ROUND_MIN_PROFIT_USD",
        "STRATEGY_EXECUTION_BUFFER_USD",
        "STRATEGY_EXECUTION_BUFFER_MAX_USD",
        "STRATEGY_MEDIAN_SAMPLING_ENABLED",
        "STRATEGY_SHADOW_EVALUATION_ENABLED",
        "STRATEGY_ENFORCE_CANARY_ARM_TOKEN",
        "STRATEGY_ENFORCE_MAX_NOTIONAL_USD",
        "STRATEGY_ENFORCE_MAX_ROUNDS",
        "STRATEGY_ENFORCE_MAX_CUMULATIVE_LOSS_USD",
        "STRATEGY_ENFORCE_MAX_CONSECUTIVE_LOSSES",
        "STRATEGY_CANARY_ARM_TOKEN",
        "STRATEGY_CANARY_MAX_FIRM_NOTIONAL_USD",
        "STRATEGY_CANARY_MAX_CUMULATIVE_LOSS_USD",
        "STRATEGY_CANARY_MAX_CONSECUTIVE_LOSSES",
        "AUTO_VAR_OPEN_ENABLED",
        "AUTO_VAR_CLOSE_ENABLED",
        "AUTO_VAR_EXECUTION_MODE",
        "AUTO_VAR_CLICK_ONLY_ENABLED",
    }
)


def reject_removed_strategy_env() -> None:
    present = sorted(key for key in REMOVED_STRATEGY_ENV_KEYS if key in os.environ)
    if present:
        joined = ", ".join(present)
        raise RuntimeError(
            "Removed strategy environment variables detected: "
            f"{joined}. Migrate to STRATEGY_EXECUTION_MODE and STRATEGY_* v1 settings."
        )


def validate_strategy_env() -> None:
    """Reject malformed explicit settings instead of trading on a default."""

    errors: list[str] = []
    mode = os.getenv("STRATEGY_EXECUTION_MODE")
    if mode is not None and mode.strip().lower() not in STRATEGY_EXECUTION_MODES:
        errors.append("STRATEGY_EXECUTION_MODE must be observe or live")

    decimal_rules: dict[str, tuple[Decimal, Decimal | None]] = {
        "STRATEGY_REFERENCE_NOTIONAL_USD": (Decimal("0"), None),
        "STRATEGY_ORDER_NOTIONAL_USD": (Decimal("0"), None),
        "STRATEGY_PROVISIONAL_RESERVE_BPS_PER_LEG": (Decimal("0"), Decimal("10")),
        "STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS": (Decimal("0"), Decimal("100")),
        "STRATEGY_PARAMETER_REFRESH_MINUTES": (Decimal("0"), Decimal("60")),
        "STRATEGY_EARLY_EXIT_MINUTES": (Decimal("1"), Decimal("120")),
        "STRATEGY_MAX_HOLD_MINUTES": (Decimal("30"), Decimal("10080")),
        "LIGHTER_HEDGE_SLIPPAGE_BPS": (
            Decimal("0"),
            MAX_HEDGE_SLIPPAGE_BPS,
        ),
    }
    for name, (minimum, maximum) in decimal_rules.items():
        if name not in os.environ:
            continue
        raw = os.getenv(name, "").strip()
        try:
            value = Decimal(raw)
        except Exception:
            errors.append(f"{name} must be a finite decimal")
            continue
        if not value.is_finite() or value <= minimum:
            errors.append(f"{name} must be greater than {minimum}")
        elif maximum is not None and value > maximum:
            errors.append(f"{name} must not exceed {maximum}")

    threshold_pct_rules = {
        "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": StrategySide.BUY,
        "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": StrategySide.SELL,
    }
    for name, side in threshold_pct_rules.items():
        if name not in os.environ:
            continue
        raw = os.getenv(name, "").strip()
        try:
            value = Decimal(raw)
        except Exception:
            errors.append(f"{name} must be a finite decimal percentage")
            continue
        if not value.is_finite():
            errors.append(f"{name} must be a finite decimal percentage")
        elif side is StrategySide.BUY and not (
            Decimal("0") < value <= Decimal("1")
        ):
            errors.append(f"{name} must be greater than 0 and at most 1 percent")
        elif side is StrategySide.SELL and not (
            Decimal("-1") <= value < Decimal("0")
        ):
            errors.append(f"{name} must be at least -1 and below 0 percent")

    integer_rules: dict[str, tuple[int, int]] = {
        "STRATEGY_PARAMETER_CONFIRMATIONS": (1, 1),
        "STRATEGY_MAX_QUOTE_AGE_MS": (100, 10_000),
        "STRATEGY_DASHBOARD_REFRESH_MS": (100, 5_000),
        "STRATEGY_ROUND_COOLDOWN_SECONDS": (0, 3_600),
        "STRATEGY_VAR_ORDER_RESULT_TIMEOUT_MS": (3_000, 60_000),
        "LIGHTER_HEDGE_MAX_ATTEMPTS": (1, 5),
        "STRATEGY_RECONCILE_INTERVAL_MS": (1_000, 60_000),
    }
    for name, (minimum, maximum) in integer_rules.items():
        if name not in os.environ:
            continue
        raw = os.getenv(name, "").strip()
        try:
            value = int(raw)
        except Exception:
            errors.append(f"{name} must be an integer")
            continue
        if str(value) != raw and raw not in {f"+{value}", f"-{abs(value)}"}:
            errors.append(f"{name} must use integer syntax")
        elif value < minimum or value > maximum:
            errors.append(f"{name} must be between {minimum} and {maximum}")

    boolean_names = (
        "STRATEGY_SAMPLING_ENABLED",
        "STRATEGY_TRACE_TELEMETRY_ENABLED",
        "STRATEGY_PERSIST_EXECUTION_SAMPLES",
        "LIGHTER_ORDER_ENTRY_WS_ENABLED",
        "LIGHTER_ORDER_ENTRY_REST_FALLBACK",
        "LIGHTER_WS_SERVER_PINGS",
    )
    for name in boolean_names:
        if name in os.environ and optional_env_bool(name) is None:
            errors.append(f"{name} must be true or false")

    if errors:
        raise RuntimeError("Invalid strategy environment: " + "; ".join(errors))


def strategy_config_from_env(current: StrategyConfig | None = None) -> StrategyConfig:
    reject_removed_strategy_env()
    validate_strategy_env()
    base = current or StrategyConfig()
    payload: dict[str, Any] = {}
    env_to_payload = {
        "STRATEGY_EXECUTION_MODE": "executionMode",
        "STRATEGY_REFERENCE_NOTIONAL_USD": "referenceNotionalUsd",
        "STRATEGY_ORDER_NOTIONAL_USD": "orderNotionalUsd",
        "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": "buyDynamicThresholdMinPct",
        "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": "sellDynamicThresholdMinPct",
        "STRATEGY_PROVISIONAL_RESERVE_BPS_PER_LEG": "provisionalReserveBpsPerLeg",
        "STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS": "maxNormalRoundWearBps",
        "STRATEGY_PARAMETER_REFRESH_MINUTES": "parameterRefreshMinutes",
        "STRATEGY_PARAMETER_CONFIRMATIONS": "parameterConfirmations",
        "STRATEGY_EARLY_EXIT_MINUTES": "earlyExitMinutes",
        "STRATEGY_MAX_HOLD_MINUTES": "maxHoldMinutes",
        "STRATEGY_MAX_QUOTE_AGE_MS": "maxQuoteAgeMs",
        "STRATEGY_DASHBOARD_REFRESH_MS": "dashboardRefreshMs",
        "STRATEGY_ROUND_COOLDOWN_SECONDS": "roundCooldownSeconds",
        "STRATEGY_VAR_ORDER_RESULT_TIMEOUT_MS": "varOrderResultTimeoutMs",
        "LIGHTER_HEDGE_SLIPPAGE_BPS": "hedgeSlippageBps",
        "LIGHTER_HEDGE_MAX_ATTEMPTS": "lighterHedgeMaxAttempts",
        "STRATEGY_RECONCILE_INTERVAL_MS": "reconcileIntervalMs",
    }
    for env_name, payload_key in env_to_payload.items():
        value = optional_env(env_name)
        if value is not None:
            payload[payload_key] = value
    sampling = optional_env_bool("STRATEGY_SAMPLING_ENABLED")
    if sampling is not None:
        payload["samplingEnabled"] = sampling
    config = strategy_config_from_payload(payload, current=base)
    if config.early_exit_seconds >= config.max_hold_seconds:
        raise RuntimeError("STRATEGY_EARLY_EXIT_MINUTES must be below max hold")
    fixed_v1_contract = {
        "STRATEGY_REFERENCE_NOTIONAL_USD": (
            config.reference_notional_usd,
            DEFAULT_REFERENCE_NOTIONAL_USD,
        ),
        "STRATEGY_PROVISIONAL_RESERVE_BPS_PER_LEG": (
            config.provisional_reserve_bps_per_leg,
            DEFAULT_PROVISIONAL_RESERVE_BPS_PER_LEG,
        ),
        "STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS": (
            config.max_normal_round_wear_bps,
            DEFAULT_MAX_NORMAL_ROUND_WEAR_BPS,
        ),
        "STRATEGY_PARAMETER_REFRESH_MINUTES": (
            config.parameter_refresh_seconds,
            DEFAULT_PARAMETER_REFRESH_SECONDS,
        ),
        "STRATEGY_PARAMETER_CONFIRMATIONS": (
            config.parameter_confirmations,
            DEFAULT_PARAMETER_CONFIRMATIONS,
        ),
        "STRATEGY_EARLY_EXIT_MINUTES": (
            config.early_exit_seconds,
            DEFAULT_EARLY_EXIT_SECONDS,
        ),
        "STRATEGY_MAX_HOLD_MINUTES": (
            config.max_hold_seconds,
            DEFAULT_MAX_HOLD_SECONDS,
        ),
    }
    contract_errors = [
        f"{name} must remain {expected} for {ADAPTIVE_MODEL_VERSION} (got {actual})"
        for name, (actual, expected) in fixed_v1_contract.items()
        if actual != expected
    ]
    if contract_errors:
        raise RuntimeError(
            f"Invalid {ADAPTIVE_MODEL_VERSION} fixed contract: "
            + "; ".join(contract_errors)
        )
    return config


def adaptive_strategy_config_hash(
    config: StrategyConfig,
    *,
    model_hash: str,
) -> str:
    """Hash every frozen runtime input that can change a strategy decision."""

    payload = {
        "model_version": ADAPTIVE_MODEL_VERSION,
        "model_hash": model_hash,
        "reference_notional_usd": decimal_to_str(config.reference_notional_usd),
        "order_notional_usd": decimal_to_str(config.order_notional_usd),
        "buy_dynamic_threshold_min_pct": decimal_to_str(
            config.buy_dynamic_threshold_min_pct
        ),
        "sell_dynamic_threshold_min_pct": decimal_to_str(
            config.sell_dynamic_threshold_min_pct
        ),
        "provisional_reserve_bps_per_leg": decimal_to_str(
            config.provisional_reserve_bps_per_leg
        ),
        "max_normal_round_wear_bps": decimal_to_str(
            config.max_normal_round_wear_bps
        ),
        "parameter_refresh_seconds": config.parameter_refresh_seconds,
        "parameter_confirmations": config.parameter_confirmations,
        "max_quote_age_ms": config.max_quote_age_ms,
        "early_exit_seconds": config.early_exit_seconds,
        "max_hold_seconds": config.max_hold_seconds,
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def quote_age_ms(received_monotonic: float | None, now_monotonic: float | None = None) -> int | None:
    if received_monotonic is None:
        return None
    now_value = time.monotonic() if now_monotonic is None else now_monotonic
    return max(0, int((now_value - received_monotonic) * 1000))


def market_data_fresh(
    var_quote_age_ms: int | None,
    lighter_quote_age_ms: int | None,
    max_quote_age_ms: int,
) -> bool:
    return (
        var_quote_age_ms is not None
        and lighter_quote_age_ms is not None
        and var_quote_age_ms <= max_quote_age_ms
        and lighter_quote_age_ms <= max_quote_age_ms
    )


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def spread_percent(diff: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if diff is None or denominator is None or denominator == 0:
        return None
    return (diff / denominator) * Decimal("100")


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered == "confirmed":
        return "filled"
    return lowered


def variational_event_source_id(event: dict[str, Any], field_name: str) -> str | None:
    direct = str(event.get(field_name) or "").strip()
    if direct:
        return direct
    raw = event.get("raw")
    if not isinstance(raw, dict):
        return None
    data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    return str(data.get(field_name) or "").strip() or None


def commit_rfq_id_from_result(result: dict[str, Any]) -> str | None:
    detail = result.get("detail")
    if not isinstance(detail, dict):
        return None
    preview: Any = detail.get("responsePreview")
    if isinstance(preview, str):
        try:
            preview = json.loads(preview)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if not isinstance(preview, dict):
        return None
    return str(preview.get("rfq_id") or "").strip() or None


def lighter_hedge_filled(record: "OrderLifecycle") -> bool:
    return record.hedge_status == "filled" and record.lighter_fill_price is not None


def lighter_order_may_still_fill(record: "OrderLifecycle") -> bool:
    # A queued task can still allocate and submit its deterministic id after a
    # matching Var close arrives.  Requiring an id here would therefore allow
    # the close path to skip protection while the open hedge is still live.
    if record.hedge_status in {"queued", "submitting"}:
        return True
    if record.lighter_client_order_id is None:
        return False
    if record.hedge_status in {"submitted", "uncertain"}:
        return True
    return record.hedge_status == "partial" and not record.lighter_outcome_final


@dataclass(slots=True)
class OrderLifecycle:
    trade_key: str
    trade_id: str
    side: str
    qty: Decimal
    asset: str
    auto_hedge_enabled: bool
    last_variational_status: str
    trace_id: str | None = None

    var_fill_price: Decimal | None = None
    var_fill_ts_iso: str | None = None
    var_fill_source: str = "event"
    var_event_origin: str = VarEventOrigin.UNKNOWN.value
    var_source_rfq: str | None = None
    var_source_quote: str | None = None
    firm_quote_id: str | None = None
    firm_price: Decimal | None = None
    firm_guard_pnl: Decimal | None = None
    firm_required_pnl: Decimal | None = None
    execution_reserve_usd: Decimal | None = None
    strategy_phase: str | None = None
    execution_loss_usd: Decimal | None = None
    execution_loss_recorded: bool = False
    adaptive_strategy_context: dict[str, Any] | None = None
    strategy_tag: str = MANUAL_STRATEGY_TAG
    open_notional_usd: Decimal | None = None
    lighter_rollback_scheduled_qty: Decimal = Decimal("0")
    lighter_qty_correction_scheduled_qty: Decimal = Decimal("0")

    lighter_side: str | None = None
    lighter_client_order_id: int | None = None
    lighter_client_order_ids: list[int] = field(default_factory=list)
    lighter_reserved_client_order_id: int | None = None
    lighter_fill_price: Decimal | None = None
    lighter_filled_qty: Decimal | None = None
    lighter_filled_quote: Decimal | None = None
    # Used only by recovery/protective records.  The Var leg may be fully
    # flattened while Lighter filled only part of the original hedge; in that
    # case the close hedge must target the real residual Lighter exposure, not
    # the full Var close quantity.
    lighter_target_qty_override: Decimal | None = None
    lighter_outcome_final: bool = False
    lighter_fill_ts_iso: str | None = None
    lighter_submitted_at_iso: str | None = None
    lighter_tx_hash: str | None = None
    lighter_reduce_only: bool = False
    hedge_error: str | None = None
    hedge_status: str = "not_started"
    execution_state: str = "UNKNOWN"

    def to_payload(self) -> dict[str, Any]:
        return {
            "trade_key": self.trade_key,
            "trade_id": self.trade_id,
            "side": self.side,
            "qty": decimal_to_str(self.qty),
            "asset": self.asset,
            "variational_filled_price": decimal_to_str(self.var_fill_price),
            "variational_filled_at": self.var_fill_ts_iso,
            "variational_fill_source": self.var_fill_source,
            "variational_event_origin": self.var_event_origin,
            "variational_source_rfq": self.var_source_rfq,
            "variational_source_quote": self.var_source_quote,
            "firm_quote_id": self.firm_quote_id,
            "firm_price": decimal_to_str(self.firm_price),
            "firm_guard_pnl": decimal_to_str(self.firm_guard_pnl),
            "firm_required_pnl": decimal_to_str(self.firm_required_pnl),
            "execution_reserve_usd": decimal_to_str(self.execution_reserve_usd),
            "strategy_phase": self.strategy_phase,
            "execution_loss_usd": decimal_to_str(self.execution_loss_usd),
            "execution_loss_recorded": self.execution_loss_recorded,
            "adaptive_strategy_context": self.adaptive_strategy_context,
            "strategy_tag": self.strategy_tag,
            "open_notional_usd": decimal_to_str(self.open_notional_usd),
            "lighter_rollback_scheduled_qty": decimal_to_str(self.lighter_rollback_scheduled_qty),
            "lighter_qty_correction_scheduled_qty": decimal_to_str(
                self.lighter_qty_correction_scheduled_qty
            ),
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_client_order_ids": self.lighter_client_order_ids,
            "lighter_reserved_client_order_id": self.lighter_reserved_client_order_id,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_qty": decimal_to_str(self.lighter_filled_qty),
            "lighter_filled_quote": decimal_to_str(self.lighter_filled_quote),
            "lighter_target_qty_override": decimal_to_str(
                self.lighter_target_qty_override
            ),
            "lighter_outcome_final": self.lighter_outcome_final,
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "lighter_submitted_at": self.lighter_submitted_at_iso,
            "lighter_reduce_only": self.lighter_reduce_only,
            "lighter_tx_hash": self.lighter_tx_hash,
            "auto_hedge_enabled": self.auto_hedge_enabled,
            "hedge_error": self.hedge_error,
            "hedge_status": self.hedge_status,
            "execution_state": self.execution_state,
            "last_variational_status": self.last_variational_status,
            "trace_id": self.trace_id,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "OrderLifecycle" | None:
        def optional_decimal(
            key: str,
            *,
            positive: bool = False,
            nonnegative: bool = False,
            default: Decimal | None = None,
        ) -> Decimal | None:
            raw = payload.get(key)
            if raw is None:
                return default
            value = to_decimal(raw)
            if (
                value is None
                or (positive and value <= 0)
                or (nonnegative and value < 0)
            ):
                raise ValueError(key)
            return value

        def client_order_id_from_value(raw: Any, key: str) -> int:
            if isinstance(raw, bool):
                raise ValueError(key)
            if isinstance(raw, int):
                value = raw
            elif isinstance(raw, str) and raw.strip().isdigit():
                value = int(raw.strip())
            else:
                raise ValueError(key)
            if not (0 < value <= LIGHTER_CLIENT_ORDER_INDEX_MAX):
                raise ValueError(key)
            return value

        def optional_client_order_id(key: str) -> int | None:
            raw = payload.get(key)
            if raw is None:
                return None
            return client_order_id_from_value(raw, key)

        try:
            qty = optional_decimal("qty", positive=True)
            var_fill_price = optional_decimal(
                "variational_filled_price",
                positive=True,
            )
            firm_price = optional_decimal("firm_price", positive=True)
            firm_guard_pnl = optional_decimal("firm_guard_pnl")
            firm_required_pnl = optional_decimal("firm_required_pnl")
            execution_reserve_usd = optional_decimal(
                "execution_reserve_usd",
                nonnegative=True,
            )
            execution_loss_usd = optional_decimal(
                "execution_loss_usd",
                nonnegative=True,
            )
            open_notional_usd = optional_decimal(
                "open_notional_usd",
                positive=True,
            )
            lighter_rollback_scheduled_qty = optional_decimal(
                "lighter_rollback_scheduled_qty",
                nonnegative=True,
                default=Decimal("0"),
            )
            lighter_qty_correction_scheduled_qty = optional_decimal(
                "lighter_qty_correction_scheduled_qty",
                nonnegative=True,
                default=Decimal("0"),
            )
            lighter_fill_price = optional_decimal(
                "lighter_filled_price",
                positive=True,
            )
            lighter_filled_qty = optional_decimal(
                "lighter_filled_qty",
                nonnegative=True,
            )
            lighter_filled_quote = optional_decimal(
                "lighter_filled_quote",
                nonnegative=True,
            )
            lighter_target_qty_override = optional_decimal(
                "lighter_target_qty_override",
                positive=True,
            )
            raw_ids = payload.get("lighter_client_order_ids", [])
            if not isinstance(raw_ids, list):
                raise ValueError("lighter_client_order_ids")
            lighter_client_order_ids = []
            for raw_value in raw_ids:
                parsed_id = client_order_id_from_value(
                    raw_value,
                    "lighter_client_order_ids",
                )
                if parsed_id in lighter_client_order_ids:
                    raise ValueError("duplicate lighter_client_order_ids")
                lighter_client_order_ids.append(parsed_id)
            lighter_client_order_id = optional_client_order_id(
                "lighter_client_order_id"
            )
            lighter_reserved_client_order_id = optional_client_order_id(
                "lighter_reserved_client_order_id"
            )
        except ValueError:
            return None
        assert qty is not None
        raw_auto_hedge_enabled = payload.get("auto_hedge_enabled", True)
        raw_execution_loss_recorded = payload.get("execution_loss_recorded", False)
        raw_lighter_reduce_only = payload.get("lighter_reduce_only", False)
        if not all(
            isinstance(value, bool)
            for value in (
                raw_auto_hedge_enabled,
                raw_execution_loss_recorded,
                raw_lighter_reduce_only,
            )
        ):
            return None
        record = cls(
            trade_key=str(payload.get("trade_key") or ""),
            trade_id=str(payload.get("trade_id") or ""),
            side=str(payload.get("side") or "").lower(),
            qty=qty,
            asset=str(payload.get("asset") or "").upper(),
            auto_hedge_enabled=raw_auto_hedge_enabled,
            last_variational_status=str(payload.get("last_variational_status") or ""),
        )
        record.var_fill_price = var_fill_price
        record.var_fill_ts_iso = payload.get("variational_filled_at")
        record.var_fill_source = str(payload.get("variational_fill_source") or "event")
        record.var_event_origin = str(
            payload.get("variational_event_origin") or VarEventOrigin.UNKNOWN.value
        )
        record.var_source_rfq = (
            str(payload.get("variational_source_rfq") or "").strip() or None
        )
        record.var_source_quote = (
            str(payload.get("variational_source_quote") or "").strip() or None
        )
        record.firm_quote_id = payload.get("firm_quote_id")
        record.firm_price = firm_price
        record.firm_guard_pnl = firm_guard_pnl
        record.firm_required_pnl = firm_required_pnl
        record.execution_reserve_usd = execution_reserve_usd
        record.strategy_phase = payload.get("strategy_phase")
        record.execution_loss_usd = execution_loss_usd
        record.execution_loss_recorded = raw_execution_loss_recorded
        dynamic_context = payload.get("adaptive_strategy_context")
        record.adaptive_strategy_context = (
            dict(dynamic_context) if isinstance(dynamic_context, dict) else None
        )
        strategy_tag = str(payload.get("strategy_tag") or MANUAL_STRATEGY_TAG).strip()
        record.strategy_tag = strategy_tag or MANUAL_STRATEGY_TAG
        record.open_notional_usd = open_notional_usd
        record.lighter_rollback_scheduled_qty = lighter_rollback_scheduled_qty or Decimal("0")
        record.lighter_qty_correction_scheduled_qty = (
            lighter_qty_correction_scheduled_qty or Decimal("0")
        )
        record.lighter_side = payload.get("lighter_order_side")
        record.lighter_client_order_ids = lighter_client_order_ids
        record.lighter_client_order_id = lighter_client_order_id
        if (
            record.lighter_client_order_id is not None
            and record.lighter_client_order_id not in record.lighter_client_order_ids
        ):
            record.lighter_client_order_ids.append(record.lighter_client_order_id)
        record.lighter_reserved_client_order_id = lighter_reserved_client_order_id
        record.lighter_fill_price = lighter_fill_price
        record.lighter_filled_qty = lighter_filled_qty
        record.lighter_filled_quote = lighter_filled_quote
        record.lighter_target_qty_override = lighter_target_qty_override
        raw_outcome_final = payload.get("lighter_outcome_final", False)
        if not isinstance(raw_outcome_final, bool):
            return None
        record.lighter_outcome_final = raw_outcome_final
        record.lighter_fill_ts_iso = payload.get("lighter_filled_at")
        record.lighter_submitted_at_iso = payload.get("lighter_submitted_at")
        record.lighter_reduce_only = raw_lighter_reduce_only
        record.lighter_tx_hash = payload.get("lighter_tx_hash")
        record.hedge_error = payload.get("hedge_error")
        record.hedge_status = str(payload.get("hedge_status") or "not_started")
        record.execution_state = str(payload.get("execution_state") or "UNKNOWN")
        trace_id = str(payload.get("trace_id") or "").strip()
        record.trace_id = trace_id or None
        return record


def lighter_order_target_qty(
    record: OrderLifecycle,
    base_amount_multiplier: int | Decimal | None,
) -> Decimal | None:
    requested_qty = record.lighter_target_qty_override or record.qty
    target = lighter_hedge_target_qty(requested_qty, base_amount_multiplier)
    if (
        record.lighter_target_qty_override is not None
        and target != record.lighter_target_qty_override
    ):
        # A protective target is derived from an actual exchange fill and must
        # remain exact.  Never silently round it into a different exposure.
        return None
    return target


def lighter_order_target_matches(
    record: OrderLifecycle,
    lighter_qty: Decimal | None,
    base_amount_multiplier: int | Decimal | None,
) -> bool:
    if lighter_qty is None or not lighter_qty.is_finite() or lighter_qty < 0:
        return False
    target = lighter_order_target_qty(record, base_amount_multiplier)
    return target is not None and lighter_qty == target


@dataclass(slots=True)
class LegResult:
    price_diff: Decimal | None
    pct: Decimal | None
    pnl: Decimal | None


@dataclass(slots=True)
class TradeRound:
    open_record: OrderLifecycle
    close_record: OrderLifecycle
    open_result: LegResult
    close_result: LegResult
    round_pnl: Decimal | None


def _pnl_from_diff(qty: Decimal, diff: Decimal | None) -> Decimal | None:
    if diff is None:
        return None
    return diff * qty


def var_open_notional_usd(record: OrderLifecycle) -> Decimal | None:
    if record.var_fill_price is None:
        return None
    return record.qty * record.var_fill_price


def leg_result_by_direction(record: OrderLifecycle) -> LegResult:
    side_n = record.side.strip().lower()
    if side_n == "buy":
        diff = spread_value(record.var_fill_price, record.lighter_fill_price)
        pct = spread_percent(diff, record.var_fill_price)
    elif side_n == "sell":
        diff = spread_value(record.lighter_fill_price, record.var_fill_price)
        pct = spread_percent(diff, record.lighter_fill_price)
    else:
        diff = spread_value(record.lighter_fill_price, record.var_fill_price)
        pct = spread_percent(diff, record.var_fill_price)
    matched_qty = min(record.qty, record.lighter_filled_qty or record.qty)
    return LegResult(price_diff=diff, pct=pct, pnl=_pnl_from_diff(matched_qty, diff))


def _can_close_round(open_record: OrderLifecycle, close_record: OrderLifecycle) -> bool:
    open_side = open_record.side.strip().lower()
    close_side = close_record.side.strip().lower()
    return (
        open_record.asset == close_record.asset
        # A one-Var-tick difference can cross a coarser Lighter base tick and
        # leave real residual exposure.  A completed round requires the exact
        # normalized base quantity on both Var legs.
        and open_record.qty == close_record.qty
        and {open_side, close_side} == {"buy", "sell"}
    )


def build_trade_rounds(records: list[OrderLifecycle]) -> tuple[OrderLifecycle | None, list[TradeRound]]:
    current_open: OrderLifecycle | None = None
    history: list[TradeRound] = []

    for record in records:
        if record.var_fill_price is None:
            continue
        if current_open is None:
            current_open = record
            continue
        if _can_close_round(current_open, record):
            open_result = leg_result_by_direction(current_open)
            close_result = leg_result_by_direction(record)
            round_pnl = None
            if open_result.pnl is not None and close_result.pnl is not None:
                round_pnl = open_result.pnl + close_result.pnl
            history.append(
                TradeRound(
                    open_record=current_open,
                    close_record=record,
                    open_result=open_result,
                    close_result=close_result,
                    round_pnl=round_pnl,
                )
            )
            current_open = None
            continue

        if (
            current_open.asset == record.asset
            and current_open.side.strip().lower() != record.side.strip().lower()
        ):
            # A partial or mismatched close is unsafe to reinterpret as a new
            # opposite position. Account reconciliation will stop automation.
            continue

        current_open = record

    return current_open, history


def runtime_recovery_records(records: list[OrderLifecycle]) -> list[OrderLifecycle]:
    """Keep only exposure that may still matter after a process restart."""
    _, completed_rounds = build_trade_rounds(records)
    settled_keys: set[str] = set()
    for trade_round in completed_rounds:
        legs = (trade_round.open_record, trade_round.close_record)
        if all(
            record.var_fill_source in {"event", "portfolio"}
            and record.last_variational_status == "filled"
            and lighter_hedge_filled(record)
            for record in legs
        ):
            settled_keys.update(record.trade_key for record in legs)
    return [record for record in records if record.trade_key not in settled_keys]


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def record_hold_seconds(record: OrderLifecycle, now: datetime | None = None) -> int | None:
    filled_at = parse_iso_datetime(record.var_fill_ts_iso)
    if filled_at is None:
        return None
    now_dt = now or datetime.now(timezone.utc)
    return max(0, int((now_dt - filled_at).total_seconds()))


def opposite_var_order_side(side: str) -> str | None:
    side_n = side.strip().lower()
    if side_n == "buy":
        return "SELL"
    if side_n == "sell":
        return "BUY"
    return None


def var_result_is_ambiguous(result: dict[str, Any]) -> bool:
    if result.get("ok"):
        return False
    error = str(result.get("error") or "").lower()
    return any(
        token in error
        for token in (
            "timed out",
            "timeout",
            "disconnected",
            "超过",
            "没有返回结果",
            "页面执行失败",
            "network",
            "abort",
        )
    )


def var_intent_crossed_commit_boundary(intent: VarOrderIntent | None) -> bool:
    return bool(
        intent is not None
        and intent.state
        in {
            VAR_INTENT_COMMITTING,
            VAR_INTENT_COMMIT_AMBIGUOUS,
            VAR_INTENT_COMMITTED,
        }
    )


def firm_guard_pnl_from_result(result: dict[str, Any]) -> Decimal | None:
    detail = result.get("detail")
    if not isinstance(detail, dict):
        return None
    quote = detail.get("quote")
    if not isinstance(quote, dict):
        return None
    return to_decimal(quote.get("guardPnl"))


def lighter_error_is_definitive(message: str) -> bool:
    lowered = message.lower()
    return any(
        token in lowered
        for token in (
            "not enough margin",
            "insufficient margin",
            "invalid order",
            "sendtx rejected",
            "signature error",
            "checkclient error",
        )
    )


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        output_dir: Path | None,
        quiet: bool,
    ) -> None:
        self.monitor = VariationalMonitor(trade_limit=500, snapshot_file=None)
        self.sink = EventSink(
            output_dir=output_dir,
            quiet=quiet,
            monitor=self.monitor,
        )
        self.command_broker = CommandBroker(quiet=quiet)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.command_port = command_port
        self.ws_server = None
        self.rest_server = None
        self.command_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server(
            "ws",
            self.host,
            self.ws_port,
            self.sink,
            command_broker=self.command_broker,
            command_bridge=False,
        )
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)
        self.command_server = await run_command_server(self.host, self.command_port, self.command_broker)

    async def stop(self) -> None:
        if self.command_server is not None:
            self.command_server.close()
            await self.command_server.wait_closed()
        if self.ws_server is not None:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        if self.rest_server is not None:
            self.rest_server.close()
            await self.rest_server.wait_closed()


class VariationalToLighterRuntime:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.ticker: str | None = None
        self.variational_ticker: str | None = None
        self.accepted_assets: set[str] = set()

        self.stop_flag = False
        self.logger = logging.getLogger("var_lighter_runtime")
        self.logger.setLevel(logging.INFO)
        for handler in self.logger.handlers:
            handler.close()
        self.logger.handlers.clear()
        self.logger.propagate = False

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        # Keep prior-run diagnostics.  A restart must not erase the only evidence
        # available for reconciling an ambiguous or one-sided execution.
        file_handler = RotatingFileHandler(
            APP_LOG_FILE,
            mode="a",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self.dashboard_console = Console()
        self.strategy_model = load_model_config(ADAPTIVE_MODEL_FILE)
        self.strategy_config = strategy_config_from_env()
        if (
            self.strategy_config.reference_notional_usd
            != self.strategy_model.reference_notional_usd
        ):
            raise RuntimeError(
                "STRATEGY_REFERENCE_NOTIONAL_USD does not match the sealed "
                f"model basis ({self.strategy_model.reference_notional_usd})"
            )
        self.strategy_config_hash = adaptive_strategy_config_hash(
            self.strategy_config,
            model_hash=self.strategy_model.model_hash,
        )

        output_dir = OUTPUT_DIR.expanduser().resolve()
        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            command_port=FORWARDER_COMMAND_PORT,
            output_dir=None,
            quiet=True,
        )
        self._market_signal_event = asyncio.Event()
        self._market_signal_revision = 0
        self._trade_signal_revision = 0
        self._trade_event_drain_lock = asyncio.Lock()
        self.execution_event_queue: asyncio.Queue[
            tuple[dict[str, Any], asyncio.Future[None]]
        ] = asyncio.Queue(maxsize=2048)
        self.execution_event_task: asyncio.Task[None] | None = None
        self._strategy_sample_event = asyncio.Event()
        self.runtime.monitor.on_quote_update = self.notify_variational_quote_signal
        self.runtime.monitor.on_trade_event = self.notify_trade_signal

        self.orders_file = output_dir / "order_metrics.jsonl" if output_dir else None
        if self.orders_file is not None:
            self.orders_file.parent.mkdir(parents=True, exist_ok=True)
        self.order_log_writer = (
            AsyncJsonlWriter(
                self.orders_file,
                max_queue_size=TRACE_QUEUE_SIZE,
                max_file_bytes=ORDER_LOG_MAX_FILE_BYTES,
                backup_count=5,
            )
            if self.orders_file is not None
            else None
        )
        trace_telemetry = optional_env_bool("STRATEGY_TRACE_TELEMETRY_ENABLED")
        self.trace_telemetry_enabled = trace_telemetry is not False
        self.trace_file = output_dir / TRACE_FILE_NAME if self.trace_telemetry_enabled else None
        if self.trace_file is not None:
            self.trace_file.parent.mkdir(parents=True, exist_ok=True)
        self.trace_writer = (
            AsyncJsonlWriter(
                self.trace_file,
                max_queue_size=TRACE_QUEUE_SIZE,
                max_file_bytes=TRACE_MAX_FILE_BYTES,
                backup_count=3,
            )
            if self.trace_file is not None
            else None
        )
        self.strategy_market_samples_file = (
            output_dir / STRATEGY_MARKET_SAMPLES_FILE_NAME
            if self.strategy_config.sampling_enabled
            else None
        )
        self.strategy_market_sample_writer = (
            AsyncJsonlWriter(
                self.strategy_market_samples_file,
                max_queue_size=8_192,
                rolling_timestamp_field="sample_timestamp_ms",
                rolling_keep_ms=STRATEGY_CACHE_KEEP_MS,
                rolling_compaction_interval_ms=STRATEGY_CACHE_COMPACTION_INTERVAL_MS,
            )
            if self.strategy_market_samples_file is not None
            else None
        )
        research_database_setting = optional_env_bool("RESEARCH_DATABASE_ENABLED")
        # Unit tests and deliberately isolated runtimes opt out by setting a
        # custom runtime directory. Production defaults to continuous research
        # capture unless explicitly disabled.
        self.research_database_enabled = (
            research_database_setting
            if research_database_setting is not None
            else optional_env("VARIATIONAL_RUNTIME_DIR") is None
        )
        research_database_path = Path(
            optional_env("RESEARCH_DATABASE_FILE")
            or RESEARCH_DATABASE_FILE.as_posix()
        )
        try:
            research_database_max_mib = int(
                optional_env("RESEARCH_DATABASE_MAX_MIB")
                or str(DEFAULT_MAX_DATABASE_BYTES // (1024 * 1024))
            )
        except ValueError as exc:
            raise RuntimeError("RESEARCH_DATABASE_MAX_MIB must be an integer") from exc
        if research_database_max_mib < 128:
            raise RuntimeError("RESEARCH_DATABASE_MAX_MIB must be at least 128")
        try:
            research_database_sync_seconds = float(
                optional_env("RESEARCH_DATABASE_SYNC_SECONDS")
                or str(DEFAULT_SYNC_INTERVAL_SECONDS)
            )
        except ValueError as exc:
            raise RuntimeError(
                "RESEARCH_DATABASE_SYNC_SECONDS must be numeric"
            ) from exc
        if not 0.25 <= research_database_sync_seconds <= 60.0:
            raise RuntimeError(
                "RESEARCH_DATABASE_SYNC_SECONDS must be between 0.25 and 60"
            )
        self.research_database_sync_seconds = research_database_sync_seconds
        self.research_database_synchronizer = (
            ResearchDatabaseSynchronizer(
                ResearchDatabase(
                    research_database_path,
                    max_bytes=research_database_max_mib * 1024 * 1024,
                ),
                default_runtime_sources(output_dir),
            )
            if self.research_database_enabled
            else None
        )
        self.research_database_task: asyncio.Task[None] | None = None
        self._state_write_lock = asyncio.Lock()
        self._runtime_state_sig: str | None = None

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque()
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self.lighter_order_fill_totals: dict[int, tuple[Decimal, Decimal]] = {}
        self.lighter_order_terminal_ids: set[int] = set()
        self.lighter_retry_pending_keys: set[str] = set()
        self.lighter_order_tasks_by_trade_key: dict[str, asyncio.Task[None]] = {}
        self.lighter_requeue_after_task_keys: set[str] = set()
        self._record_lock = asyncio.Lock()
        self.strategy_window_store = RollingWindowStore()
        self.strategy_window_stats: dict[StrategySide, dict[int, Any]] = {
            StrategySide.BUY: {},
            StrategySide.SELL: {},
        }
        self.strategy_epoch_activator = EpochActivator(
            model=self.strategy_model,
            confirmations=self.strategy_config.parameter_confirmations,
        )
        self.strategy_engine = StrategyEngine(
            max_frame_age_ms=self.strategy_config.max_quote_age_ms,
            early_exit_seconds=self.strategy_config.early_exit_seconds,
            max_hold_seconds=self.strategy_config.max_hold_seconds,
            epsilon=self.strategy_model.epsilon,
            buy_open_dynamic_threshold_minimum=(
                self.strategy_config.buy_dynamic_threshold_min_pct
                / PERCENT_DIVISOR
            ),
            sell_open_dynamic_threshold_minimum=(
                self.strategy_config.sell_dynamic_threshold_min_pct
                / PERCENT_DIVISOR
            ),
        )
        self.active_parameter_epoch: ParameterEpoch | None = None
        self.last_market_frame: MarketFrame | None = None
        # Presentation-only cache.  Strategy decisions continue to read
        # ``last_market_frame`` and therefore still fail closed as soon as a
        # live frame becomes unavailable.  The dashboard keeps the last valid
        # rates so a brief quote gap does not flash between values and dashes.
        self._dashboard_last_market_frame: MarketFrame | None = None
        self._strategy_frame_build_lock = asyncio.Lock()
        self._last_valid_strategy_frame_ms: int | None = None
        self._last_recorded_strategy_sample_ms: int | None = None
        self._strategy_parameter_block_reason = "strategy_windows_not_ready"
        self.last_strategy_decision: StrategyDecision | None = None
        self.last_strategy_decision_at_ms: int | None = None
        self._selected_open_candidate: OpenCandidate | None = None
        self._last_open_decision_trace_signature: tuple[Any, ...] | None = None
        self._last_open_decision_trace_ms = 0
        self._opportunity_samples: dict[StrategySide, deque[OpportunitySample]] = {
            StrategySide.BUY: deque(),
            StrategySide.SELL: deque(),
        }
        self._last_parameter_refresh_ms = 0
        self._strategy_started_at_ms = time.time_ns() // 1_000_000
        self._strategy_history_resume_pending = False
        self._strategy_history_resume_state = "not_loaded"
        self._strategy_history_resume_samples = 0
        self._strategy_history_resume_coverage_ms = 0
        self._strategy_history_resume_gap_ms: int | None = None
        self.strategy_sample_task: asyncio.Task[None] | None = None
        self.execution_loss_samples: defaultdict[
            tuple[str, str], deque[Decimal]
        ] = defaultdict(lambda: deque(maxlen=EXECUTION_SAMPLE_LIMIT_PER_BUCKET))
        self.execution_loss_sample_records: defaultdict[
            tuple[str, str, Decimal], deque[ExecutionLossSample]
        ] = defaultdict(lambda: deque(maxlen=EXECUTION_SAMPLE_LIMIT_PER_BUCKET))
        persist_samples = optional_env_bool("STRATEGY_PERSIST_EXECUTION_SAMPLES")
        self.execution_sample_persistence_enabled = persist_samples is not False
        self._execution_samples_revision = 0
        self._execution_samples_persisted_revision = 0
        self._open_execution_headroom_cache: dict[
            tuple[str, Decimal, Decimal], tuple[int, Decimal]
        ] = {}
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_in_progress = False
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0

        self.trade_event_cursor = 0
        # Set only by run().  Unit-level lifecycle tests can inject historical
        # timestamps, while a real process rejects trades created before launch.
        self.var_event_accept_after: datetime | None = None

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        self.lighter_client: SignerClient | None = None
        self._lighter_signer_lock = asyncio.Lock()
        # Keep the dedicated WebSocket as the low-latency live-open path.
        # REST remains available for close/recovery safety, not as a silent
        # downgrade for new exposure.
        self.lighter_order_entry_enabled = (
            optional_env_bool("LIGHTER_ORDER_ENTRY_WS_ENABLED") is not False
        )
        self.lighter_order_entry_rest_fallback = (
            optional_env_bool("LIGHTER_ORDER_ENTRY_REST_FALLBACK") is not False
        )
        self.lighter_order_entry: LighterOrderEntry | None = None
        self._lighter_order_entry_last_observed_ready: bool | None = None

        self.lighter_market_index = 0
        self.base_amount_multiplier = 0
        self.price_multiplier = 0

        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_order_book_ticks: dict[str, dict[int, int]] = {"bids": {}, "asks": {}}
        self.lighter_vwap_cache: dict[tuple[str, int], tuple[Decimal, Decimal]] = {}
        self.lighter_execution_tick_cache: dict[tuple[str, int], tuple[int, int]] = {}
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_order_book_offset = 0
        self.lighter_order_book_nonce: int | None = None
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_sequence_gap = False
        self.lighter_order_book_lock = asyncio.Lock()
        self.lighter_book_received_monotonic: float | None = None
        self.lighter_private_stream_ready = False
        self._last_lighter_order_refresh_at = 0.0
        self.market_generation = 0

        # Market data and private execution reports must not contend with order
        # submission.  ``lighter_ws_task`` remains as a compatibility alias for
        # older local subclasses/tests; production uses the two tasks below.
        self.lighter_ws_task: asyncio.Task[None] | None = None
        self.lighter_market_ws_task: asyncio.Task[None] | None = None
        self.lighter_private_ws_task: asyncio.Task[None] | None = None
        self.trade_task: asyncio.Task[None] | None = None
        self.strategy_signal_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        dashboard_setting = optional_env_bool("OPERATIONS_DASHBOARD_ENABLED")
        self.operations_dashboard_enabled = dashboard_setting is not False
        try:
            self.operations_dashboard_port = int(
                optional_env("OPERATIONS_DASHBOARD_PORT")
                or str(DEFAULT_OPERATIONS_DASHBOARD_PORT)
            )
        except ValueError as exc:
            raise RuntimeError("OPERATIONS_DASHBOARD_PORT must be an integer") from exc
        if not 1 <= self.operations_dashboard_port <= 65535:
            raise RuntimeError("OPERATIONS_DASHBOARD_PORT must be between 1 and 65535")
        self.operations_dashboard_server: OperationsDashboardServer | None = None
        self._operations_dashboard_sequence = 0
        self._operator_action_lock = asyncio.Lock()
        self._operator_action_inflight = False
        self.reconcile_task: asyncio.Task[None] | None = None
        self.lighter_order_watchdog_task: asyncio.Task[None] | None = None
        self.var_intent_watchdog_task: asyncio.Task[None] | None = None
        self.hedge_tasks: set[asyncio.Task[None]] = set()
        self.last_auto_var_order_status = "-"
        self.last_auto_var_close_status = "-"
        self._last_auto_var_order_at = 0.0
        self._last_round_closed_at = 0.0
        self._auto_var_order_inflight = False
        self._canary_round_count = 0
        self._canary_cumulative_loss_usd = Decimal("0")
        self._canary_consecutive_losses = 0
        self._canary_completed_close_keys: set[str] = set()
        self._round_cooldown_close_keys: set[str] = set()
        self._canary_session_state = CANARY_SESSION_OBSERVING
        self._max_hold_alerted_trade_keys: set[str] = set()
        self._close_stability_trade_key: str | None = None
        self._close_zero_wear_started_ms: int | None = None
        self._close_zero_wear_last_sample_ms: int | None = None
        self._close_zero_wear_last_above = False
        self._close_zero_wear_intervals: deque[tuple[int, int]] = deque()
        self._close_range_deferral_started_ms: dict[str, int] = {}
        self.pending_var_intent: VarOrderIntent | None = None
        self.automation_paused = False
        self.automation_pause_reason = "-"
        self.operator_open_paused = False
        self.automation_ready = False
        self.last_reconcile_status = "not checked"
        self.last_reconcile_outcome = AccountReconcileOutcome.UNKNOWN
        self.reconcile_degraded_reason: str | None = "not checked"
        self.last_account_snapshot: AccountSnapshot | None = None
        self._reconcile_failure_count = 0
        self._reconcile_pause_reason: str | None = None
        self._reconcile_mismatch_first_token: str | None = None
        self._reconcile_mismatch_first_monotonic: float | None = None

    async def persist_runtime_state(self) -> None:
        # Serialize the complete snapshot-and-write operation.  Taking a
        # snapshot before waiting for the write lock allows an older caller to
        # overwrite a newer intent/record snapshot when the two calls race.
        async with self._state_write_lock:
            async with self._record_lock:
                snapshot_asset = self.variational_ticker
                snapshot_ticker = self.ticker
                ordered_records = [
                    self.records[key]
                    for key in self.record_order
                    if key in self.records
                ]
                recovery_records = runtime_recovery_records(ordered_records)
                records = copy.deepcopy(
                    [record.to_payload() for record in recovery_records]
                )
                recovery_order_ids: set[int] = set()
                for record in recovery_records:
                    recovery_order_ids.update(record.lighter_client_order_ids)
                    if record.lighter_reserved_client_order_id is not None:
                        recovery_order_ids.add(record.lighter_reserved_client_order_id)
                lighter_order_cumulative = [
                    {
                        "client_order_id": client_order_id,
                        "filled_base": decimal_to_str(
                            self.lighter_order_fill_totals.get(
                                client_order_id,
                                (Decimal("0"), Decimal("0")),
                            )[0]
                        ),
                        "filled_quote": decimal_to_str(
                            self.lighter_order_fill_totals.get(
                                client_order_id,
                                (Decimal("0"), Decimal("0")),
                            )[1]
                        ),
                        "terminal": client_order_id
                        in self.lighter_order_terminal_ids,
                    }
                    for client_order_id in sorted(recovery_order_ids)
                ]
                sample_revision = self._execution_samples_revision
                sample_snapshot = [
                    sample
                    for values in self.execution_loss_sample_records.values()
                    for sample in values
                ]
                intent = self.pending_var_intent
                intent_payload = (
                    {
                        "phase": intent.phase,
                        "side": intent.side,
                        "amount": decimal_to_str(intent.amount),
                        "market": intent.market,
                        "request_id": intent.request_id,
                        "order_id": intent.order_id,
                        "provisional_trade_key": intent.provisional_trade_key,
                        "state": intent.state,
                        "trace_id": intent.trace_id,
                        "firm_quote_id": intent.firm_quote_id,
                        "firm_price": decimal_to_str(intent.firm_price),
                        "firm_qty": decimal_to_str(intent.firm_qty),
                        "firm_target_notional_usd": decimal_to_str(
                            intent.firm_target_notional_usd
                        ),
                        "firm_guard_pnl": decimal_to_str(intent.firm_guard_pnl),
                        "firm_required_pnl": decimal_to_str(intent.firm_required_pnl),
                        "execution_reserve_usd": decimal_to_str(
                            intent.execution_reserve_usd
                        ),
                        "lighter_vwap": decimal_to_str(intent.lighter_vwap),
                        "lighter_quote_age_ms": intent.lighter_quote_age_ms,
                        "lighter_client_order_index": (
                            intent.lighter_client_order_index
                        ),
                        "lighter_client_order_collision": (
                            intent.lighter_client_order_collision
                        ),
                        "sent_at": intent.sent_at_iso,
                        "prepared_at": intent.prepared_at_iso,
                        "adaptive_strategy_context": copy.deepcopy(
                            intent.adaptive_strategy_context
                        ),
                        "strategy_tag": intent.strategy_tag,
                        "commit_rfq_id": intent.commit_rfq_id,
                        "confirmed_trade_key": intent.confirmed_trade_key,
                    }
                    if intent is not None
                    else None
                )
                payload = {
                    "version": 2,
                    "strategy_model": ADAPTIVE_MODEL_VERSION,
                    "strategy_model_hash": self.strategy_model.model_hash,
                    "strategy_config_hash": self.strategy_config_hash,
                    "asset": snapshot_asset,
                    "ticker": snapshot_ticker,
                    "records": records,
                    "lighter_order_cumulative": lighter_order_cumulative,
                    "pending_var_intent": intent_payload,
                    "last_round_closed_at": self._last_round_closed_at,
                    "canary_session": {
                        "round_count": self._canary_round_count,
                        "cumulative_loss_usd": decimal_to_str(
                            self._canary_cumulative_loss_usd
                        ),
                        "consecutive_losses": self._canary_consecutive_losses,
                        "state": self._canary_session_state,
                    },
                    "automation_paused": self.automation_paused,
                    "automation_pause_reason": self.automation_pause_reason,
                    "operator_open_paused": self.operator_open_paused,
                }
            state_sig = json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            if state_sig != self._runtime_state_sig:
                payload["saved_at"] = utc_now()
                await asyncio.to_thread(self._write_json_atomic, RUNTIME_STATE_FILE, payload)
                self._runtime_state_sig = state_sig
            if (
                self.execution_sample_persistence_enabled
                and sample_revision > self._execution_samples_persisted_revision
                and snapshot_asset
            ):
                await asyncio.to_thread(
                    write_execution_sample_records,
                    EXECUTION_SAMPLES_FILE,
                    EXECUTION_SAMPLE_VERSION,
                    snapshot_asset,
                    sample_snapshot,
                )
                if self.variational_ticker == snapshot_asset:
                    self._execution_samples_persisted_revision = sample_revision

    def _settled_execution_keys_locked(self) -> set[str]:
        ordered_records = [
            self.records[key] for key in self.record_order if key in self.records
        ]
        _, completed_rounds = build_trade_rounds(ordered_records)
        settled_keys: set[str] = set()
        for trade_round in completed_rounds:
            legs = (trade_round.open_record, trade_round.close_record)
            if all(
                record.var_fill_source in {"event", "portfolio"}
                and record.last_variational_status == "filled"
                and lighter_hedge_filled(record)
                for record in legs
            ):
                settled_keys.update(record.trade_key for record in legs)
        return settled_keys

    async def prune_settled_execution_state(self) -> int:
        """Bound display history without evicting exposure or recovery state.

        This is deliberately a cold-path operation: it only removes fully
        settled round legs after their final fill/reconciliation.  Every
        unresolved, failed, recovery, queued, or current-position record is
        retained regardless of age.
        """
        async with self._record_lock:
            settled_keys = self._settled_execution_keys_locked()
            settled_in_order = [
                key for key in self.record_order if key in settled_keys and key in self.records
            ]
            prune_candidates = set(
                settled_in_order[:-SETTLED_EXECUTION_HISTORY_LIMIT]
            )
            removable_keys = {
                key
                for key in prune_candidates
                if (
                    (task := self.lighter_order_tasks_by_trade_key.get(key)) is None
                    or task.done()
                )
            }
            if not removable_keys:
                return 0

            removed_order_ids: set[int] = set()
            for key in removable_keys:
                record = self.records.pop(key, None)
                if record is not None:
                    removed_order_ids.update(record.lighter_client_order_ids)
                    if record.lighter_client_order_id is not None:
                        removed_order_ids.add(record.lighter_client_order_id)
                    if record.lighter_reserved_client_order_id is not None:
                        removed_order_ids.add(record.lighter_reserved_client_order_id)
                self.lighter_retry_pending_keys.discard(key)
                self.lighter_requeue_after_task_keys.discard(key)
                self._canary_completed_close_keys.discard(key)
                self._round_cooldown_close_keys.discard(key)
                self._max_hold_alerted_trade_keys.discard(key)
                task = self.lighter_order_tasks_by_trade_key.get(key)
                if task is None or task.done():
                    self.lighter_order_tasks_by_trade_key.pop(key, None)

            for client_order_id, trade_key in list(
                self.lighter_client_order_to_trade_key.items()
            ):
                if trade_key not in removable_keys:
                    continue
                self.lighter_client_order_to_trade_key.pop(client_order_id, None)
                self.lighter_order_fill_totals.pop(client_order_id, None)
                self.lighter_order_terminal_ids.discard(client_order_id)

            for client_order_id in removed_order_ids:
                mapped_trade_key = self.lighter_client_order_to_trade_key.get(
                    client_order_id
                )
                if mapped_trade_key in removable_keys:
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
                if mapped_trade_key is None or mapped_trade_key in removable_keys:
                    self.lighter_order_fill_totals.pop(client_order_id, None)
                    self.lighter_order_terminal_ids.discard(client_order_id)

            self.record_order = deque(
                key for key in self.record_order if key not in removable_keys
            )
            return len(removable_keys)

    async def discard_stale_manual_flat_record(
        self,
        record: OrderLifecycle,
    ) -> bool:
        """Drop local manual exposure only after both accounts confirm flat.

        A user can close both legs while the runtime is stopped, so no local
        close event exists to pair with the restored manual open. Account
        reconciliation is the authoritative recovery boundary in that case.
        Adaptive records and any in-flight hedge remain ineligible.
        """

        async with self._record_lock:
            current = self.records.get(record.trade_key)
            task = self.lighter_order_tasks_by_trade_key.get(record.trade_key)
            if (
                current is not record
                or record.strategy_tag != MANUAL_STRATEGY_TAG
                or not lighter_hedge_filled(record)
                or self.pending_var_intent is not None
                or (task is not None and not task.done())
            ):
                return False

            order_ids = set(record.lighter_client_order_ids)
            if record.lighter_client_order_id is not None:
                order_ids.add(record.lighter_client_order_id)
            if record.lighter_reserved_client_order_id is not None:
                order_ids.add(record.lighter_reserved_client_order_id)

            self.records.pop(record.trade_key, None)
            self.record_order = deque(
                key for key in self.record_order if key != record.trade_key
            )
            self.lighter_retry_pending_keys.discard(record.trade_key)
            self.lighter_requeue_after_task_keys.discard(record.trade_key)
            self.lighter_order_tasks_by_trade_key.pop(record.trade_key, None)
            self._canary_completed_close_keys.discard(record.trade_key)
            self._round_cooldown_close_keys.discard(record.trade_key)
            self._max_hold_alerted_trade_keys.discard(record.trade_key)
            for client_order_id in order_ids:
                if (
                    self.lighter_client_order_to_trade_key.get(client_order_id)
                    == record.trade_key
                ):
                    self.lighter_client_order_to_trade_key.pop(
                        client_order_id,
                        None,
                    )
                self.lighter_order_fill_totals.pop(client_order_id, None)
                self.lighter_order_terminal_ids.discard(client_order_id)
            return True

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)
        os.replace(tmp_path, path)

    async def load_execution_samples_for_asset(self, asset: str) -> None:
        self.execution_loss_samples.clear()
        self.execution_loss_sample_records.clear()
        self._execution_samples_revision = 0
        self._execution_samples_persisted_revision = 0
        self._open_execution_headroom_cache.clear()
        if not self.execution_sample_persistence_enabled:
            return
        stored_records = await asyncio.to_thread(
            read_execution_sample_records,
            EXECUTION_SAMPLES_FILE,
            EXECUTION_SAMPLE_VERSION,
            asset,
            notional_usd=self.strategy_config.order_notional_usd,
        )
        for sample in stored_records:
            if sample.loss_usd > 0:
                self.execution_loss_samples[(sample.phase, sample.side)].append(
                    sample.loss_usd
                )
            bucket = sample.notional_bucket or Decimal("0")
            self.execution_loss_sample_records[
                (sample.phase, sample.side, bucket)
            ].append(sample)

    async def load_runtime_state(self, asset: str) -> bool:
        if not RUNTIME_STATE_FILE.exists():
            return False
        try:
            payload = await asyncio.to_thread(self._read_json_file, RUNTIME_STATE_FILE)
        except Exception as exc:
            raise RuntimeError(f"Runtime state is unreadable: {exc}") from exc
        if str(payload.get("asset") or "").upper() != asset.upper():
            raise RuntimeError(
                "Runtime state asset does not match the activated market; "
                "refusing to overwrite recovery evidence"
            )

        raw_rows = payload.get("records", [])
        raw_lighter_cumulative = payload.get("lighter_order_cumulative")
        raw_intent = payload.get("pending_var_intent")
        if not isinstance(raw_rows, list):
            raise RuntimeError("Runtime state records must be a list")
        if raw_lighter_cumulative is not None and not isinstance(
            raw_lighter_cumulative,
            list,
        ):
            raise RuntimeError("Runtime state Lighter cumulative data is malformed")
        if raw_intent is not None and not isinstance(raw_intent, dict):
            raise RuntimeError("Runtime state pending_var_intent is malformed")
        has_saved_exposure = bool(raw_rows) or isinstance(raw_intent, dict)
        if payload.get("version") != 2:
            if has_saved_exposure:
                raise RuntimeError(
                    "Pre-v2 runtime state contains a position or pending intent; "
                    "adaptive startup is refused until the account is reconciled to zero"
                )
            return False
        model_state_mismatch = (
            payload.get("strategy_model") != ADAPTIVE_MODEL_VERSION
            or payload.get("strategy_model_hash") != self.strategy_model.model_hash
        )
        manual_only_model_migration = bool(
            model_state_mismatch
            and raw_intent is None
            and raw_rows
            and all(
                isinstance(row, dict)
                and row.get("strategy_tag") == MANUAL_STRATEGY_TAG
                for row in raw_rows
            )
        )
        saved_pause_reason = str(payload.get("automation_pause_reason") or "")
        migration_reconciliation_pause = bool(
            manual_only_model_migration
            and payload.get("automation_paused") is True
            and saved_pause_reason.startswith("Account reconciliation failed:")
        )
        flat_v4_telemetry_migration = bool(
            model_state_mismatch
            and not has_saved_exposure
            and payload.get("strategy_model") == ADAPTIVE_MODEL_VERSION
            and payload.get("strategy_model_hash")
            in MIGRATABLE_FLAT_V4_MODEL_HASHES
        )
        if model_state_mismatch:
            if has_saved_exposure and not manual_only_model_migration:
                raise RuntimeError(
                    f"Runtime state model does not match {ADAPTIVE_MODEL_VERSION}; "
                    "startup with exposure is refused"
                )
            if bool(payload.get("automation_paused")) and not migration_reconciliation_pause:
                raise RuntimeError(
                    "Runtime state model changed but contains an automation safety pause; "
                    "automatic reset is refused"
                )
            if not flat_v4_telemetry_migration and not manual_only_model_migration:
                return False

        loaded_records: list[OrderLifecycle] = []
        loaded_trade_keys: set[str] = set()
        loaded_lighter_ids: dict[int, str] = {}
        for row in raw_rows:
            if not isinstance(row, dict):
                raise RuntimeError("Runtime state contains a malformed record row")
            record = OrderLifecycle.from_payload(row)
            if record is None or not record.trade_key:
                raise RuntimeError("Runtime state contains an invalid execution record")
            if record.trade_key in loaded_trade_keys:
                raise RuntimeError(
                    f"Runtime state contains duplicate trade key: {record.trade_key}"
                )
            loaded_trade_keys.add(record.trade_key)
            if record.side not in {"buy", "sell"} or record.asset != asset.upper():
                raise RuntimeError(
                    "Runtime state record side/asset does not match the activated market"
                )
            record_lighter_ids = set(record.lighter_client_order_ids)
            if record.lighter_reserved_client_order_id is not None:
                record_lighter_ids.add(record.lighter_reserved_client_order_id)
            for client_order_id in record_lighter_ids:
                if not (0 < client_order_id <= LIGHTER_CLIENT_ORDER_INDEX_MAX):
                    raise RuntimeError(
                        "Runtime state contains an invalid Lighter client order id"
                    )
                previous_trade_key = loaded_lighter_ids.get(client_order_id)
                if (
                    previous_trade_key is not None
                    and previous_trade_key != record.trade_key
                ):
                    raise RuntimeError(
                        "Runtime state reuses one Lighter client order id across records"
                    )
                loaded_lighter_ids[client_order_id] = record.trade_key
            if record.strategy_tag not in {MANUAL_STRATEGY_TAG, ADAPTIVE_MODEL_VERSION}:
                raise RuntimeError(
                    f"Unknown strategy tag in runtime state: {record.strategy_tag}"
                )
            if record.strategy_tag == ADAPTIVE_MODEL_VERSION:
                if record.strategy_phase == "open":
                    frozen = open_candidate_from_payload(
                        record.adaptive_strategy_context
                    )
                    if frozen is None:
                        raise RuntimeError(
                            "Adaptive runtime state is missing its frozen ParameterEpoch"
                        )
                    if (
                        frozen.epoch.model_version != ADAPTIVE_MODEL_VERSION
                        or frozen.epoch.model_hash != self.strategy_model.model_hash
                        or frozen.direction.value != record.side.upper()
                    ):
                        raise RuntimeError(
                            "Adaptive runtime state has a mismatched frozen ParameterEpoch"
                        )
                elif record.strategy_phase == "close":
                    context = record.adaptive_strategy_context
                    if (
                        not isinstance(context, dict)
                        or context.get("schema") != "adaptive-close-context-v1"
                        or context.get("strategyTag") != ADAPTIVE_MODEL_VERSION
                    ):
                        raise RuntimeError(
                            "Adaptive close state is missing its frozen close context"
                        )
            if record.strategy_phase == "emergency_close":
                context = record.adaptive_strategy_context
                if (
                    not isinstance(context, dict)
                    or context.get("schema")
                    != "adaptive-emergency-close-context-v1"
                    or context.get("strategyTag") != record.strategy_tag
                    or not str(context.get("openTradeKey") or "").strip()
                ):
                    raise RuntimeError(
                        "Emergency close state is missing its recovery context"
                    )
            loaded_records.append(record)

        loaded_records_by_key = {
            record.trade_key: record for record in loaded_records
        }
        for index, record in enumerate(loaded_records):
            if record.lighter_side is not None and record.lighter_side not in {
                "BUY",
                "SELL",
            }:
                raise RuntimeError(
                    "Runtime state contains an invalid Lighter order side"
                )
            for timestamp in (
                record.var_fill_ts_iso,
                record.lighter_fill_ts_iso,
                record.lighter_submitted_at_iso,
            ):
                if timestamp is not None and (
                    not isinstance(timestamp, str)
                    or parse_iso_datetime(timestamp) is None
                ):
                    raise RuntimeError(
                        "Runtime state contains an invalid execution timestamp"
                    )
            filled_base = record.lighter_filled_qty or Decimal("0")
            filled_quote = record.lighter_filled_quote or Decimal("0")
            if filled_base == 0:
                if filled_quote != 0 or record.lighter_fill_price is not None:
                    raise RuntimeError(
                        "Runtime state contains inconsistent Lighter fill totals"
                    )
            elif (
                record.lighter_fill_price is None
                or record.lighter_fill_price != filled_quote / filled_base
            ):
                raise RuntimeError(
                    "Runtime state contains inconsistent Lighter fill VWAP"
                )
            if record.strategy_phase == "open" and record.var_fill_price is not None:
                expected_open_notional = record.qty * record.var_fill_price
                if record.open_notional_usd != expected_open_notional:
                    raise RuntimeError(
                        "Runtime state open notional does not match its confirmed fill"
                    )
            if record.strategy_phase == "close" and record.strategy_tag == ADAPTIVE_MODEL_VERSION:
                current_open, _rounds = build_trade_rounds(loaded_records[:index])
                frozen = (
                    open_candidate_from_payload(current_open.adaptive_strategy_context)
                    if current_open is not None
                    else None
                )
                context = record.adaptive_strategy_context
                if (
                    current_open is None
                    or frozen is None
                    or current_open.asset != record.asset
                    or current_open.side == record.side
                    or current_open.qty != record.qty
                    or not isinstance(context, dict)
                    or context.get("epochId") != frozen.epoch.epoch_id
                ):
                    raise RuntimeError(
                        "Adaptive close state does not match its frozen open position"
                    )
            if record.strategy_phase == "emergency_close":
                context = record.adaptive_strategy_context
                assert isinstance(context, dict)
                open_trade_key = str(context.get("openTradeKey") or "").strip()
                open_record = loaded_records_by_key.get(open_trade_key)
                if (
                    open_record is None
                    or open_record.asset != record.asset
                    or open_record.side == record.side
                    or open_record.qty != record.qty
                ):
                    raise RuntimeError(
                        "Emergency close state does not match its source position"
                    )

        loaded_fill_totals: dict[int, tuple[Decimal, Decimal]] = {}
        loaded_terminal_ids: set[int] = set()
        if raw_lighter_cumulative is not None:
            for row in raw_lighter_cumulative:
                if not isinstance(row, dict):
                    raise RuntimeError(
                        "Runtime state contains a malformed Lighter cumulative row"
                    )
                raw_client_order_id = row.get("client_order_id")
                if isinstance(raw_client_order_id, bool):
                    client_order_id = None
                elif isinstance(raw_client_order_id, int):
                    client_order_id = raw_client_order_id
                elif (
                    isinstance(raw_client_order_id, str)
                    and raw_client_order_id.strip().isdigit()
                ):
                    client_order_id = int(raw_client_order_id.strip())
                else:
                    client_order_id = None
                filled_base = to_decimal(row.get("filled_base"))
                filled_quote = to_decimal(row.get("filled_quote"))
                terminal = row.get("terminal")
                if (
                    client_order_id is None
                    or client_order_id not in loaded_lighter_ids
                    or client_order_id in loaded_fill_totals
                    or filled_base is None
                    or filled_base < 0
                    or filled_quote is None
                    or filled_quote < 0
                    or not isinstance(terminal, bool)
                ):
                    raise RuntimeError(
                        "Runtime state contains invalid Lighter cumulative data"
                    )
                loaded_fill_totals[client_order_id] = (
                    filled_base,
                    filled_quote,
                )
                if terminal:
                    loaded_terminal_ids.add(client_order_id)
            if set(loaded_fill_totals) != set(loaded_lighter_ids):
                raise RuntimeError(
                    "Runtime state is missing per-order Lighter cumulative data"
                )
        else:
            # Compatibility is safe only when an aggregate can be assigned to
            # one unambiguous order.  Multiple IOC attempts require the exact
            # per-order cumulative values to avoid duplicate residual hedges.
            for record in loaded_records:
                actual_ids = list(record.lighter_client_order_ids)
                total_base = record.lighter_filled_qty or Decimal("0")
                total_quote = record.lighter_filled_quote or Decimal("0")
                if (total_base > 0 or total_quote > 0) and len(actual_ids) != 1:
                    raise RuntimeError(
                        "Runtime state cannot safely reconstruct multi-order "
                        "Lighter partial fills"
                    )
                for client_order_id in set(actual_ids) | (
                    {record.lighter_reserved_client_order_id}
                    if record.lighter_reserved_client_order_id is not None
                    else set()
                ):
                    loaded_fill_totals[client_order_id] = (
                        (total_base, total_quote)
                        if actual_ids == [client_order_id]
                        else (Decimal("0"), Decimal("0"))
                    )
                if record.lighter_outcome_final:
                    loaded_terminal_ids.update(actual_ids)

        for record in loaded_records:
            aggregate_base = sum(
                (
                    loaded_fill_totals.get(
                        client_order_id,
                        (Decimal("0"), Decimal("0")),
                    )[0]
                    for client_order_id in record.lighter_client_order_ids
                ),
                Decimal("0"),
            )
            aggregate_quote = sum(
                (
                    loaded_fill_totals.get(
                        client_order_id,
                        (Decimal("0"), Decimal("0")),
                    )[1]
                    for client_order_id in record.lighter_client_order_ids
                ),
                Decimal("0"),
            )
            if (
                aggregate_base != (record.lighter_filled_qty or Decimal("0"))
                or aggregate_quote != (record.lighter_filled_quote or Decimal("0"))
            ):
                raise RuntimeError(
                    "Runtime state Lighter aggregate does not match per-order totals"
                )
            all_terminal = bool(record.lighter_client_order_ids) and set(
                record.lighter_client_order_ids
            ).issubset(loaded_terminal_ids)
            if record.lighter_outcome_final != all_terminal:
                raise RuntimeError(
                    "Runtime state Lighter terminal flags are inconsistent"
                )

        if not self.args.auto_hedge and (
            isinstance(raw_intent, dict)
            or any(
                record.strategy_tag == ADAPTIVE_MODEL_VERSION
                for record in loaded_records
            )
        ):
            raise RuntimeError(
                "Adaptive runtime exposure cannot be restored with --no-hedge"
            )

        # A private execution report can become terminal before a delayed
        # Variational portfolio-recovery event is processed.  Older runtime
        # snapshots may therefore contain exact, terminal Lighter fill totals
        # whose lifecycle text was incorrectly rolled back to recovery_check.
        # The persisted cumulative fills are authoritative and do not require
        # a network reconciliation before restoring HEDGED.
        for record in loaded_records:
            self._normalize_terminal_lighter_hedge_locked(record)

        recovery_records = runtime_recovery_records(loaded_records)
        recovery_order_ids: set[int] = set()
        for record in recovery_records:
            recovery_order_ids.update(record.lighter_client_order_ids)
            if record.lighter_reserved_client_order_id is not None:
                recovery_order_ids.add(record.lighter_reserved_client_order_id)
        recovery_fill_totals = {
            client_order_id: totals
            for client_order_id, totals in loaded_fill_totals.items()
            if client_order_id in recovery_order_ids
        }
        recovery_terminal_ids = loaded_terminal_ids & recovery_order_ids
        async with self._record_lock:
            self.records = {record.trade_key: record for record in recovery_records}
            self.record_order = deque(record.trade_key for record in recovery_records)
            self.lighter_client_order_to_trade_key.clear()
            self.lighter_order_fill_totals = recovery_fill_totals
            self.lighter_order_terminal_ids = recovery_terminal_ids
            for record in recovery_records:
                record_lighter_ids = set(record.lighter_client_order_ids)
                if record.lighter_reserved_client_order_id is not None:
                    record_lighter_ids.add(record.lighter_reserved_client_order_id)
                for client_order_id in record_lighter_ids:
                    self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
        if not any(self.execution_loss_samples.values()):
            for record in loaded_records:
                if (
                    record.execution_loss_recorded
                    and record.execution_loss_usd is not None
                    and record.execution_loss_usd > 0
                    and record.strategy_phase
                ):
                    self.execution_loss_samples[
                        (record.strategy_phase, record.side.upper())
                    ].append(record.execution_loss_usd)
                    notional = var_open_notional_usd(record)
                    if notional is not None and notional > 0:
                        sample = ExecutionLossSample.from_loss(
                            timestamp=record.lighter_fill_ts_iso or record.var_fill_ts_iso,
                            asset=record.asset,
                            phase=record.strategy_phase,
                            side=record.side,
                            notional_usd=notional,
                            loss_usd=record.execution_loss_usd,
                        )
                        assert sample.notional_bucket is not None
                        self.execution_loss_sample_records[
                            (sample.phase, sample.side, sample.notional_bucket)
                        ].append(sample)
                    self._execution_samples_revision += 1

        raw_automation_paused = payload.get("automation_paused", False)
        if not isinstance(raw_automation_paused, bool):
            raise RuntimeError("Runtime state automation_paused must be boolean")
        raw_pause_reason = payload.get("automation_pause_reason", "-")
        if not isinstance(raw_pause_reason, str):
            raise RuntimeError("Runtime state automation_pause_reason must be text")
        if raw_automation_paused:
            saved_reason = raw_pause_reason.strip() or "Recovered safety pause"
            self.pause_automation(saved_reason)
            if saved_reason.startswith(
                (
                    "Account reconciliation",
                    "Lighter hedge failed:",
                    "Lighter order status unresolved after",
                    "Recovered an unresolved Var order",
                    "Var commit was accepted but its position/fill could not be confirmed",
                    "Var fill direction/market mismatch",
                    "Var fill matched the pending side/market",
                )
            ):
                self._reconcile_pause_reason = saved_reason

        raw_operator_open_paused = payload.get("operator_open_paused", False)
        if not isinstance(raw_operator_open_paused, bool):
            raise RuntimeError("Runtime state operator_open_paused must be boolean")
        self.operator_open_paused = raw_operator_open_paused

        if isinstance(raw_intent, dict):
            def intent_text(key: str, *, required: bool = False) -> str | None:
                raw = raw_intent.get(key)
                if raw is None:
                    if required:
                        raise RuntimeError(
                            f"Runtime state pending intent is missing {key}"
                        )
                    return None
                if not isinstance(raw, str):
                    raise RuntimeError(
                        f"Runtime state pending intent {key} must be text"
                    )
                value = raw.strip()
                if required and not value:
                    raise RuntimeError(
                        f"Runtime state pending intent is missing {key}"
                    )
                return value or None

            def intent_decimal(
                key: str,
                *,
                required: bool = False,
                positive: bool = False,
                nonnegative: bool = False,
            ) -> Decimal | None:
                raw = raw_intent.get(key)
                if raw is None:
                    if required:
                        raise RuntimeError(
                            f"Runtime state pending intent is missing {key}"
                        )
                    return None
                value = to_decimal(raw)
                if (
                    value is None
                    or (positive and value <= 0)
                    or (nonnegative and value < 0)
                ):
                    raise RuntimeError(
                        f"Runtime state pending intent has invalid {key}"
                    )
                return value

            phase = intent_text("phase", required=True)
            side = intent_text("side", required=True)
            market = intent_text("market", required=True)
            state = intent_text("state", required=True)
            strategy_tag = intent_text("strategy_tag", required=True)
            amount = intent_decimal("amount", required=True, positive=True)
            assert phase is not None and side is not None and market is not None
            assert state is not None and strategy_tag is not None and amount is not None
            side = side.upper()
            market = market.upper()
            if phase not in {"open", "close", "emergency_close"}:
                raise RuntimeError("Runtime state pending intent has invalid phase")
            if side not in {"BUY", "SELL"} or market != asset.upper():
                raise RuntimeError(
                    "Runtime state pending intent side/market is invalid"
                )
            valid_intent_states = {
                VAR_INTENT_QUOTING,
                VAR_INTENT_PREPARED,
                VAR_INTENT_COMMITTING,
                VAR_INTENT_COMMIT_AMBIGUOUS,
                VAR_INTENT_COMMITTED,
            }
            if state not in valid_intent_states:
                raise RuntimeError("Runtime state pending intent has invalid state")
            if (
                (phase in {"open", "close"} and strategy_tag != ADAPTIVE_MODEL_VERSION)
                or strategy_tag not in {ADAPTIVE_MODEL_VERSION, MANUAL_STRATEGY_TAG}
                or (phase != "emergency_close" and strategy_tag == MANUAL_STRATEGY_TAG)
            ):
                raise RuntimeError("Pending v2 intent has an unknown strategy tag")

            sent_at_iso = intent_text("sent_at", required=True)
            prepared_at_iso = intent_text("prepared_at")
            if sent_at_iso is None or parse_iso_datetime(sent_at_iso) is None:
                raise RuntimeError("Runtime state pending intent has invalid sent_at")
            prepared_state = state != VAR_INTENT_QUOTING
            if prepared_state and (
                prepared_at_iso is None
                or parse_iso_datetime(prepared_at_iso) is None
            ):
                raise RuntimeError(
                    "Runtime state prepared intent has invalid prepared_at"
                )

            raw_collision = raw_intent.get("lighter_client_order_collision", 0)
            if isinstance(raw_collision, bool) or not isinstance(raw_collision, int):
                raise RuntimeError(
                    "Runtime state pending intent has invalid Lighter collision"
                )
            if not (0 <= raw_collision < LIGHTER_CLIENT_ORDER_COLLISION_LIMIT):
                raise RuntimeError(
                    "Runtime state pending intent has invalid Lighter collision"
                )
            raw_lighter_index = raw_intent.get("lighter_client_order_index")
            if raw_lighter_index is None:
                lighter_client_order_index = None
            elif isinstance(raw_lighter_index, bool) or not isinstance(
                raw_lighter_index,
                int,
            ):
                raise RuntimeError(
                    "Runtime state pending intent has invalid Lighter order id"
                )
            else:
                lighter_client_order_index = raw_lighter_index
            if (
                lighter_client_order_index is not None
                and not (
                    0
                    < lighter_client_order_index
                    <= LIGHTER_CLIENT_ORDER_INDEX_MAX
                )
            ):
                raise RuntimeError(
                    "Runtime state pending intent has invalid Lighter order id"
                )

            raw_quote_age = raw_intent.get("lighter_quote_age_ms")
            if raw_quote_age is None:
                lighter_quote_age_ms = None
            elif (
                isinstance(raw_quote_age, bool)
                or not isinstance(raw_quote_age, int)
                or raw_quote_age < 0
            ):
                raise RuntimeError(
                    "Runtime state pending intent has invalid Lighter quote age"
                )
            else:
                lighter_quote_age_ms = raw_quote_age

            firm_quote_id = intent_text("firm_quote_id")
            trace_id = intent_text("trace_id")
            firm_price = intent_decimal("firm_price", positive=True)
            firm_qty = intent_decimal("firm_qty", positive=True)
            firm_target_notional_usd = (
                intent_decimal("firm_target_notional_usd", positive=True)
                or amount
            )
            if prepared_state and (
                firm_quote_id is None
                or trace_id is None
                or firm_price is None
                or firm_qty is None
                or lighter_client_order_index is None
            ):
                raise RuntimeError(
                    "Runtime state prepared intent is missing Firm/Lighter metadata"
                )

            request_id = intent_text("request_id")
            order_id = intent_text("order_id")
            provisional_trade_key = intent_text("provisional_trade_key")
            confirmed_trade_key = intent_text("confirmed_trade_key")
            commit_rfq_id = intent_text("commit_rfq_id")
            for referenced_key in (
                provisional_trade_key,
                confirmed_trade_key,
            ):
                if referenced_key is not None and referenced_key not in loaded_trade_keys:
                    raise RuntimeError(
                        "Runtime state pending intent references a missing record"
                    )
            if state == VAR_INTENT_COMMITTED and provisional_trade_key is None:
                raise RuntimeError(
                    "Runtime state committed intent is missing its provisional record"
                )
            if lighter_client_order_index in loaded_lighter_ids:
                owner_key = loaded_lighter_ids[lighter_client_order_index]
                if owner_key not in {provisional_trade_key, confirmed_trade_key}:
                    raise RuntimeError(
                        "Runtime state pending intent reuses another Lighter order id"
                    )

            raw_context = raw_intent.get("adaptive_strategy_context")
            if raw_context is not None and not isinstance(raw_context, dict):
                raise RuntimeError(
                    "Runtime state pending intent has malformed strategy context"
                )
            adaptive_context = copy.deepcopy(raw_context)
            if prepared_state and phase == "open":
                frozen = open_candidate_from_payload(adaptive_context)
                if (
                    frozen is None
                    or frozen.epoch.model_hash != self.strategy_model.model_hash
                    or frozen.epoch.config_hash != self.strategy_config_hash
                    or frozen.direction.value != side
                ):
                    raise RuntimeError(
                        "Prepared adaptive open intent is missing its frozen ParameterEpoch"
                    )
            elif prepared_state and phase == "close":
                if (
                    not isinstance(adaptive_context, dict)
                    or adaptive_context.get("schema")
                    != "adaptive-close-context-v1"
                    or adaptive_context.get("strategyTag")
                    != ADAPTIVE_MODEL_VERSION
                ):
                    raise RuntimeError(
                        "Prepared adaptive close intent is missing its frozen close context"
                    )
            elif prepared_state and phase == "emergency_close":
                if (
                    not isinstance(adaptive_context, dict)
                    or adaptive_context.get("schema")
                    != "adaptive-emergency-close-context-v1"
                    or adaptive_context.get("strategyTag") != strategy_tag
                ):
                    raise RuntimeError(
                        "Prepared emergency intent is missing its recovery context"
                    )

            if phase == "open":
                if amount != self.strategy_config.order_notional_usd:
                    raise RuntimeError(
                        "Runtime state open intent does not use the configured target amount"
                    )
                if prepared_state:
                    assert firm_price is not None and firm_qty is not None
                    firm_notional = firm_price * firm_qty
                    if (
                        firm_notional
                        > firm_target_notional_usd
                        + FIRM_NOTIONAL_TOLERANCE_USD
                        or firm_notional
                        < firm_target_notional_usd
                        - FIRM_NOTIONAL_TOLERANCE_USD
                    ):
                        raise RuntimeError(
                            "Runtime state open intent is outside the target amount ±1U"
                        )
            elif prepared_state:
                reference_key = provisional_trade_key or confirmed_trade_key
                if reference_key is not None:
                    reference_index = next(
                        (
                            index
                            for index, record in enumerate(loaded_records)
                            if record.trade_key == reference_key
                        ),
                        len(loaded_records),
                    )
                    position_records = loaded_records[:reference_index]
                else:
                    position_records = loaded_records
                current_open, _rounds = build_trade_rounds(position_records)
                assert firm_qty is not None
                if (
                    current_open is None
                    or current_open.asset != market
                    or current_open.side.upper() == side
                    or current_open.qty != firm_qty
                ):
                    raise RuntimeError(
                        "Runtime state close intent does not match its open position"
                    )
                if phase == "close":
                    frozen = open_candidate_from_payload(
                        current_open.adaptive_strategy_context
                    )
                    if (
                        frozen is None
                        or not isinstance(adaptive_context, dict)
                        or adaptive_context.get("epochId")
                        != frozen.epoch.epoch_id
                    ):
                        raise RuntimeError(
                            "Runtime state close intent epoch does not match its open"
                        )
                else:
                    assert isinstance(adaptive_context, dict)
                    if adaptive_context.get("openTradeKey") != current_open.trade_key:
                        raise RuntimeError(
                            "Runtime state emergency intent does not match its open"
                        )

            # A COMMITTING state restored from disk no longer has a live HTTP
            # request that can resolve it.  Treat it as ambiguous so portfolio
            # recovery is allowed instead of waiting forever on a monotonic
            # timestamp that cannot survive a restart.
            restored_state = (
                VAR_INTENT_COMMIT_AMBIGUOUS
                if state == VAR_INTENT_COMMITTING
                else state
            )
            self.pending_var_intent = VarOrderIntent(
                phase=phase,
                side=side,
                amount=amount,
                sent_monotonic=time.monotonic(),
                market=market,
                request_id=request_id,
                order_id=order_id,
                provisional_trade_key=provisional_trade_key,
                state=restored_state,
                trace_id=trace_id,
                firm_quote_id=firm_quote_id,
                firm_price=firm_price,
                firm_qty=firm_qty,
                firm_target_notional_usd=firm_target_notional_usd,
                firm_guard_pnl=intent_decimal("firm_guard_pnl"),
                firm_required_pnl=intent_decimal("firm_required_pnl"),
                execution_reserve_usd=intent_decimal(
                    "execution_reserve_usd",
                    nonnegative=True,
                ),
                lighter_vwap=intent_decimal("lighter_vwap", positive=True),
                lighter_quote_age_ms=lighter_quote_age_ms,
                lighter_client_order_index=lighter_client_order_index,
                lighter_client_order_collision=raw_collision,
                sent_at_iso=sent_at_iso,
                prepared_at_iso=prepared_at_iso,
                adaptive_strategy_context=adaptive_context,
                strategy_tag=strategy_tag,
                commit_rfq_id=commit_rfq_id,
                confirmed_trade_key=confirmed_trade_key,
            )
            recovery_reason = (
                "Recovered an unresolved Var order; account reconciliation required"
            )
            if not self.automation_paused:
                self.pause_automation(recovery_reason)
                self._reconcile_pause_reason = recovery_reason

        raw_last_round_closed_at = payload.get("last_round_closed_at", 0)
        if isinstance(raw_last_round_closed_at, bool):
            raise RuntimeError("Runtime state last_round_closed_at is invalid")
        try:
            parsed_last_round_closed_at = float(raw_last_round_closed_at)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Runtime state last_round_closed_at is invalid"
            ) from exc
        if not math.isfinite(parsed_last_round_closed_at) or parsed_last_round_closed_at < 0:
            raise RuntimeError("Runtime state last_round_closed_at is invalid")
        self._last_round_closed_at = parsed_last_round_closed_at
        raw_canary = payload.get("canary_session")
        if raw_canary is not None and not isinstance(raw_canary, dict):
            raise RuntimeError("Runtime state canary_session is malformed")
        if isinstance(raw_canary, dict):
            raw_round_count = raw_canary.get("round_count")
            raw_consecutive_losses = raw_canary.get("consecutive_losses")
            if (
                isinstance(raw_round_count, bool)
                or not isinstance(raw_round_count, int)
                or raw_round_count < 0
                or raw_round_count > 10_000
                or isinstance(raw_consecutive_losses, bool)
                or not isinstance(raw_consecutive_losses, int)
                or raw_consecutive_losses < 0
                or raw_consecutive_losses > 10_000
            ):
                raise RuntimeError("Runtime state canary counters are invalid")
            self._canary_round_count = raw_round_count
            cumulative_loss = to_decimal(raw_canary.get("cumulative_loss_usd"))
            if cumulative_loss is None or cumulative_loss < 0:
                raise RuntimeError("Runtime state canary loss counter is invalid")
            self._canary_cumulative_loss_usd = cumulative_loss
            self._canary_consecutive_losses = raw_consecutive_losses
            saved_state = raw_canary.get("state")
            if saved_state not in {
                CANARY_SESSION_OBSERVING,
                CANARY_SESSION_ARMED,
                CANARY_SESSION_REVIEW_REQUIRED,
                CANARY_SESSION_HALTED,
            }:
                raise RuntimeError("Runtime state canary state is invalid")
            # Keep genuine safety pauses derived above, but do not restore the
            # former one-shot/loss-limit REVIEW_REQUIRED or HALTED gates. PnL
            # counters remain historical metrics and never disable live opens.
            if self.automation_paused or self.pending_var_intent is not None:
                self._canary_session_state = CANARY_SESSION_HALTED
            elif saved_state == CANARY_SESSION_ARMED:
                self._canary_session_state = CANARY_SESSION_ARMED
            else:
                self._canary_session_state = CANARY_SESSION_OBSERVING
        elif self.strategy_config.execution_mode == "live":
            self._canary_session_state = CANARY_SESSION_OBSERVING
        self._runtime_state_sig = None
        self.logger.info(
            "Restored %s unfinished records for %s; dropped %s completed-history records",
            len(recovery_records),
            asset,
            len(loaded_records) - len(recovery_records),
        )
        return True

    @staticmethod
    def _read_json_file(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("runtime state must be a JSON object")
        return payload

    async def get_variational_position_snapshot(
        self,
        asset: str,
    ) -> tuple[Decimal, Decimal | None, str | None, float | None]:
        async with self.runtime.monitor._lock:
            row = self.runtime.monitor.positions.get(asset)
            qty = Decimal("0")
            avg_entry_price = None
            updated_at = None
            if isinstance(row, dict):
                qty = to_decimal(row.get("qty")) or Decimal("0")
                avg_entry_price = to_decimal(row.get("avg_entry_price"))
                updated_at = str(row.get("updated_at") or "") or None
            received = self.runtime.monitor._portfolio_received_monotonic
            age = (
                max(0.0, asyncio.get_running_loop().time() - received)
                if received is not None
                else None
            )
        return qty, avg_entry_price, updated_at, age

    async def get_variational_position(self, asset: str) -> Decimal:
        qty, _, _, _ = await self.get_variational_position_snapshot(asset)
        return qty

    async def get_variational_portfolio_metadata(
        self,
        asset: str,
    ) -> VariationalPortfolioMetadata:
        async with self.runtime.monitor._lock:
            row = self.runtime.monitor.positions.get(asset)
            summary = self.runtime.monitor.portfolio_summary
            received = self.runtime.monitor._portfolio_received_monotonic
            age = (
                max(0.0, asyncio.get_running_loop().time() - received)
                if received is not None
                else None
            )
            return VariationalPortfolioMetadata(
                has_snapshot=bool(summary),
                request_id=self.runtime.monitor.portfolio_request_id,
                published_at=str(summary.get("published_at") or "") or None,
                captured_at=self.runtime.monitor.portfolio_captured_at,
                position_updated_at=(
                    str(row.get("updated_at") or "") or None
                    if isinstance(row, dict)
                    else None
                ),
                fingerprint=self.runtime.monitor.portfolio_fingerprint,
                content_revision=self.runtime.monitor.portfolio_content_revision,
                age_seconds=age,
            )

    async def latest_confirmed_variational_fill_time(self) -> datetime | None:
        async with self._record_lock:
            timestamps = [
                timestamp
                for record in self.records.values()
                if record.last_variational_status == "filled"
                and (timestamp := parse_iso_datetime(record.var_fill_ts_iso)) is not None
            ]
        return max(timestamps, default=None)

    def variational_portfolio_degraded_outcome(
        self,
        metadata: VariationalPortfolioMetadata,
        latest_fill_time: datetime | None,
    ) -> AccountReconcileOutcome | None:
        if not metadata.has_snapshot:
            return AccountReconcileOutcome.UNKNOWN
        if metadata.age_seconds is None:
            return AccountReconcileOutcome.UNKNOWN
        if latest_fill_time is None:
            return None

        # Portfolio events are change-driven: a flat account can legitimately
        # publish no new content for hours. Transport and market freshness are
        # checked independently before submission, so wall-clock age alone
        # must not invalidate an authoritative unchanged account snapshot.
        # A confirmed fill still requires portfolio content at least as new as
        # that fill.
        reference_time = parse_iso_datetime(
            metadata.position_updated_at or metadata.published_at or ""
        )
        if reference_time is None or reference_time < latest_fill_time:
            return AccountReconcileOutcome.STALE
        return None

    @staticmethod
    def reconciliation_snapshot_token(
        metadata: VariationalPortfolioMetadata,
        *,
        var_position: Decimal,
        lighter_position: Decimal,
        active_orders: int,
    ) -> str:
        portfolio_token = "|".join(
            (
                metadata.request_id or "-",
                metadata.published_at or "-",
                metadata.fingerprint or "-",
                str(metadata.content_revision),
            )
        )
        if not metadata.has_snapshot:
            portfolio_token = f"compat-{time.monotonic_ns()}"
        return (
            f"{portfolio_token}|var={var_position}|lighter={lighter_position}"
            f"|active={active_orders}"
        )

    async def get_lighter_account_snapshot(self) -> tuple[Decimal, int]:
        if self.lighter_client is None:
            self.initialize_lighter_client()
        account_api = AccountApi(self.lighter_client.api_client)
        account_result = await account_api.account(
            by="index",
            value=str(self.account_index),
            _request_timeout=5,
        )
        lighter_position = Decimal("0")
        accounts = getattr(account_result, "accounts", None) or []
        if accounts:
            for position in getattr(accounts[0], "positions", None) or []:
                if int(getattr(position, "market_id", -1)) != self.lighter_market_index:
                    continue
                size = to_decimal(getattr(position, "position", None)) or Decimal("0")
                sign = Decimal(str(getattr(position, "sign", 0) or 0))
                lighter_position = size * sign
                break

        async with self._lighter_signer_lock:
            auth_token, auth_error = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index
            )
        if auth_error is not None:
            raise RuntimeError(f"Lighter auth token error: {auth_error}")
        orders_result = await self.lighter_client.order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self.lighter_market_index,
            auth=auth_token,
            _request_timeout=5,
        )
        active_orders = len(getattr(orders_result, "orders", None) or [])
        return lighter_position, active_orders

    async def reconcile_lighter_client_order(
        self,
        client_order_id: int,
    ) -> LighterOrderReconcileOutcome:
        """Resolve one deterministic order id without confusing absence and outage."""
        if self.lighter_client is None:
            return LighterOrderReconcileOutcome.UNKNOWN
        try:
            orders, inactive_history_complete = (
                await self._fetch_lighter_orders_for_reconciliation(
                    {client_order_id},
                )
            )
        except Exception as exc:
            self.logger.warning(
                "Lighter order reconciliation unavailable for client_order_id=%s: %s",
                client_order_id,
                exc,
            )
            return LighterOrderReconcileOutcome.UNKNOWN

        for raw in orders:
            try:
                matched_id = int(raw.get("client_order_id"))
            except (TypeError, ValueError):
                continue
            if matched_id == client_order_id:
                await self.handle_lighter_fill_update(raw)
                return LighterOrderReconcileOutcome.FOUND
        if callable(getattr(self.lighter_client.order_api, "trades", None)):
            try:
                trade_fills = (
                    await self._fetch_lighter_trade_fills_for_reconciliation(
                        {client_order_id}
                    )
                )
            except Exception as exc:
                self.logger.warning(
                    "Lighter trade reconciliation unavailable for "
                    "client_order_id=%s: %s",
                    client_order_id,
                    exc,
                )
                return LighterOrderReconcileOutcome.UNKNOWN
            for raw in trade_fills:
                if to_int(raw.get("client_order_id")) != client_order_id:
                    continue
                await self.handle_lighter_fill_update(raw)
                return LighterOrderReconcileOutcome.FOUND
        return (
            LighterOrderReconcileOutcome.CONFIRMED_ABSENT
            if inactive_history_complete
            else LighterOrderReconcileOutcome.UNKNOWN
        )

    @staticmethod
    def _lighter_order_payload(order: Any) -> dict[str, Any] | None:
        raw = order.to_dict() if hasattr(order, "to_dict") else order
        return raw if isinstance(raw, dict) else None

    async def _fetch_lighter_trade_fills_for_reconciliation(
        self,
        target_ids: set[int],
    ) -> list[dict[str, Any]]:
        """Recover cumulative IOC fills from the authoritative trades API.

        Lighter can acknowledge and execute an IOC while omitting it from both
        active and inactive order queries.  Account trades still retain the
        deterministic client id, so synthesize the same cumulative payload the
        private order stream would have delivered.
        """

        if not target_ids or self.lighter_client is None:
            return []
        trades_endpoint = getattr(self.lighter_client.order_api, "trades", None)
        if not callable(trades_endpoint):
            return []

        async with self._lighter_signer_lock:
            auth_token, auth_error = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index
            )
        if auth_error is not None:
            raise RuntimeError(f"Lighter auth token error: {auth_error}")

        totals: dict[int, tuple[Decimal, Decimal, int | None]] = {}
        seen_trade_ids: set[int] = set()
        seen_cursors: set[str] = set()
        cursor: str | None = None
        matched_a_page = False
        for _page_index in range(LIGHTER_TRADE_RECONCILE_MAX_PAGES):
            request_kwargs: dict[str, Any] = {
                "sort_by": "timestamp",
                "sort_dir": "desc",
                "limit": LIGHTER_TRADE_RECONCILE_PAGE_LIMIT,
                "market_id": self.lighter_market_index,
                "account_index": self.account_index,
                "auth": auth_token,
                "_request_timeout": 5,
            }
            if cursor is not None:
                request_kwargs["cursor"] = cursor
            result = await trades_endpoint(**request_kwargs)
            page_matched = False
            for trade in getattr(result, "trades", None) or []:
                raw = self._lighter_order_payload(trade)
                if raw is None:
                    continue
                trade_id = to_int(raw.get("trade_id"))
                if trade_id is not None:
                    if trade_id in seen_trade_ids:
                        continue
                    seen_trade_ids.add(trade_id)
                candidate_ids = {
                    candidate
                    for candidate in (
                        to_int(raw.get("ask_client_id")),
                        to_int(raw.get("bid_client_id")),
                    )
                    if candidate is not None and candidate in target_ids
                }
                if not candidate_ids:
                    continue
                size = to_decimal(raw.get("size"))
                quote = to_decimal(raw.get("usd_amount"))
                if size is None or quote is None or size <= 0 or quote <= 0:
                    continue
                page_matched = True
                transaction_time = to_int(
                    raw.get("transaction_time") or raw.get("timestamp")
                )
                for client_order_id in candidate_ids:
                    previous_size, previous_quote, previous_time = totals.get(
                        client_order_id,
                        (Decimal("0"), Decimal("0"), None),
                    )
                    totals[client_order_id] = (
                        previous_size + size,
                        previous_quote + quote,
                        max(
                            value
                            for value in (previous_time, transaction_time)
                            if value is not None
                        )
                        if previous_time is not None or transaction_time is not None
                        else None,
                    )

            # Fills for one IOC are contiguous in the descending trade feed.
            # Read one clean page beyond the match so a page boundary cannot
            # truncate a large or fragmented fill.
            if matched_a_page and not page_matched:
                break
            matched_a_page = matched_a_page or page_matched
            raw_next_cursor = getattr(result, "next_cursor", None)
            next_cursor = (
                str(raw_next_cursor).strip()
                if raw_next_cursor is not None
                else ""
            )
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return [
            {
                "client_order_id": client_order_id,
                # The order is absent from the active endpoint, so any
                # recovered execution is terminal.  Exact target fills are
                # normalized to `filled` by _apply_lighter_fill_update before
                # this terminal status is otherwise interpreted.
                "status": "canceled_by_market",
                "filled_base_amount": str(size),
                "filled_quote_amount": str(quote),
                "transaction_time": transaction_time,
                "recovered_from_trades": True,
            }
            for client_order_id, (size, quote, transaction_time) in totals.items()
        ]

    async def _fetch_lighter_orders_for_reconciliation(
        self,
        target_ids: set[int],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch active orders and enough inactive pages to prove absence safely.

        The inactive endpoint is paginated.  Reaching a page cap, a cursor
        cycle, or an API error must never be treated as proof that a
        deterministic client order id was not submitted.
        """

        if self.lighter_client is None:
            raise RuntimeError("Lighter client is not initialized")
        async with self._lighter_signer_lock:
            auth_token, auth_error = self.lighter_client.create_auth_token_with_expiry(
                api_key_index=self.api_key_index
            )
        if auth_error is not None:
            raise RuntimeError(f"Lighter auth token error: {auth_error}")

        active_result = await self.lighter_client.order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self.lighter_market_index,
            auth=auth_token,
            _request_timeout=5,
        )
        orders = [
            payload
            for order in (getattr(active_result, "orders", None) or [])
            if (payload := self._lighter_order_payload(order)) is not None
        ]
        found_ids = {
            client_order_id
            for raw in orders
            if (client_order_id := to_int(raw.get("client_order_id"))) is not None
        }
        if target_ids and target_ids.issubset(found_ids):
            return orders, False

        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page_index in range(LIGHTER_INACTIVE_ORDER_MAX_PAGES):
            request_kwargs: dict[str, Any] = {
                "account_index": self.account_index,
                "limit": LIGHTER_INACTIVE_ORDER_PAGE_LIMIT,
                "market_id": self.lighter_market_index,
                "auth": auth_token,
                "_request_timeout": 5,
            }
            if cursor is not None:
                request_kwargs["cursor"] = cursor
            inactive_result = await self.lighter_client.order_api.account_inactive_orders(
                **request_kwargs
            )
            page_orders = [
                payload
                for order in (getattr(inactive_result, "orders", None) or [])
                if (payload := self._lighter_order_payload(order)) is not None
            ]
            orders.extend(page_orders)
            found_ids.update(
                client_order_id
                for raw in page_orders
                if (client_order_id := to_int(raw.get("client_order_id"))) is not None
            )
            if target_ids and target_ids.issubset(found_ids):
                return orders, False

            raw_next_cursor = getattr(inactive_result, "next_cursor", None)
            next_cursor = (
                str(raw_next_cursor).strip()
                if raw_next_cursor is not None
                else ""
            )
            if not next_cursor:
                return orders, True
            if next_cursor in seen_cursors:
                self.logger.warning(
                    "Lighter inactive-order cursor repeated; absence remains unconfirmed"
                )
                return orders, False
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        self.logger.warning(
            "Lighter inactive-order history exceeded %s pages; absence remains unconfirmed",
            LIGHTER_INACTIVE_ORDER_MAX_PAGES,
        )
        return orders, False

    def _terminal_lighter_hedge_is_complete_locked(
        self,
        record: OrderLifecycle,
    ) -> bool:
        return bool(
            record.hedge_status != "overfilled"
            and record.lighter_outcome_final
            and record.lighter_client_order_ids
            and lighter_order_target_matches(
                record,
                record.lighter_filled_qty,
                self.base_amount_multiplier,
            )
        )

    def _normalize_terminal_lighter_hedge_locked(
        self,
        record: OrderLifecycle,
    ) -> bool:
        """Restore an authoritative terminal fill after a stale event overwrite."""

        if not self._terminal_lighter_hedge_is_complete_locked(record):
            return False
        changed = (
            record.hedge_status != "filled"
            or record.execution_state != EXECUTION_STATE_HEDGED
            or record.hedge_error is not None
        )
        record.hedge_status = "filled"
        record.execution_state = EXECUTION_STATE_HEDGED
        record.hedge_error = None
        return changed

    async def refresh_pending_lighter_orders(self) -> None:
        pending_ids: set[int] = set()
        normalized_records: list[OrderLifecycle] = []
        async with self._record_lock:
            for record in self.records.values():
                if self._normalize_terminal_lighter_hedge_locked(record):
                    normalized_records.append(record)
                if record.hedge_status in {
                    "queued",
                    "submitting",
                    "submitted",
                    "retrying",
                    "uncertain",
                    "partial",
                }:
                    pending_ids.update(record.lighter_client_order_ids)
                elif (
                    record.hedge_status == "recovery_check"
                    and (
                        record.lighter_client_order_id is not None
                        or record.lighter_reserved_client_order_id is not None
                    )
                ):
                    pending_ids.add(
                        record.lighter_client_order_id
                        or record.lighter_reserved_client_order_id
                    )
        if normalized_records:
            await self.persist_runtime_state()
            for record in normalized_records:
                await self.record_completed_canary_round_for_leg(record)
        if not pending_ids or self.lighter_client is None:
            return
        orders, inactive_history_complete = (
            await self._fetch_lighter_orders_for_reconciliation(pending_ids)
        )
        found_ids: set[int] = set()
        for raw in orders:
            with contextlib.suppress(Exception):
                client_order_id = int(raw.get("client_order_id"))
                if client_order_id in pending_ids:
                    found_ids.add(client_order_id)
                    await self.handle_lighter_fill_update(raw)

        now = datetime.now(timezone.utc)
        trade_probe_ids: set[int] = set()
        async with self._record_lock:
            for record in self.records.values():
                if record.hedge_status == "recovery_check":
                    probe_id = (
                        record.lighter_client_order_id
                        or record.lighter_reserved_client_order_id
                    )
                    if probe_id is not None and probe_id not in found_ids:
                        trade_probe_ids.add(probe_id)
                    continue
                if record.hedge_status not in {"submitted", "uncertain", "partial"}:
                    continue
                submitted_at = parse_iso_datetime(record.lighter_submitted_at_iso)
                if (
                    submitted_at is not None
                    and (now - submitted_at).total_seconds()
                    > LIGHTER_FILL_TIMEOUT_SECONDS
                ):
                    trade_probe_ids.update(
                        set(record.lighter_client_order_ids) - found_ids
                    )

        if trade_probe_ids:
            try:
                recovered_trades = (
                    await self._fetch_lighter_trade_fills_for_reconciliation(
                        trade_probe_ids
                    )
                )
            except Exception as exc:
                recovered_trades = []
                self.logger.warning(
                    "Lighter trade-history reconciliation failed: %s",
                    exc,
                )
            for raw in recovered_trades:
                client_order_id = to_int(raw.get("client_order_id"))
                if client_order_id is None:
                    continue
                found_ids.add(client_order_id)
                await self.handle_lighter_fill_update(raw)

        recovery_records_to_schedule: list[OrderLifecycle] = []
        async with self._record_lock:
            for record in self.records.values():
                if record.hedge_status != "recovery_check":
                    continue
                probe_id = (
                    record.lighter_client_order_id
                    or record.lighter_reserved_client_order_id
                )
                if (
                    probe_id is None
                    or probe_id in found_ids
                    or not inactive_history_complete
                ):
                    continue
                record.hedge_status = "not_started"
                record.execution_state = EXECUTION_STATE_VAR_COMMITTED
                record.hedge_error = None
                self.lighter_client_order_to_trade_key.pop(probe_id, None)
                recovery_records_to_schedule.append(record)
        if recovery_records_to_schedule:
            await self.persist_runtime_state()
            for record in recovery_records_to_schedule:
                self.trace_event(
                    "lighter_recovery_order_not_found",
                    record.trace_id,
                    trade_key=record.trade_key,
                    client_order_id=(
                        record.lighter_client_order_id
                        or record.lighter_reserved_client_order_id
                    ),
                )
                self.schedule_lighter_order(record)

        timed_out_records: list[OrderLifecycle] = []
        for record in list(self.records.values()):
            if record.hedge_status not in {"submitted", "uncertain", "partial"}:
                continue
            submitted_at = parse_iso_datetime(record.lighter_submitted_at_iso)
            if (
                submitted_at is None
                or (now - submitted_at).total_seconds()
                <= LIGHTER_FILL_TIMEOUT_SECONDS
            ):
                continue
            unresolved = set(record.lighter_client_order_ids) - found_ids
            if unresolved:
                timed_out_records.append(record)

        flat_accounts_confirmed = False
        if any(record.lighter_reduce_only for record in timed_out_records):
            accounts_match = await self.reconcile_accounts(allow_resume=True)
            snapshot = self.last_account_snapshot
            flat_accounts_confirmed = bool(
                accounts_match
                and snapshot is not None
                and abs(snapshot.var_position) <= VAR_POSITION_TOLERANCE
                and abs(snapshot.lighter_position) <= VAR_POSITION_TOLERANCE
                and snapshot.lighter_active_orders == 0
            )

        for record in timed_out_records:
            current = self.records.get(record.trade_key)
            if current is None or current.hedge_status not in {
                "submitted",
                "uncertain",
                "partial",
            }:
                continue
            unresolved = set(current.lighter_client_order_ids) - found_ids
            if not unresolved:
                continue
            if flat_accounts_confirmed and current.lighter_reduce_only:
                async with self._record_lock:
                    current.hedge_status = "reconciled_flat"
                    current.execution_state = EXECUTION_STATE_HEDGED
                    current.hedge_error = None
                    current.lighter_outcome_final = True
                    self.lighter_order_terminal_ids.update(
                        current.lighter_client_order_ids
                    )
                    payload = current.to_payload()
                self.trace_event(
                    "lighter_close_reconciled_flat",
                    current.trace_id,
                    trade_key=current.trade_key,
                    unresolved_client_order_ids=sorted(unresolved),
                )
                await self.append_order_log(
                    "lighter_close_reconciled_flat",
                    payload,
                )
                await self.persist_runtime_state()
                continue

            async with self._record_lock:
                if current.hedge_status not in {
                    "submitted",
                    "uncertain",
                    "partial",
                }:
                    continue
                current.hedge_status = "uncertain"
                current.hedge_error = (
                    "Lighter order status unresolved after "
                    f"{LIGHTER_FILL_TIMEOUT_SECONDS:.0f}s"
                )
                reason = current.hedge_error
            self.pause_for_reconciliation(reason)
            await self.persist_runtime_state()

    async def prepare_restored_lighter_recovery(self) -> list[OrderLifecycle]:
        """Normalize crash-window hedge states before startup reconciliation.

        An opening hedge may safely resume after its deterministic order id is
        checked.  A close record with only a pre-Commit reserved id is
        different: no Lighter close was necessarily authorized, because the
        opening hedge may have failed or only partially filled.  That case is
        failed closed instead of guessing a full close quantity.
        """

        safe_without_probe: list[OrderLifecycle] = []
        unsafe_close_keys: list[str] = []
        changed = False
        async with self._record_lock:
            pending_provisional_key = (
                self.pending_var_intent.provisional_trade_key
                if self.pending_var_intent is not None
                else None
            )
            for record in self.records.values():
                if record.hedge_status not in {
                    "not_started",
                    "queued",
                    "submitting",
                }:
                    continue
                if (
                    record.trade_key == pending_provisional_key
                    and record.var_fill_source not in {"event", "portfolio"}
                ):
                    # Startup must first prove that the Var Commit filled.  An
                    # unconfirmed provisional record can only reconcile or
                    # roll back an existing Lighter order, never create one.
                    continue
                actual_order_id = record.lighter_client_order_id
                reserved_order_id = record.lighter_reserved_client_order_id
                if record.lighter_reduce_only and actual_order_id is None:
                    record.hedge_status = "recovery_required"
                    record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                    record.hedge_error = (
                        "Restored Var close has no durable Lighter submission; "
                        "reconcile the original hedge before any protective close"
                    )
                    unsafe_close_keys.append(record.trade_key)
                    changed = True
                    continue
                probe_id = actual_order_id or reserved_order_id
                if probe_id is not None:
                    record.hedge_status = "recovery_check"
                    record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                    record.hedge_error = (
                        "Checking deterministic Lighter order before crash recovery"
                    )
                    self.lighter_client_order_to_trade_key[probe_id] = record.trade_key
                    changed = True
                else:
                    # Manual/open fills persist a generated order id before
                    # transport send.  With no id on disk, no order crossed the
                    # send boundary and a fresh schedule is safe.
                    safe_without_probe.append(record)

        if unsafe_close_keys:
            self.pause_for_reconciliation(
                "Restored Var close lacks a durable Lighter submission; "
                "automatic protective close is blocked"
            )
            self.logger.error(
                "Crash recovery blocked ambiguous Lighter close records: %s",
                ", ".join(unsafe_close_keys),
            )
        if changed:
            await self.persist_runtime_state()
        return safe_without_probe

    async def lighter_order_watchdog_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(0.1)
            poll_interval = (
                LIGHTER_ORDER_REST_POLL_SECONDS
                if self.lighter_private_stream_ready
                else LIGHTER_ORDER_REST_FALLBACK_POLL_SECONDS
            )
            now = time.monotonic()
            if now - self._last_lighter_order_refresh_at < poll_interval:
                continue
            self._last_lighter_order_refresh_at = now
            try:
                await self.refresh_pending_lighter_orders()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Lighter order reconciliation failed: %s", exc)

    async def emergency_flatten_var(
        self,
        record: OrderLifecycle,
        *,
        intent_phase: str = "emergency_close",
    ) -> bool:
        if intent_phase not in {"emergency_close", "operator_var_only_close"}:
            raise ValueError("unsupported emergency Var close phase")
        if record.lighter_reduce_only or self.pending_var_intent is not None:
            return False
        current_open = await self._current_open_record()
        if current_open is None or current_open.trade_key != record.trade_key:
            self.pause_automation(
                "Emergency Var flatten skipped because the source position is no "
                "longer the locally tracked open; reconcile both accounts"
            )
            return False
        side = opposite_var_order_side(record.side)
        close_qty = normalize_var_base_qty(record.qty)
        close_notional = var_open_notional_usd(record)
        if side is None or close_qty is None or close_notional is None:
            self.pause_automation("Emergency Var flatten could not determine side or quantity")
            return False
        if not await self.runtime.command_broker.extension_connected():
            self.pause_automation("Emergency Var flatten unavailable: command channel disconnected")
            return False

        self._auto_var_order_inflight = True
        expected_intent = await self.mark_and_persist_var_intent(
            intent_phase,
            side,
            close_notional,
        )
        self.last_auto_var_close_status = f"emergency {side} {self._fmt_notional(close_notional)}"
        commit_dispatched = False
        succeeded = False
        try:
            common = {
                "side": side,
                "amount": decimal_to_str(close_notional) or "0",
                "base_qty": decimal_to_str(close_qty),
                "market": self.variational_ticker,
                "timeout_ms": self.strategy_config.var_order_result_timeout_ms,
                "phase": "close",
            }
            quote_result = await self.runtime.command_broker.request_place_order(
                **common,
                fetch_stage="quote",
                guard={"required": False},
            )
            quote_result_detail = (
                quote_result.get("detail")
                if isinstance(quote_result.get("detail"), dict)
                else {}
            )
            quote = (
                quote_result_detail.get("quote")
                if isinstance(quote_result_detail.get("quote"), dict)
                else {}
            )
            quote_id = str(quote.get("quoteId") or "").strip()
            firm_price = to_decimal(quote.get("firmPrice"))
            firm_qty = to_decimal(quote.get("firmQty"))
            quote_error: str | None = None
            if not quote_result.get("ok"):
                quote_error = str(
                    quote_result.get("error") or "emergency Firm Quote failed"
                )
            elif (
                not quote_id
                or firm_price is None
                or firm_price <= 0
                or firm_qty is None
                or firm_qty <= 0
            ):
                quote_error = (
                    "emergency Firm Quote is missing quoteId, firmPrice, or firmQty"
                )
            elif firm_qty != close_qty:
                quote_error = (
                    "emergency Firm Quote base quantity mismatch: "
                    f"expected {close_qty}, got {firm_qty}"
                )

            if quote_error is not None:
                result = {
                    **quote_result,
                    "ok": False,
                    "error": quote_error,
                    "detail": quote_result_detail,
                }
            else:
                guarded_quote = {
                    **quote,
                    "quoteId": quote_id,
                    "firmPrice": decimal_to_str(firm_price),
                    "firmQty": decimal_to_str(firm_qty),
                    "adaptiveStrategy": {
                        "schema": "adaptive-emergency-close-context-v1",
                        "strategyTag": record.strategy_tag,
                        "openTradeKey": record.trade_key,
                        "openQty": decimal_to_str(record.qty),
                        "requestedCloseNotionalUsd": decimal_to_str(
                            close_notional
                        ),
                    },
                    "strategyTag": record.strategy_tag,
                }
                trace_id = new_trace_id()
                prepared_intent = await self.prepare_pending_var_intent(
                    phase=intent_phase,
                    side=side,
                    amount=close_notional,
                    trace_id=trace_id,
                    firm_quote=guarded_quote,
                    expected_intent=expected_intent,
                )
                if prepared_intent is None or not await self.mark_pending_var_intent_committing(
                    phase=intent_phase,
                    side=side,
                    trace_id=trace_id,
                    expected_intent=prepared_intent,
                ):
                    result = {
                        "ok": False,
                        "requestId": quote_result.get("requestId"),
                        "error": "emergency Var intent changed before Commit",
                        "detail": {**quote_result_detail, "quote": guarded_quote},
                    }
                else:
                    async with self._record_lock:
                        ordered_records = [
                            self.records[key]
                            for key in self.record_order
                            if key in self.records
                        ]
                        current_open, _rounds = build_trade_rounds(ordered_records)
                        commit_allowed = (
                            self.pending_var_intent is prepared_intent
                            and prepared_intent.state == VAR_INTENT_COMMITTING
                            and not self._asset_switch_in_progress
                            and current_open is not None
                            and current_open.trade_key == record.trade_key
                            and current_open.qty == close_qty
                        )
                    if not commit_allowed:
                        result = {
                            "ok": False,
                            "requestId": quote_result.get("requestId"),
                            "error": "position or market changed before emergency Var Commit",
                            "detail": {**quote_result_detail, "quote": guarded_quote},
                        }
                    else:
                        commit_dispatched = True
                        try:
                            commit_response = await self.runtime.command_broker.request_place_order(
                                **common,
                                fetch_stage="commit",
                                firm_quote=guarded_quote,
                                guard={"required": False},
                            )
                        except asyncio.CancelledError:
                            await self.mark_pending_var_intent_commit_ambiguous(
                                phase=intent_phase,
                                side=side,
                                trace_id=trace_id,
                                expected_intent=prepared_intent,
                            )
                            raise
                        except Exception:
                            await self.mark_pending_var_intent_commit_ambiguous(
                                phase=intent_phase,
                                side=side,
                                trace_id=trace_id,
                                expected_intent=prepared_intent,
                            )
                            raise
                        result = {
                            **commit_response,
                            "detail": {
                                **(
                                    commit_response.get("detail")
                                    if isinstance(commit_response.get("detail"), dict)
                                    else {}
                                ),
                                "quote": guarded_quote,
                            },
                        }
                        if result.get("ok"):
                            async with self._record_lock:
                                if self.pending_var_intent is prepared_intent:
                                    prepared_intent.commit_accepted_monotonic = time.monotonic()
                        elif var_result_is_ambiguous(result):
                            await self.mark_pending_var_intent_commit_ambiguous(
                                phase=intent_phase,
                                side=side,
                                trace_id=trace_id,
                                expected_intent=prepared_intent,
                            )

            if not commit_dispatched:
                self.clear_matching_var_intent(
                    side,
                    force=True,
                    expected_intent=expected_intent,
                )
            await self.append_auto_var_result_log(
                phase=intent_phase,
                side=side,
                amount=close_notional,
                base_qty=close_qty,
                expected_pnl=None,
                result=result,
            )
            if not result.get("ok"):
                if not var_result_is_ambiguous(result):
                    self.clear_matching_var_intent(
                        side,
                        force=True,
                        expected_intent=expected_intent,
                    )
                self.pause_automation(f"Emergency Var flatten failed: {result.get('error') or 'unknown'}")
            else:
                succeeded = True
        except Exception as exc:
            if not commit_dispatched:
                self.clear_matching_var_intent(
                    side,
                    force=True,
                    expected_intent=expected_intent,
                )
            self.pause_automation(f"Emergency Var flatten uncertain: {exc}")
        finally:
            self._auto_var_order_inflight = False
            await self.persist_runtime_state()
        return succeeded

    def transition_in_progress(self) -> bool:
        if self.pending_var_intent is not None or self._auto_var_order_inflight:
            return True
        if any(not task.done() for task in self.hedge_tasks):
            return True
        for record in self.records.values():
            if record.hedge_status not in {"queued", "submitting", "submitted", "retrying", "uncertain", "partial"}:
                continue
            submitted_at = parse_iso_datetime(record.lighter_submitted_at_iso)
            if submitted_at is None:
                return True
            if (datetime.now(timezone.utc) - submitted_at).total_seconds() <= LIGHTER_FILL_TIMEOUT_SECONDS:
                return True
        return False

    async def mark_reconcile_degraded(
        self,
        outcome: AccountReconcileOutcome,
        detail: str,
    ) -> bool:
        if outcome not in {
            AccountReconcileOutcome.STALE,
            AccountReconcileOutcome.UNKNOWN,
        }:
            raise ValueError(f"invalid degraded reconcile outcome: {outcome.value}")
        status = f"{outcome.value}: {detail}"
        changed = (
            self.automation_ready
            or self.last_reconcile_outcome is not outcome
            or self.last_reconcile_status != status
            or self.reconcile_degraded_reason != detail
            or self._reconcile_mismatch_first_token is not None
        )
        self.automation_ready = False
        self.last_reconcile_outcome = outcome
        self.last_reconcile_status = status
        self.reconcile_degraded_reason = detail
        self._reconcile_failure_count = 0
        self._reconcile_mismatch_first_token = None
        self._reconcile_mismatch_first_monotonic = None
        if changed:
            await self.persist_runtime_state()
        return False

    async def reconcile_accounts(self, *, allow_resume: bool = False) -> bool:
        if not self.variational_ticker:
            return False
        if self.transition_in_progress() and not allow_resume:
            self.last_reconcile_status = "waiting for active transition"
            return False

        try:
            var_position = await self.get_variational_position(self.variational_ticker)
            portfolio_metadata = await self.get_variational_portfolio_metadata(
                self.variational_ticker
            )
            latest_fill_time = await self.latest_confirmed_variational_fill_time()
        except Exception as exc:
            return await self.mark_reconcile_degraded(
                AccountReconcileOutcome.UNKNOWN,
                f"Variational portfolio query failed: {exc}",
            )

        degraded_outcome = self.variational_portfolio_degraded_outcome(
            portfolio_metadata,
            latest_fill_time,
        )
        if degraded_outcome is not None:
            if degraded_outcome is AccountReconcileOutcome.STALE:
                detail = (
                    "Variational portfolio snapshot predates the latest confirmed fill "
                    f"(stream={portfolio_metadata.request_id or '-'})"
                )
            else:
                detail = "Variational portfolio snapshot is unavailable or malformed"
            return await self.mark_reconcile_degraded(degraded_outcome, detail)

        try:
            lighter_position, active_orders = await self.get_lighter_account_snapshot()
        except Exception as exc:
            return await self.mark_reconcile_degraded(
                AccountReconcileOutcome.UNKNOWN,
                f"Lighter account query failed: {exc}",
            )
        previous_account_snapshot = self.last_account_snapshot
        snapshot = AccountSnapshot(
            var_position=var_position,
            lighter_position=lighter_position,
            lighter_active_orders=active_orders,
            captured_at=utc_now(),
        )
        self.last_account_snapshot = snapshot

        current_open = await self._current_open_record()
        reconciled_stale_manual_flat = False
        accounts_are_flat = bool(
            abs(var_position) <= VAR_POSITION_TOLERANCE
            and abs(lighter_position) <= VAR_POSITION_TOLERANCE
            and active_orders == 0
        )
        if (
            current_open is not None
            and current_open.strategy_tag == MANUAL_STRATEGY_TAG
            and accounts_are_flat
            and await self.discard_stale_manual_flat_record(current_open)
        ):
            reconciled_stale_manual_flat = True
            self.logger.info(
                "Reconciled stale manual runtime position to confirmed account flat: %s",
                current_open.trade_key,
            )
            self.trace_event(
                "manual_runtime_position_reconciled_flat",
                current_open.trace_id,
                trade_key=current_open.trade_key,
                var_position=var_position,
                lighter_position=lighter_position,
                active_orders=active_orders,
            )
            current_open = None
        if current_open is None:
            expected_var = Decimal("0")
            expected_lighter = Decimal("0")
            lighter_target_valid = True
        else:
            expected_var = current_open.qty if current_open.side == "buy" else -current_open.qty
            lighter_target = lighter_order_target_qty(
                current_open,
                self.base_amount_multiplier,
            )
            lighter_target_valid = lighter_target is not None
            expected_lighter = (
                (-lighter_target if current_open.side == "buy" else lighter_target)
                if lighter_target is not None
                else Decimal("0")
            )

        positions_match = (
            lighter_target_valid
            and abs(var_position - expected_var) <= VAR_POSITION_TOLERANCE
            and lighter_position == expected_lighter
            and active_orders == 0
        )
        if not positions_match:
            status = (
                f"FRESH_MISMATCH: Var {var_position}/{expected_var}, "
                f"Lighter {lighter_position}/{expected_lighter}, active={active_orders}"
            )
            snapshot_token = self.reconciliation_snapshot_token(
                portfolio_metadata,
                var_position=var_position,
                lighter_position=lighter_position,
                active_orders=active_orders,
            )
            now_monotonic = time.monotonic()
            state_changed = (
                self.automation_ready
                or self.last_reconcile_outcome
                is not AccountReconcileOutcome.FRESH_MISMATCH
                or self.last_reconcile_status != status
            )
            self.automation_ready = False
            self.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MISMATCH
            self.last_reconcile_status = status
            self.reconcile_degraded_reason = status
            if self._reconcile_mismatch_first_token is None:
                self._reconcile_mismatch_first_token = snapshot_token
                self._reconcile_mismatch_first_monotonic = now_monotonic
                self._reconcile_failure_count = 1
                state_changed = True
            elif snapshot_token != self._reconcile_mismatch_first_token:
                self._reconcile_failure_count = 2
                mismatch_started = self._reconcile_mismatch_first_monotonic
                if (
                    mismatch_started is not None
                    and now_monotonic - mismatch_started
                    >= RECONCILE_MISMATCH_CONFIRM_SECONDS
                ):
                    reason = f"Account reconciliation failed: {status}"
                    state_changed = self.pause_for_reconciliation(reason) or state_changed
            if state_changed:
                await self.persist_runtime_state()
            return False

        resume_reconcile_pause = (
            self.automation_paused
            and self._reconcile_pause_reason is not None
            and self.automation_pause_reason == self._reconcile_pause_reason
        )
        previous_had_exposure = bool(
            previous_account_snapshot is not None
            and (
                abs(previous_account_snapshot.var_position)
                > VAR_POSITION_TOLERANCE
                or abs(previous_account_snapshot.lighter_position)
                > VAR_POSITION_TOLERANCE
                or previous_account_snapshot.lighter_active_orders != 0
            )
        )
        restored_failed_hedge_pause = bool(
            resume_reconcile_pause
            and self.automation_pause_reason.startswith("Lighter hedge failed:")
        )
        recovered_to_flat = bool(
            current_open is None
            and abs(var_position) <= VAR_POSITION_TOLERANCE
            and abs(lighter_position) <= VAR_POSITION_TOLERANCE
            and active_orders == 0
            and (
                previous_had_exposure
                or restored_failed_hedge_pause
                or reconciled_stale_manual_flat
            )
            and self.round_cooldown_remaining_seconds() == 0
        )
        state_changed = (
            not self.automation_ready
            or self.last_reconcile_outcome is not AccountReconcileOutcome.FRESH_MATCH
            or self.reconcile_degraded_reason is not None
            or self._reconcile_failure_count != 0
            or self._reconcile_mismatch_first_token is not None
            or resume_reconcile_pause
            or reconciled_stale_manual_flat
            or (allow_resume and self.pending_var_intent is not None)
            or recovered_to_flat
        )
        self._reconcile_failure_count = 0
        self._reconcile_mismatch_first_token = None
        self._reconcile_mismatch_first_monotonic = None
        self.automation_ready = True
        self.last_reconcile_outcome = AccountReconcileOutcome.FRESH_MATCH
        self.reconcile_degraded_reason = None
        self.last_reconcile_status = (
            f"FRESH_MATCH: Var {var_position}, Lighter {lighter_position}, active=0"
        )
        if allow_resume and self.pending_var_intent is not None:
            self.pending_var_intent = None
        if resume_reconcile_pause:
            resumed_pause_reason = self.automation_pause_reason
            self.automation_paused = False
            self.automation_pause_reason = "-"
            self._reconcile_pause_reason = None
            self.last_auto_var_order_status = "reconciled"
            self.last_auto_var_close_status = "reconciled"
            self.logger.info(
                "Automation resumed after exact account reconciliation: %s",
                resumed_pause_reason,
            )
            self.trace_event(
                "automation_resumed_after_reconciliation",
                None,
                pause_reason=resumed_pause_reason,
                var_position=var_position,
                lighter_position=lighter_position,
                active_orders=active_orders,
            )
        if recovered_to_flat:
            self._last_round_closed_at = time.time()
            self.last_auto_var_order_status = (
                f"reconciled; cooldown {self.strategy_config.round_cooldown_seconds}s"
            )
            self.last_auto_var_close_status = self.last_auto_var_order_status
            self.trace_event(
                "round_cooldown_started_after_flat_recovery",
                None,
                cooldown_seconds=self.strategy_config.round_cooldown_seconds,
                previous_var_position=(
                    previous_account_snapshot.var_position
                    if previous_account_snapshot is not None
                    else None
                ),
                previous_lighter_position=(
                    previous_account_snapshot.lighter_position
                    if previous_account_snapshot is not None
                    else None
                ),
                restored_failed_hedge_pause=restored_failed_hedge_pause,
            )
        if state_changed:
            await self.persist_runtime_state()
        return True

    async def reconcile_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(self.strategy_config.reconcile_interval_seconds)
            try:
                self.log_lighter_order_entry_transition()
                await self.reconcile_accounts()
                await self.prune_settled_execution_state()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.mark_reconcile_degraded(
                    AccountReconcileOutcome.UNKNOWN,
                    f"Account reconciliation error: {exc}",
                )

    def pause_for_reconciliation(self, reason: str) -> bool:
        if (
            self.automation_paused
            and self._reconcile_pause_reason == reason
            and self.automation_pause_reason == reason
        ):
            return False
        reconcile_owns_pause = (
            not self.automation_paused
            or (
                self._reconcile_pause_reason is not None
                and self.automation_pause_reason == self._reconcile_pause_reason
            )
        )
        if not reconcile_owns_pause:
            return False
        self._reconcile_pause_reason = reason
        self.pause_automation(reason)
        return True

    def pause_automation(self, reason: str) -> None:
        if self.automation_paused and self.automation_pause_reason == reason:
            if self.strategy_config.execution_mode == "live":
                self._canary_session_state = CANARY_SESSION_HALTED
            self.last_auto_var_order_status = f"paused: {reason}"
            self.last_auto_var_close_status = f"paused: {reason}"
            return
        self.automation_paused = True
        self.automation_pause_reason = reason
        if self.strategy_config.execution_mode == "live":
            self._canary_session_state = CANARY_SESSION_HALTED
        self.last_auto_var_order_status = f"paused: {reason}"
        self.last_auto_var_close_status = f"paused: {reason}"
        self.logger.warning("Automation paused: %s", reason)

    def _occupied_lighter_client_order_ids_locked(
        self,
        *,
        exclude_trade_key: str | None = None,
        exclude_intent: VarOrderIntent | None = None,
    ) -> set[int]:
        occupied: set[int] = set()
        for trade_key, record in self.records.items():
            if trade_key == exclude_trade_key:
                continue
            if record.lighter_reserved_client_order_id is not None:
                occupied.add(record.lighter_reserved_client_order_id)
            if record.lighter_client_order_id is not None:
                occupied.add(record.lighter_client_order_id)
            occupied.update(record.lighter_client_order_ids)
        intent = self.pending_var_intent
        if (
            intent is not None
            and intent is not exclude_intent
            and intent.lighter_client_order_index is not None
        ):
            occupied.add(intent.lighter_client_order_index)
        return occupied

    def _reserve_lighter_client_order_index_locked(
        self,
        *,
        firm_quote_id: str,
        phase: str,
        side: str,
        attempt: int = 0,
        exclude_trade_key: str | None = None,
        exclude_intent: VarOrderIntent | None = None,
    ) -> tuple[int, int]:
        occupied = self._occupied_lighter_client_order_ids_locked(
            exclude_trade_key=exclude_trade_key,
            exclude_intent=exclude_intent,
        )
        market = self.variational_ticker or self.ticker or "UNKNOWN"
        for collision in range(LIGHTER_CLIENT_ORDER_COLLISION_LIMIT):
            candidate = deterministic_lighter_client_order_index(
                account_index=self.account_index,
                market=market,
                firm_quote_id=firm_quote_id,
                phase=phase,
                side=side,
                attempt=attempt,
                collision=collision,
            )
            if candidate not in occupied:
                return candidate, collision
        raise RuntimeError("Unable to reserve a unique deterministic Lighter client order index")

    def _apply_intent_metadata_locked(
        self,
        record: OrderLifecycle,
        intent: VarOrderIntent,
    ) -> None:
        record.trace_id = record.trace_id or intent.trace_id
        record.strategy_phase = record.strategy_phase or intent.phase
        record.firm_quote_id = record.firm_quote_id or intent.firm_quote_id
        record.firm_price = record.firm_price or intent.firm_price
        record.firm_guard_pnl = record.firm_guard_pnl or intent.firm_guard_pnl
        record.firm_required_pnl = record.firm_required_pnl or intent.firm_required_pnl
        record.execution_reserve_usd = record.execution_reserve_usd or intent.execution_reserve_usd
        if record.adaptive_strategy_context is None and intent.adaptive_strategy_context is not None:
            record.adaptive_strategy_context = dict(intent.adaptive_strategy_context)
        if record.strategy_tag == MANUAL_STRATEGY_TAG:
            record.strategy_tag = intent.strategy_tag
        if (
            intent.phase == "open"
            and record.open_notional_usd is None
            and record.var_fill_price is not None
        ):
            record.open_notional_usd = record.qty * record.var_fill_price
        if not record.lighter_client_order_ids and record.lighter_reserved_client_order_id is None:
            record.lighter_reserved_client_order_id = intent.lighter_client_order_index
        if intent.phase == "operator_var_only_close":
            record.auto_hedge_enabled = False
            record.lighter_reduce_only = False
        elif intent.phase != "open":
            record.lighter_reduce_only = True
        if record.execution_state == "UNKNOWN":
            record.execution_state = EXECUTION_STATE_VAR_COMMITTED

    def mark_var_intent_sent(
        self,
        phase: str,
        side: str,
        amount: Decimal,
    ) -> VarOrderIntent:
        intent = VarOrderIntent(
            phase=phase,
            side=side.strip().upper(),
            amount=amount,
            sent_monotonic=time.monotonic(),
            market=(self.variational_ticker or "").upper(),
            state=VAR_INTENT_QUOTING,
            sent_at_iso=utc_now(),
            strategy_tag=ADAPTIVE_MODEL_VERSION,
        )
        self.pending_var_intent = intent
        return intent

    async def mark_and_persist_var_intent(
        self,
        phase: str,
        side: str,
        amount: Decimal,
    ) -> VarOrderIntent:
        intent = self.mark_var_intent_sent(phase, side, amount)
        await self.persist_runtime_state()
        return intent

    async def prepare_pending_var_intent(
        self,
        *,
        phase: str,
        side: str,
        amount: Decimal,
        trace_id: str,
        firm_quote: dict[str, Any],
        expected_intent: VarOrderIntent | None = None,
    ) -> VarOrderIntent | None:
        side_n = side.strip().upper()
        quote_id = str(firm_quote.get("quoteId") or "").strip()
        firm_price = to_decimal(firm_quote.get("firmPrice"))
        firm_qty = to_decimal(firm_quote.get("firmQty"))
        if not quote_id or firm_price is None or firm_price <= 0 or firm_qty is None or firm_qty <= 0:
            return None
        firm_target_notional = (
            to_decimal(firm_quote.get("targetNotionalUsd"))
            or to_decimal(firm_quote.get("firmNotionalUsd"))
            or amount
        )
        if firm_target_notional <= 0:
            return None

        async with self._record_lock:
            intent = self.pending_var_intent
            if expected_intent is not None:
                if (
                    intent is not expected_intent
                    or intent.state != VAR_INTENT_QUOTING
                    or intent.phase != phase
                    or intent.side != side_n
                    or intent.amount != amount
                    or intent.market != (self.variational_ticker or "").upper()
                ):
                    return None
            if intent is not None and (intent.phase != phase or intent.side != side_n):
                return None
            if intent is None:
                if expected_intent is not None:
                    return None
                intent = VarOrderIntent(
                    phase=phase,
                    side=side_n,
                    amount=amount,
                    sent_monotonic=time.monotonic(),
                    market=(self.variational_ticker or "").upper(),
                    sent_at_iso=utc_now(),
                    strategy_tag=ADAPTIVE_MODEL_VERSION,
                )
                self.pending_var_intent = intent
            if phase == "operator_var_only_close":
                intent.lighter_client_order_index = None
                intent.lighter_client_order_collision = 0
            elif (
                intent.firm_quote_id != quote_id
                or intent.lighter_client_order_index is None
            ):
                (
                    intent.lighter_client_order_index,
                    intent.lighter_client_order_collision,
                ) = self._reserve_lighter_client_order_index_locked(
                    firm_quote_id=quote_id,
                    phase=phase,
                    side=side_n,
                    exclude_intent=intent,
                )
            intent.state = VAR_INTENT_PREPARED
            intent.trace_id = trace_id
            intent.firm_quote_id = quote_id
            intent.firm_price = firm_price
            intent.firm_qty = firm_qty
            intent.firm_target_notional_usd = firm_target_notional
            intent.firm_guard_pnl = to_decimal(firm_quote.get("guardPnl"))
            intent.firm_required_pnl = to_decimal(firm_quote.get("guardMinPnl"))
            intent.execution_reserve_usd = to_decimal(firm_quote.get("executionReserveUsd"))
            intent.lighter_vwap = to_decimal(firm_quote.get("lighterVwap"))
            intent.lighter_quote_age_ms = to_int(firm_quote.get("lighterQuoteAgeMs"))
            dynamic_context = firm_quote.get("adaptiveStrategy")
            intent.adaptive_strategy_context = (
                dict(dynamic_context) if isinstance(dynamic_context, dict) else None
            )
            strategy_tag = str(
                firm_quote.get("strategyTag") or ADAPTIVE_MODEL_VERSION
            ).strip()
            intent.strategy_tag = strategy_tag or ADAPTIVE_MODEL_VERSION
            intent.prepared_at_iso = utc_now()

        await self.persist_runtime_state()
        return intent

    async def mark_pending_var_intent_committing(
        self,
        *,
        phase: str,
        side: str,
        trace_id: str,
        expected_intent: VarOrderIntent | None = None,
    ) -> bool:
        async with self._record_lock:
            intent = self.pending_var_intent
            if (
                intent is None
                or (expected_intent is not None and intent is not expected_intent)
                or intent.state != VAR_INTENT_PREPARED
                or intent.phase != phase
                or intent.side != side.strip().upper()
                or intent.trace_id != trace_id
            ):
                return False
            intent.state = VAR_INTENT_COMMITTING
            return True

    async def pending_var_commit_precondition_error(
        self,
        *,
        intent: VarOrderIntent,
        phase: str,
        side: str,
        amount: Decimal,
        trace_id: str,
        expected_open_trade_key: str | None,
        base_qty: Decimal | None,
        require_live_ready: bool,
    ) -> str | None:
        """Atomically validate and cross the local Var Commit boundary.

        Keeping the intent PREPARED until every final gate passes prevents a
        same-sized manual fill from being mistaken for an automatic Commit
        that was never dispatched.
        """

        side_n = side.strip().upper()
        phase_n = phase.strip().lower()
        async with self._record_lock:
            current_intent = self.pending_var_intent
            if (
                current_intent is not intent
                or intent.state != VAR_INTENT_PREPARED
                or intent.phase != phase
                or intent.side != side_n
                or intent.amount != amount
                or intent.trace_id != trace_id
                or intent.market != (self.variational_ticker or "").upper()
            ):
                return "original Var execution intent changed before commit"
            if self.automation_paused:
                return "automation paused before Var commit"
            if self._asset_switch_in_progress:
                return "market switch started before Var commit"
            if self._canary_session_state == CANARY_SESSION_HALTED:
                return "live execution entered a safety pause before Var commit"

            ordered_records = [
                self.records[key]
                for key in self.record_order
                if key in self.records
            ]
            current_open, _rounds = build_trade_rounds(ordered_records)
            if phase_n == "open":
                if current_open is not None:
                    return "position changed before Var open commit"
                if require_live_ready and (
                    self.strategy_config.execution_mode != "live"
                    or self._canary_session_state != CANARY_SESSION_ARMED
                ):
                    return "live execution readiness changed before Var open commit"
                if require_live_ready and not self.lighter_order_entry_is_ready():
                    return "dedicated Lighter order-entry WebSocket disconnected before Var open commit"
            elif phase_n == "close":
                if (
                    current_open is None
                    or expected_open_trade_key is None
                    or current_open.trade_key != expected_open_trade_key
                    or base_qty is None
                    or current_open.qty != base_qty
                ):
                    return "position changed before Var close commit"
            else:
                return "unsupported guarded Var commit phase"
            intent.state = VAR_INTENT_COMMITTING
        return None

    async def mark_pending_var_intent_commit_ambiguous(
        self,
        *,
        phase: str,
        side: str,
        trace_id: str,
        expected_intent: VarOrderIntent,
    ) -> bool:
        """Make an uncertain Commit recoverable and release an observed fill."""

        record_to_schedule: OrderLifecycle | None = None
        payload: dict[str, Any] | None = None
        async with self._record_lock:
            intent = self.pending_var_intent
            if (
                intent is not expected_intent
                or intent.state != VAR_INTENT_COMMITTING
                or intent.phase != phase
                or intent.side != side.strip().upper()
                or intent.trace_id != trace_id
            ):
                return False
            intent.state = VAR_INTENT_COMMIT_AMBIGUOUS
            if intent.confirmed_trade_key:
                record = self.records.get(intent.confirmed_trade_key)
                if (
                    record is not None
                    and record.var_fill_source in {"event", "portfolio"}
                    and record.last_variational_status == "filled"
                    and record.hedge_status == "waiting_commit"
                ):
                    if phase == "emergency_close":
                        record.hedge_status = "recovery_required"
                        record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                        record.hedge_error = (
                            "Emergency Var close confirmed while Commit response was ambiguous; "
                            "reconcile residual Lighter exposure"
                        )
                    else:
                        record.hedge_status = "not_started"
                        record.execution_state = EXECUTION_STATE_VAR_COMMITTED
                        record.hedge_error = None
                        record_to_schedule = record
                    self.pending_var_intent = None
                    payload = record.to_payload()

        if record_to_schedule is not None and self.args.auto_hedge:
            self.schedule_lighter_order(record_to_schedule)
        if payload is not None:
            await self.append_order_log("variational_commit_ambiguous_fill_released", payload)
        await self.persist_runtime_state()
        return True

    def has_pending_var_intent(self) -> bool:
        return self.pending_var_intent is not None

    def expire_pending_var_intent(self) -> bool:
        intent = self.pending_var_intent
        if intent is None:
            return False
        if time.monotonic() - intent.sent_monotonic <= AUTO_VAR_FILL_TIMEOUT_SECONDS:
            return False
        if not self.automation_paused:
            self.pause_automation(
                f"Var {intent.phase} {intent.side} sent but no fill event within "
                f"{AUTO_VAR_FILL_TIMEOUT_SECONDS:.0f}s"
            )
        return True

    async def inspect_pending_var_intent_from_portfolio(
        self,
    ) -> VarPortfolioRecoveryOutcome:
        intent = self.pending_var_intent
        if intent is None:
            return VarPortfolioRecoveryOutcome.UNKNOWN
        # A live Commit may produce a trade/portfolio event before its HTTP
        # response.  That is a normal ordering race, not crash recovery.  Keep
        # the prepared intent intact until Commit returns explicitly.
        if (
            intent.state == VAR_INTENT_COMMITTING
            and intent.commit_accepted_monotonic is None
        ):
            return VarPortfolioRecoveryOutcome.UNKNOWN
        if time.monotonic() - intent.sent_monotonic < VAR_PORTFOLIO_RECOVERY_DELAY_SECONDS:
            return VarPortfolioRecoveryOutcome.UNKNOWN
        if not self.variational_ticker or intent.market != self.variational_ticker:
            return VarPortfolioRecoveryOutcome.UNKNOWN

        var_position, avg_entry_price, updated_at, portfolio_age = (
            await self.get_variational_position_snapshot(intent.market)
        )
        if portfolio_age is None or portfolio_age > VAR_PORTFOLIO_RECOVERY_MAX_AGE_SECONDS:
            return VarPortfolioRecoveryOutcome.UNKNOWN
        portfolio_received_monotonic = time.monotonic() - portfolio_age
        if (
            intent.commit_accepted_monotonic is not None
            and portfolio_received_monotonic < intent.commit_accepted_monotonic
        ):
            return VarPortfolioRecoveryOutcome.UNKNOWN

        current_open = await self._current_open_record(
            exclude_trade_key=intent.provisional_trade_key,
        )
        side = intent.side.lower()
        if intent.phase == "open":
            expected_sign = Decimal("1") if intent.side == "BUY" else Decimal("-1")
            if var_position * expected_sign > VAR_POSITION_TOLERANCE:
                recovered_qty = abs(var_position)
                recovered_price = avg_entry_price or intent.firm_price
            elif abs(var_position) <= VAR_POSITION_TOLERANCE:
                return VarPortfolioRecoveryOutcome.CONFIRMED_NOT_FILLED
            else:
                return VarPortfolioRecoveryOutcome.UNKNOWN
        elif intent.phase in {"close", "emergency_close"}:
            if current_open is None:
                return VarPortfolioRecoveryOutcome.UNKNOWN
            if abs(var_position) <= VAR_POSITION_TOLERANCE:
                recovered_qty = current_open.qty
                recovered_price = intent.firm_price
            else:
                expected_open_position = current_open.qty * (
                    Decimal("1")
                    if current_open.side == "buy"
                    else Decimal("-1")
                )
                if (
                    abs(var_position - expected_open_position)
                    <= VAR_POSITION_TOLERANCE
                ):
                    return VarPortfolioRecoveryOutcome.CONFIRMED_NOT_FILLED
                return VarPortfolioRecoveryOutcome.UNKNOWN
        else:
            return VarPortfolioRecoveryOutcome.UNKNOWN

        if recovered_price is None:
            var_bid, var_ask, _ = await self.get_variational_best_bid_ask(intent.market)
            recovered_price = var_ask if intent.side == "BUY" else var_bid
        if recovered_price is None or recovered_price <= 0:
            return VarPortfolioRecoveryOutcome.UNKNOWN

        recovered_trade_id = intent.order_id or (
            f"portfolio-{intent.phase}-{int(time.time() * 1000)}"
        )
        event = {
            "trade_id": recovered_trade_id,
            "side": side,
            "qty": decimal_to_str(recovered_qty),
            "asset": intent.market,
            "status": "filled",
            "price": decimal_to_str(recovered_price),
            "timestamp": updated_at or utc_now(),
            "recovered_from_portfolio": True,
        }
        await self.process_variational_trade_event(event)
        if self.pending_var_intent is intent:
            self.clear_matching_var_intent(intent.side, force=True)
        self.logger.warning(
            "Recovered missing Var %s fill from portfolio: side=%s qty=%s price=%s",
            intent.phase,
            intent.side,
            recovered_qty,
            recovered_price,
        )
        await self.append_order_log("variational_fill_recovered", event)
        await self.persist_runtime_state()
        return VarPortfolioRecoveryOutcome.FILLED

    async def recover_pending_var_intent_from_portfolio(self) -> bool:
        return (
            await self.inspect_pending_var_intent_from_portfolio()
            is VarPortfolioRecoveryOutcome.FILLED
        )

    async def var_intent_watchdog_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(0.25)
            try:
                recovery_outcome = (
                    await self.inspect_pending_var_intent_from_portfolio()
                )
                if recovery_outcome is VarPortfolioRecoveryOutcome.FILLED:
                    continue
                intent = self.pending_var_intent
                if (
                    intent is not None
                    and intent.commit_accepted_monotonic is not None
                    and time.monotonic() - intent.commit_accepted_monotonic
                    >= VAR_COMMIT_CONFIRM_TIMEOUT_SECONDS
                ):
                    with contextlib.suppress(Exception):
                        await self.refresh_pending_lighter_orders()
                    recovery_outcome = (
                        await self.inspect_pending_var_intent_from_portfolio()
                    )
                    if (
                        recovery_outcome
                        is VarPortfolioRecoveryOutcome.CONFIRMED_NOT_FILLED
                    ):
                        rolled_back = await self.rollback_unconfirmed_var_commit(
                            expected_intent=intent,
                        )
                        if not rolled_back:
                            await self.mark_unconfirmed_var_commit_ambiguous(intent)
                    elif recovery_outcome is VarPortfolioRecoveryOutcome.UNKNOWN:
                        await self.mark_unconfirmed_var_commit_ambiguous(intent)
                    continue
                self.expire_pending_var_intent()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Var intent portfolio recovery failed: %s", exc)

    def clear_matching_var_intent(
        self,
        side: str,
        *,
        event: dict[str, Any] | None = None,
        force: bool = False,
        expected_intent: VarOrderIntent | None = None,
    ) -> bool:
        intent = self.pending_var_intent
        if intent is None:
            return False
        if expected_intent is not None and intent is not expected_intent:
            return False
        normalized_side = "BUY" if side.strip().lower() == "buy" else "SELL"
        if intent.side != normalized_side:
            return False
        if not force and event is not None and not self.var_event_matches_intent(intent, event):
            return False
        self.pending_var_intent = None
        return True

    def var_event_matches_intent(self, intent: VarOrderIntent, event: dict[str, Any]) -> bool:
        # A quote or locally prepared intent has not crossed the exchange
        # commit boundary.  Binding a same-sized manual fill at either stage
        # could create an unintended Lighter hedge.  Only a Commit that was
        # dispatched (or subsequently acknowledged/left ambiguous) may own a
        # Var fill event.
        allowed_state = intent.state in {
            VAR_INTENT_COMMITTING,
            VAR_INTENT_COMMIT_AMBIGUOUS,
            VAR_INTENT_COMMITTED,
        }
        # PREPARED is intentionally excluded for live events: Commit has not
        # crossed the local dispatch boundary.  After a restart, however, the
        # durable file can still show PREPARED even when Commit and the
        # deterministic Lighter send both completed just before a crash.  Only
        # the internally-created, authoritative portfolio recovery event may
        # correlate that state, so it can query the reserved Lighter id before
        # deciding whether any resend is safe.
        if event.get("recovered_from_portfolio") and intent.state == VAR_INTENT_PREPARED:
            allowed_state = True
        if not allowed_state:
            return False
        side = str(event.get("side") or "").strip().upper()
        asset = str(event.get("asset") or "").strip().upper()
        if side != intent.side or (intent.market and asset != intent.market):
            return False
        event_source_rfq = variational_event_source_id(event, "source_rfq")
        if (
            event_source_rfq is not None
            and intent.commit_rfq_id is not None
            and event_source_rfq != intent.commit_rfq_id
        ):
            return False

        event_id = str(event.get("trade_id") or "").strip()
        if intent.order_id:
            return event_id == str(intent.order_id)

        qty = to_decimal(event.get("qty"))
        price = to_decimal(event.get("price"))
        if qty is None or price is None or qty <= 0 or price <= 0 or intent.amount <= 0:
            return False
        if intent.firm_qty is not None and intent.firm_qty > 0:
            qty_tolerance = max(VAR_POSITION_TOLERANCE, intent.firm_qty * Decimal("0.01"))
            if abs(qty - intent.firm_qty) > qty_tolerance:
                return False
        event_notional = qty * price
        # ``intent.amount`` is the authorization/request amount.  Quantity-
        # based closes can legitimately have a different notional after the
        # market moves, so once Firm Quote is frozen its exact economics are
        # the correlation anchor for both open and close fills.
        expected_notional = intent.amount
        if (
            intent.firm_price is not None
            and intent.firm_price > 0
            and intent.firm_qty is not None
            and intent.firm_qty > 0
        ):
            expected_notional = intent.firm_price * intent.firm_qty
        if abs(event_notional - expected_notional) / expected_notional > Decimal("0.05"):
            return False

        # Without an exchange order id, amount alone is not enough: a replayed
        # manual/template trade can have the same side and size.  Require the
        # exchange trade timestamp to follow this exact local intent.
        intent_time = parse_iso_datetime(intent.prepared_at_iso or intent.sent_at_iso or "")
        event_time = parse_iso_datetime(str(event.get("timestamp") or ""))
        if intent_time is not None and self.var_event_accept_after is not None:
            if event_time is None:
                return False
            if event_time < intent_time - timedelta(seconds=2):
                return False
        return True

    def automation_can_submit_var_order(
        self,
        status_target: str,
        *,
        allow_reconcile_degraded: bool = False,
    ) -> bool:
        # A degraded read-only account snapshot may temporarily permit closing
        # a locally tracked position.  It never bypasses a safety pause or a
        # fresh position/order mismatch.
        degraded_close_allowed = bool(
            allow_reconcile_degraded
            and not self.automation_paused
            and self.last_reconcile_outcome
            in {AccountReconcileOutcome.STALE, AccountReconcileOutcome.UNKNOWN}
        )
        if self.automation_paused:
            return False
        if self._asset_switch_in_progress:
            setattr(self, status_target, "switching market")
            return False
        if self.expire_pending_var_intent():
            return False
        if self.pending_var_intent is not None:
            intent = self.pending_var_intent
            setattr(self, status_target, f"waiting Var fill {intent.phase} {intent.side}")
            return False
        if self.transition_in_progress():
            setattr(self, status_target, "waiting Lighter hedge")
            return False
        if getattr(self, status_target, "") == "waiting Lighter hedge":
            setattr(self, status_target, "-")
        if not self.automation_ready and not degraded_close_allowed:
            if self.last_reconcile_outcome is AccountReconcileOutcome.FRESH_MISMATCH:
                setattr(self, status_target, "blocked: fresh account mismatch")
            return False
        return True

    def round_cooldown_remaining_seconds(self) -> int:
        cooldown = max(0, self.strategy_config.round_cooldown_seconds)
        if cooldown == 0 or self._last_round_closed_at <= 0:
            return 0
        elapsed = time.time() - self._last_round_closed_at
        return max(0, int(cooldown - elapsed + 0.999))

    def live_open_block_reason(self) -> str | None:
        """Return the first independent gate blocking a new live open."""

        config = self.strategy_config
        if config.execution_mode != "live":
            return "observe mode never opens a position"
        if not self.args.auto_hedge:
            return "automatic Lighter hedge is disabled"
        if self.operator_open_paused:
            return "new opens are paused by operator"
        if self.automation_paused:
            self._canary_session_state = CANARY_SESSION_HALTED
            return "automation is paused"
        if self._canary_session_state in {
            CANARY_SESSION_REVIEW_REQUIRED,
            CANARY_SESSION_HALTED,
        }:
            # Normalize legacy one-shot/loss-limit state. Real safety pauses
            # are represented by automation_paused and were handled above.
            self._canary_session_state = CANARY_SESSION_OBSERVING
        if self.active_parameter_epoch is None:
            return "parameter epoch is not yet available"
        if self.active_parameter_epoch.window_source != "live" or not all(
            (window := self.strategy_window_stats.get(side, {}).get(60))
            is not None
            and window.ready
            and window.source == "live"
            for side in StrategySide
        ):
            return "one-hour observe warmup is not complete"
        if not self.lighter_order_entry_is_ready():
            return "dedicated Lighter order-entry WebSocket is not ready"
        snapshot = self.last_account_snapshot
        if snapshot is None:
            return "fresh account reconciliation is required"
        snapshot_at = parse_iso_datetime(snapshot.captured_at)
        maximum_account_age = max(
            10.0,
            float(config.reconcile_interval_seconds) * 2,
        )
        if (
            snapshot_at is None
            or (datetime.now(timezone.utc) - snapshot_at).total_seconds()
            > maximum_account_age
        ):
            return "account reconciliation snapshot is stale"
        if (
            abs(snapshot.var_position) > VAR_POSITION_TOLERANCE
            or abs(snapshot.lighter_position) > VAR_POSITION_TOLERANCE
            or snapshot.lighter_active_orders != 0
        ):
            return "account must be flat with no active Lighter orders"
        if self.pending_var_intent is not None or self.transition_in_progress():
            return "an execution intent or hedge is still active"
        self._canary_session_state = CANARY_SESSION_ARMED
        return None

    async def record_completed_canary_round(
        self,
        close_record: OrderLifecycle,
    ) -> bool:
        """Record a fully settled round and adaptive live-session metrics.

        The post-round cooldown starts only after both exchanges have confirmed
        both legs.  Starting it at the Var close fill shortens the advertised
        cooldown by however long the final Lighter hedge takes.
        """

        async with self._record_lock:
            ordered_records = [
                self.records[key]
                for key in self.record_order
                if key in self.records
            ]
            _current_open, rounds = build_trade_rounds(ordered_records)
            settled_round = next(
                (
                    trade_round
                    for trade_round in reversed(rounds)
                    if trade_round.close_record.trade_key == close_record.trade_key
                    and trade_round.open_record.var_fill_source
                    in {"event", "portfolio"}
                    and trade_round.close_record.var_fill_source
                    in {"event", "portfolio"}
                    and trade_round.open_record.last_variational_status == "filled"
                    and trade_round.close_record.last_variational_status == "filled"
                    and lighter_hedge_filled(trade_round.open_record)
                    and lighter_hedge_filled(trade_round.close_record)
                ),
                None,
            )
            if settled_round is None:
                return False
            if close_record.trade_key not in self._round_cooldown_close_keys:
                self._round_cooldown_close_keys.add(close_record.trade_key)
                self._last_round_closed_at = time.time()
            if (
                settled_round.open_record.strategy_tag != ADAPTIVE_MODEL_VERSION
                or close_record.trade_key in self._canary_completed_close_keys
            ):
                return False
            self._canary_completed_close_keys.add(close_record.trade_key)
            self._canary_round_count += 1
            round_pnl = settled_round.round_pnl
            if round_pnl is not None and round_pnl < 0:
                self._canary_cumulative_loss_usd += -round_pnl
                self._canary_consecutive_losses += 1
            elif round_pnl is not None:
                self._canary_consecutive_losses = 0
            self._canary_session_state = (
                CANARY_SESSION_HALTED
                if self.automation_paused
                else CANARY_SESSION_ARMED
            )
            stats = {
                "round_count": self._canary_round_count,
                "round_pnl": round_pnl,
                "cumulative_loss_usd": self._canary_cumulative_loss_usd,
                "consecutive_losses": self._canary_consecutive_losses,
                "state_after": self._canary_session_state,
            }
        self.last_auto_var_order_status = (
            "live round complete; automation remains paused"
            if self._canary_session_state == CANARY_SESSION_HALTED
            else "live round complete; continuous execution remains ARMED"
        )
        self.trace_event(
            "live_round_complete",
            close_record.trace_id,
            close_trade_key=close_record.trade_key,
            **stats,
        )
        return True

    async def record_completed_canary_round_for_leg(
        self,
        record: OrderLifecycle,
    ) -> bool:
        """Settle a round when this update supplied whichever confirmation was last."""

        async with self._record_lock:
            ordered_records = [
                self.records[key]
                for key in self.record_order
                if key in self.records
            ]
            _current_open, rounds = build_trade_rounds(ordered_records)
            close_records = [
                trade_round.close_record
                for trade_round in rounds
                if record.trade_key
                in {
                    trade_round.open_record.trade_key,
                    trade_round.close_record.trade_key,
                }
            ]
        settled = False
        for close_record in close_records:
            settled = (
                await self.record_completed_canary_round(close_record)
                or settled
            )
        return settled

    def print_startup_next_steps(self) -> None:
        is_zh = self.args.lang == "zh"
        if is_zh:
            lines = [
                "Python 脚本已就位，请回到 Chrome 加载并启动扩展。若 Chrome 插件已启动，请刷新网页。",
                "如需英文看板，可使用 `python main.py --lang en` 启动。",
            ]
            title = "启动指引"
        else:
            lines = [
                "Python runtime is ready. Go back to Chrome and load/start the extension.",
                "If the Chrome extension has already started, please refresh the webpage."
            ]
            title = "Startup Guide"
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style="yellow"))

    def setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None) -> None:
        self.stop_flag = True

    def initialize_lighter_client(self) -> SignerClient:
        if self.lighter_client is None:
            api_key_private_key = required_env("LIGHTER_PRIVATE_KEY")
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key},
            )
            err = self.lighter_client.check_client()
            if err is not None:
                raise RuntimeError(f"CheckClient error: {err}")
        return self.lighter_client

    async def get_lighter_market_config(self) -> tuple[int, int, int]:
        if not self.ticker:
            raise RuntimeError("Ticker is not resolved yet")
        def fetch_market_data() -> dict[str, Any]:
            response = requests.get(
                f"{self.lighter_base_url}/api/v1/orderBooks",
                headers={"accept": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            return response.json()

        data = await asyncio.to_thread(fetch_market_data)

        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                price_decimals = int(market["supported_price_decimals"])
                size_decimals = int(market["supported_size_decimals"])
                return int(market["market_id"]), pow(10, size_decimals), pow(10, price_decimals)

        raise RuntimeError(f"Ticker {self.ticker} not found in Lighter order books")

    async def detect_current_variational_asset(self) -> str | None:
        async with self.runtime.monitor._lock:
            if self.runtime.monitor.current_quote_asset:
                asset = str(self.runtime.monitor.current_quote_asset).strip().upper()
                quote = self.runtime.monitor.quotes.get(asset)
                if (
                    asset
                    and asset != "UNKNOWN"
                    and isinstance(quote, dict)
                    and to_decimal(quote.get("bid")) is not None
                    and to_decimal(quote.get("ask")) is not None
                ):
                    return asset

        return None

    async def wait_for_ticker_resolution(self) -> str:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            asset = await self.detect_current_variational_asset()
            if asset:
                return asset
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        raise RuntimeError("Timed out deriving ticker from Variational quote/trade messages")

    async def _reset_state_for_asset_switch(self) -> None:
        async with self._record_lock:
            self.records.clear()
            self.record_order.clear()
            self.lighter_client_order_to_trade_key.clear()
            self.lighter_order_fill_totals.clear()
            self.lighter_order_terminal_ids.clear()
            self.lighter_retry_pending_keys.clear()
            self.lighter_order_tasks_by_trade_key.clear()
            self.lighter_requeue_after_task_keys.clear()
        self.strategy_window_store = RollingWindowStore()
        self.strategy_window_stats = {StrategySide.BUY: {}, StrategySide.SELL: {}}
        self.strategy_epoch_activator = EpochActivator(
            model=self.strategy_model,
            confirmations=self.strategy_config.parameter_confirmations,
        )
        self.active_parameter_epoch = None
        self.last_market_frame = None
        self._dashboard_last_market_frame = None
        self._last_valid_strategy_frame_ms = None
        self._last_recorded_strategy_sample_ms = None
        self._strategy_parameter_block_reason = "strategy_windows_not_ready"
        self.last_strategy_decision = None
        self.last_strategy_decision_at_ms = None
        self._selected_open_candidate = None
        self._last_open_decision_trace_signature = None
        self._last_open_decision_trace_ms = 0
        self._opportunity_samples = {
            StrategySide.BUY: deque(),
            StrategySide.SELL: deque(),
        }
        self._close_range_deferral_started_ms.clear()
        self._last_parameter_refresh_ms = 0
        self._strategy_started_at_ms = time.time_ns() // 1_000_000
        self._strategy_history_resume_pending = False
        self._strategy_history_resume_state = "not_loaded"
        self._strategy_history_resume_samples = 0
        self._strategy_history_resume_coverage_ms = 0
        self._strategy_history_resume_gap_ms = None
        self.execution_loss_samples.clear()
        self.execution_loss_sample_records.clear()
        self._execution_samples_revision = 0
        self._execution_samples_persisted_revision = 0
        self._open_execution_headroom_cache.clear()
        self.pending_var_intent = None
        self.automation_ready = False

    @staticmethod
    def _read_strategy_sample_history(
        path: Path,
        *,
        asset: str,
        reference_notional_usd: Decimal,
        order_notional_usd: Decimal,
        now_ms: int,
        minimum_timestamp_ms: int | None = None,
    ) -> tuple[str, list[tuple[int, DirectionalRates]], int]:
        """Read the latest qualified contiguous session without trusting prices.

        Historical rows only seed robust rate statistics.  They never create a
        MarketFrame, an order candidate, or an execution price.  A new live
        frame is still mandatory after restart.  Partial sessions may continue
        sampling across a short restart, while normal window readiness still
        prevents trading before enough coverage exists.
        """

        if not path.is_file():
            return "sample_file_missing", [], 0
        valid: list[tuple[int, DirectionalRates]] = []
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        # A process killed during an append may leave one
                        # partial tail row.  It contributes no data and cannot
                        # weaken the contiguous-gap validation below.
                        continue
                    if not isinstance(row, dict) or row.get("valid") is not True:
                        continue
                    if row.get("version") != STRATEGY_MARKET_SAMPLE_VERSION:
                        continue
                    if str(row.get("asset") or "").strip().upper() != asset:
                        continue
                    if to_decimal(row.get("reference_notional_usd")) != reference_notional_usd:
                        continue
                    if to_decimal(row.get("order_notional_usd")) != order_notional_usd:
                        continue
                    timestamp_ms = row.get("sample_timestamp_ms")
                    buy = to_decimal(row.get("reference_buy_rate"))
                    sell = to_decimal(row.get("reference_sell_rate"))
                    if (
                        isinstance(timestamp_ms, bool)
                        or not isinstance(timestamp_ms, int)
                        or timestamp_ms < 0
                        or (
                            minimum_timestamp_ms is not None
                            and timestamp_ms < minimum_timestamp_ms
                        )
                        or buy is None
                        or sell is None
                    ):
                        continue
                    valid.append(
                        (timestamp_ms, DirectionalRates(buy=buy, sell=sell))
                    )
        except OSError as exc:
            return f"sample_file_unreadable:{type(exc).__name__}", [], 0
        if not valid:
            return "no_matching_valid_samples", [], 0
        valid.sort(key=lambda item: item[0])
        latest_ms = valid[-1][0]
        if latest_ms > now_ms + 5_000:
            return "history_clock_from_future", [], 0
        initial_gap_ms = max(0, now_ms - latest_ms)
        if initial_gap_ms > STRATEGY_HISTORY_RESUME_MAX_GAP_MS:
            return "history_stale_over_5m", [], initial_gap_ms

        # Keep only the newest internally-contiguous session.  Earlier process
        # runs and all in-session gaps >=60s remain hard boundaries.
        session: list[tuple[int, DirectionalRates]] = [valid[-1]]
        newer_ms = latest_ms
        for sample in reversed(valid[:-1]):
            timestamp_ms = sample[0]
            if newer_ms - timestamp_ms >= STRATEGY_MAX_SAMPLE_GAP_MS:
                break
            session.append(sample)
            newer_ms = timestamp_ms
        session.reverse()
        coverage_ms = session[-1][0] - session[0][0]
        density = (
            Decimal(len(session))
            * Decimal("1000")
            / Decimal(max(1, coverage_ms))
        )
        if density < Decimal("0.10"):
            return "history_density_too_low", [], initial_gap_ms

        # Return only the rolling hour plus one boundary sample.  The boundary
        # proves complete one-hour coverage but is excluded by window medians.
        cutoff_ms = session[-1][0] - STRATEGY_STATISTICS_WINDOW_MS
        boundary_index = 0
        for index, sample in enumerate(session):
            if sample[0] <= cutoff_ms:
                boundary_index = index
                continue
            break
        resume_state = (
            "pending_first_live_frame"
            if coverage_ms >= STRATEGY_STATISTICS_WINDOW_MS
            else "pending_partial_history"
        )
        return resume_state, session[boundary_index:], initial_gap_ms

    def _strategy_sample_session_cutoff_ms(self, asset: str) -> int | None:
        path = self.strategy_market_samples_file
        if path is None:
            return None
        marker = path.with_name(STRATEGY_SAMPLE_SESSION_FILE_NAME)
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        marker_asset = str(payload.get("asset") or "").strip().upper()
        if marker_asset and marker_asset != asset:
            return None
        timestamp_ms = payload.get("minimum_sample_timestamp_ms")
        if (
            isinstance(timestamp_ms, bool)
            or not isinstance(timestamp_ms, int)
            or timestamp_ms < 0
        ):
            return None
        return timestamp_ms

    async def load_strategy_sample_history(self, asset: str) -> bool:
        path = self.strategy_market_samples_file
        if path is None:
            self._strategy_history_resume_state = "sampling_disabled"
            return False
        now_ms = time.time_ns() // 1_000_000
        minimum_timestamp_ms = self._strategy_sample_session_cutoff_ms(asset)
        state, samples, initial_gap_ms = await asyncio.to_thread(
            self._read_strategy_sample_history,
            path,
            asset=asset,
            reference_notional_usd=self.strategy_config.reference_notional_usd,
            order_notional_usd=self.strategy_config.order_notional_usd,
            now_ms=now_ms,
            minimum_timestamp_ms=minimum_timestamp_ms,
        )
        self._strategy_history_resume_state = state
        self._strategy_history_resume_gap_ms = initial_gap_ms
        if not samples:
            return False
        for timestamp_ms, rates in samples:
            self.strategy_window_store.add(timestamp_ms=timestamp_ms, rates=rates)
            for side in StrategySide:
                self._opportunity_samples[side].append(
                    OpportunitySample(timestamp_ms, rates.for_side(side))
                )
        latest_ms = samples[-1][0]
        cutoff = latest_ms - STRATEGY_STATISTICS_WINDOW_MS
        for side in StrategySide:
            while (
                self._opportunity_samples[side]
                and self._opportunity_samples[side][0].timestamp_ms < cutoff
            ):
                self._opportunity_samples[side].popleft()
        self._last_recorded_strategy_sample_ms = latest_ms
        self._strategy_history_resume_pending = True
        self._strategy_history_resume_samples = len(samples)
        self._strategy_history_resume_coverage_ms = samples[-1][0] - samples[0][0]
        self.logger.info(
            "Loaded %s qualified strategy samples for restart resume; coverage=%sms gap=%sms",
            len(samples),
            self._strategy_history_resume_coverage_ms,
            initial_gap_ms,
        )
        return True

    async def activate_asset(self, variational_asset: str, reason: str) -> None:
        asset = variational_asset.strip().upper()
        if not asset or asset == "UNKNOWN":
            return
        if asset != "BTC":
            raise RuntimeError(f"{ADAPTIVE_MODEL_VERSION} supports BTC only")

        async with self._asset_switch_lock:
            next_ticker = resolve_lighter_ticker(asset)
            if self.variational_ticker == asset and self.ticker == next_ticker:
                return

            if self.variational_ticker is not None:
                current_open = await self._current_open_record()
                if current_open is not None or self.transition_in_progress():
                    self.pause_automation(
                        f"Blocked market switch {self.variational_ticker}->{asset}: active trade state exists"
                    )
                    return

            self._asset_switch_in_progress = True
            self.automation_ready = False
            try:
                for task in list(self.hedge_tasks):
                    if not task.done():
                        task.cancel()
                if self.hedge_tasks:
                    await asyncio.gather(*self.hedge_tasks, return_exceptions=True)
                    self.hedge_tasks.clear()

                self.variational_ticker = asset
                self.ticker = next_ticker
                self.market_generation += 1
                self.accepted_assets = {
                    asset,
                    next_ticker,
                    resolve_variational_ticker(next_ticker),
                }

                (
                    self.lighter_market_index,
                    self.base_amount_multiplier,
                    self.price_multiplier,
                ) = await self.get_lighter_market_config()
                await self.reset_lighter_order_book()
                await self._reset_state_for_asset_switch()
                await self.load_execution_samples_for_asset(asset)
                await self.load_runtime_state(asset)
                await self.load_strategy_sample_history(asset)
                await self.persist_runtime_state()

                await self.stop_lighter_streams()
                await self.start_lighter_streams()
                await self.wait_for_lighter_order_book_ready()
                self.logger.info(
                    "Switched market (%s): variational_asset=%s -> lighter_ticker=%s market_id=%s",
                    reason,
                    self.variational_ticker,
                    self.ticker,
                    self.lighter_market_index,
                )
            finally:
                self._asset_switch_in_progress = False

    async def wait_for_variational_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise RuntimeError("Timed out waiting for Variational events stream heartbeat")

    async def wait_for_variational_portfolio_ready(self) -> None:
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.monotonic() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            portfolio_age = state.get("portfolio_age")
            if state.get("has_portfolio") and portfolio_age is not None and portfolio_age <= HEARTBEAT_STALE_SECONDS:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise RuntimeError("Timed out waiting for a fresh Variational portfolio snapshot")

    async def wait_for_lighter_order_book_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            if self.lighter_order_book_ready:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Timed out waiting for Lighter order book")

    async def reset_lighter_order_book(self) -> None:
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_ticks["bids"].clear()
            self.lighter_order_book_ticks["asks"].clear()
            self.lighter_vwap_cache.clear()
            self.lighter_execution_tick_cache.clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_nonce = None
            self.lighter_order_book_ready = False
            self.lighter_snapshot_loaded = False
            self.lighter_order_book_sequence_gap = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None
            self.lighter_book_received_monotonic = None

    def update_lighter_order_book(self, side: str, levels: list[Any]) -> None:
        price_multiplier = int(self.price_multiplier or 1)
        base_multiplier = int(self.base_amount_multiplier or 1)
        changed = False
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue

            scaled_price = price * price_multiplier
            scaled_size = size * base_multiplier
            price_tick = int(scaled_price.to_integral_value(rounding=ROUND_DOWN))
            size_tick = int(scaled_size.to_integral_value(rounding=ROUND_DOWN))
            if scaled_price != price_tick or scaled_size != size_tick:
                raise ValueError(
                    f"Lighter order-book level is not aligned to fixed precision: {price}/{size}"
                )
            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)
            if price_tick <= 0:
                continue
            previous_tick = self.lighter_order_book_ticks[side].get(price_tick)
            if size_tick > 0:
                self.lighter_order_book_ticks[side][price_tick] = size_tick
            else:
                self.lighter_order_book_ticks[side].pop(price_tick, None)
            changed = changed or previous_tick != (size_tick if size_tick > 0 else None)
        if changed:
            self.lighter_vwap_cache.clear()
            self.lighter_execution_tick_cache.clear()

    def refresh_lighter_best_prices_locked(self) -> None:
        price_multiplier = Decimal(str(self.price_multiplier or 1))
        bid_tick = max(self.lighter_order_book_ticks["bids"], default=None)
        ask_tick = min(self.lighter_order_book_ticks["asks"], default=None)
        if bid_tick is not None or ask_tick is not None:
            self.lighter_best_bid = (
                Decimal(bid_tick) / price_multiplier if bid_tick is not None else None
            )
            self.lighter_best_ask = (
                Decimal(ask_tick) / price_multiplier if ask_tick is not None else None
            )
            return
        # Compatibility for restored/mock books that predate the tick cache.
        self.lighter_best_bid = max(self.lighter_order_book["bids"], default=None)
        self.lighter_best_ask = min(self.lighter_order_book["asks"], default=None)

    def validate_order_book_update(self, order_book: dict[str, Any]) -> bool:
        new_offset = int(order_book.get("offset", 0) or 0)
        if new_offset <= self.lighter_order_book_offset:
            return False
        begin_nonce = order_book.get("begin_nonce")
        if begin_nonce is not None and self.lighter_order_book_nonce is not None:
            return int(begin_nonce) == self.lighter_order_book_nonce
        return True

    async def handle_lighter_fill_update(self, order: dict[str, Any]) -> None:
        """Route execution reports through one writer when the runtime is live."""
        task = self.execution_event_task
        if task is None or task.done() or asyncio.current_task() is task:
            await self._apply_lighter_fill_update(order)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        await self.execution_event_queue.put((order, future))
        await future

    async def execution_event_loop(self) -> None:
        try:
            while True:
                order, future = await self.execution_event_queue.get()
                try:
                    await self._apply_lighter_fill_update(order)
                except asyncio.CancelledError:
                    if not future.done():
                        future.cancel()
                    raise
                except Exception as exc:
                    if not future.done():
                        future.set_exception(exc)
                else:
                    if not future.done():
                        future.set_result(None)
                finally:
                    self.execution_event_queue.task_done()
        except asyncio.CancelledError:
            while True:
                try:
                    _order, future = self.execution_event_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    if not future.done():
                        future.cancel()
                finally:
                    self.execution_event_queue.task_done()
            return

    async def _apply_lighter_fill_update(self, order: dict[str, Any]) -> None:
        client_order_id_raw = order.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            return

        status = str(order.get("status") or "").lower()
        filled_quote = to_decimal(order.get("filled_quote_amount"))
        filled_base = to_decimal(order.get("filled_base_amount"))

        now_iso = utc_now()
        late_close_record: OrderLifecycle | None = None
        rollback_record: OrderLifecycle | None = None
        overfill_correction_record: OrderLifecycle | None = None
        overfill_reason: str | None = None
        should_pause = False
        should_retry = False
        event_name = "lighter_order_update"

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                return
            record = self.records.get(trade_key)
            if record is None:
                return
            if record.trace_id is None:
                record.trace_id = new_trace_id()
            if (
                record.hedge_status == "recovery_check"
                and client_order_id == record.lighter_reserved_client_order_id
                and client_order_id not in record.lighter_client_order_ids
            ):
                record.lighter_client_order_id = client_order_id
                record.lighter_client_order_ids.append(client_order_id)
            previous = self.lighter_order_fill_totals.get(
                client_order_id,
                (Decimal("0"), Decimal("0")),
            )
            current_total = (
                filled_base if filled_base is not None else previous[0],
                filled_quote if filled_quote is not None else previous[1],
            )
            # Both private-stream and REST reports carry cumulative totals.
            # A delayed terminal/partial report must never roll a newer fill
            # backwards or reopen a fully hedged lifecycle for retry.
            if current_total[0] < previous[0] or current_total[1] < previous[1]:
                return
            if (
                previous == current_total
                and client_order_id in self.lighter_order_terminal_ids
            ):
                return
            self.lighter_order_fill_totals[client_order_id] = current_total

            total_base = Decimal("0")
            total_quote = Decimal("0")
            for order_id in record.lighter_client_order_ids:
                base_part, quote_part = self.lighter_order_fill_totals.get(
                    order_id,
                    (Decimal("0"), Decimal("0")),
                )
                total_base += base_part
                total_quote += quote_part
            record.lighter_filled_qty = total_base
            record.lighter_filled_quote = total_quote
            record.lighter_fill_price = total_quote / total_base if total_base > 0 else None

            lighter_target = lighter_order_target_qty(
                record,
                self.base_amount_multiplier,
            )
            overfilled = lighter_target is not None and total_base > lighter_target
            fully_filled = lighter_target is not None and total_base == lighter_target
            terminal_status = (
                status == "filled"
                or status.startswith("canceled")
                or status in {"cancelled", "expired", "rejected"}
            )
            if terminal_status:
                self.lighter_order_terminal_ids.add(client_order_id)
            all_orders_terminal = set(record.lighter_client_order_ids).issubset(
                self.lighter_order_terminal_ids
            )
            record.lighter_outcome_final = bool(
                record.lighter_client_order_ids and all_orders_terminal
            )
            if overfilled and record.var_fill_source != "unconfirmed_commit":
                assert lighter_target is not None
                excess_qty = total_base - lighter_target
                record.hedge_status = "overfilled"
                record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                record.hedge_error = (
                    f"Lighter overfilled {total_base}/{lighter_target}; "
                    f"correcting excess {excess_qty}"
                )
                overfill_reason = record.hedge_error
                overfill_correction_record = self._build_lighter_qty_correction_locked(
                    record,
                    excess_qty,
                )
                event_name = "lighter_overfill"
            elif fully_filled:
                record.lighter_fill_ts_iso = now_iso
                record.hedge_status = "filled"
                record.execution_state = EXECUTION_STATE_HEDGED
                record.hedge_error = None
                event_name = "lighter_fill"
                late_close_record = self._find_waiting_close_for_open_locked(record)
            elif terminal_status:
                if not all_orders_terminal:
                    record.hedge_status = "partial" if total_base > 0 else "submitted"
                    record.execution_state = (
                        EXECUTION_STATE_HEDGE_PARTIAL
                        if total_base > 0
                        else EXECUTION_STATE_HEDGE_SUBMITTED
                    )
                    record.hedge_error = (
                        f"Waiting remaining Lighter IOC updates: filled {total_base}/{lighter_target or record.qty}"
                    )
                else:
                    attempts_left = (
                        len(record.lighter_client_order_ids)
                        < self.strategy_config.lighter_hedge_max_attempts
                    )
                    record.hedge_status = (
                        "retrying" if attempts_left else ("partial" if total_base > 0 else "error")
                    )
                    record.execution_state = (
                        EXECUTION_STATE_HEDGE_PARTIAL
                        if total_base > 0
                        else EXECUTION_STATE_HEDGE_ERROR
                    )
                    record.hedge_error = (
                        f"Lighter IOC {status or 'ended'}: filled {total_base}/{lighter_target or record.qty}"
                    )
                    should_retry = attempts_left
                    should_pause = not attempts_left
                event_name = "lighter_partial" if total_base > 0 else "lighter_error"
            elif total_base > 0:
                record.hedge_status = "partial"
                record.execution_state = EXECUTION_STATE_HEDGE_PARTIAL
                record.hedge_error = f"Lighter partially filled {total_base}/{record.qty}"
            else:
                record.hedge_status = "submitted"
                record.execution_state = EXECUTION_STATE_HEDGE_SUBMITTED
            if record.var_fill_source == "unconfirmed_commit":
                should_retry = False
                rollback_record = self._build_lighter_rollback_locked(record)
            if should_retry:
                if record.trade_key in self.lighter_retry_pending_keys:
                    should_retry = False
                else:
                    self.lighter_retry_pending_keys.add(record.trade_key)
            self._capture_execution_loss_locked(record)
            payload = record.to_payload()
            trace_id = record.trace_id

        self.trace_event(
            "lighter_order_report",
            trace_id,
            lifecycle_event=event_name,
            trade_key=record.trade_key,
            client_order_id=client_order_id,
            status=status,
            filled_base=filled_base,
            filled_quote=filled_quote,
            hedge_status=record.hedge_status,
            queued_at=order.get("queued_at"),
            executed_at=order.get("executed_at"),
            transaction_time=order.get("transaction_time"),
        )
        if event_name == "lighter_fill":
            self.trace_event(
                "lighter_fill",
                trace_id,
                trade_key=record.trade_key,
                client_order_id=client_order_id,
                filled_qty=record.lighter_filled_qty,
                fill_price=record.lighter_fill_price,
            )
        await self.append_order_log(event_name, payload)
        if overfill_correction_record is not None:
            self.schedule_lighter_order(overfill_correction_record)
            await self.append_order_log(
                "lighter_qty_correction_queued",
                overfill_correction_record.to_payload(),
            )
        if rollback_record is not None:
            self.schedule_lighter_order(rollback_record)
            await self.append_order_log("lighter_rollback_queued", rollback_record.to_payload())
        if event_name == "lighter_fill":
            await self.record_completed_canary_round_for_leg(record)
        await self.persist_runtime_state()
        if event_name == "lighter_fill":
            await self.prune_settled_execution_state()
        if should_retry or should_pause:
            async with self._record_lock:
                current_filled = record.lighter_filled_qty or Decimal("0")
                hedge_already_resolved = (
                    record.hedge_status in {"filled", "overfilled"}
                    or lighter_order_target_matches(
                        record,
                        current_filled,
                        self.base_amount_multiplier,
                    )
                )
                if hedge_already_resolved:
                    should_retry = False
                    should_pause = False
                    self.lighter_retry_pending_keys.discard(record.trade_key)
        if overfill_reason is not None:
            self.pause_automation(overfill_reason)
            await self.persist_runtime_state()
            return
        if should_retry:
            self.queue_lighter_retry_after_current(record)
            return
        if should_pause:
            self.pause_for_reconciliation(
                f"Lighter hedge failed: "
                f"{record.hedge_error or 'Lighter IOC did not fully fill'}"
            )
            await self.emergency_flatten_var(record)
        if late_close_record is not None:
            self.schedule_lighter_order(late_close_record)

    def _find_waiting_close_for_open_locked(self, open_record: OrderLifecycle) -> OrderLifecycle | None:
        try:
            open_index = list(self.record_order).index(open_record.trade_key)
        except ValueError:
            return None

        opposite_side = "sell" if open_record.side == "buy" else "buy"
        for trade_key in list(self.record_order)[open_index + 1 :]:
            candidate = self.records.get(trade_key)
            if candidate is None:
                continue
            if candidate.asset != open_record.asset or candidate.side != opposite_side:
                continue
            if candidate.lighter_fill_ts_iso is not None or candidate.lighter_client_order_id is not None:
                return None
            if candidate.hedge_status not in {"waiting_open_hedge", "skipped"}:
                return None
            candidate.lighter_reduce_only = True
            candidate.hedge_error = None
            return candidate
        return None

    def build_lighter_ws_url(self) -> str:
        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            return f"{LIGHTER_WS_URL}?server_pings=true"
        return LIGHTER_WS_URL

    def notify_market_signal(self) -> None:
        """Coalesce quote/trade/book bursts without putting work on their I/O path."""
        self._market_signal_revision += 1
        self._market_signal_event.set()

    def notify_variational_quote_signal(self) -> None:
        """Wake decisions and sample immediately after a fresh Var quote.

        Variational currently updates more slowly than the Lighter book.  A
        fixed one-second sampler can repeatedly miss the short interval after
        a fresh source quote.  This event is only a wake up: each source still
        has to satisfy its own freshness check in
        ``current_adaptive_market_frame``.
        """

        self._strategy_sample_event.set()
        self.notify_market_signal()

    def notify_trade_signal(self) -> None:
        """Mark a Var fill as decision-critical before waking shared consumers."""

        self._trade_signal_revision += 1
        self.notify_market_signal()

    async def wait_for_market_signal(self, last_revision: int) -> int:
        """Wait for a merged market update, with a slow health-check fallback."""
        while not self.stop_flag:
            revision = self._market_signal_revision
            if revision != last_revision:
                # Yield one turn so a quote/book burst becomes one evaluation.
                await asyncio.sleep(0)
                return self._market_signal_revision
            self._market_signal_event.clear()
            if self._market_signal_revision != last_revision:
                continue
            try:
                await asyncio.wait_for(
                    self._market_signal_event.wait(),
                    timeout=EVENT_SIGNAL_FALLBACK_SECONDS,
                )
            except asyncio.TimeoutError:
                return self._market_signal_revision
        return self._market_signal_revision

    async def stop_lighter_streams(self) -> None:
        tasks = {
            task
            for task in (
                self.lighter_ws_task,
                self.lighter_market_ws_task,
                self.lighter_private_ws_task,
            )
            if task is not None and not task.done()
        }
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.lighter_ws_task = None
        self.lighter_market_ws_task = None
        self.lighter_private_ws_task = None
        self.lighter_private_stream_ready = False

    async def start_lighter_streams(self) -> None:
        # Preserve the one-method seam used by local integrations.  The base
        # runtime always starts independent market and private connections.
        if type(self).handle_lighter_ws is not VariationalToLighterRuntime.handle_lighter_ws:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            return
        self.lighter_market_ws_task = asyncio.create_task(
            self.handle_lighter_market_ws(), name="lighter-market-ws"
        )
        self.lighter_private_ws_task = asyncio.create_task(
            self.handle_lighter_private_ws(), name="lighter-private-ws"
        )
        # Compatibility alias only; do not use it as the source of truth.
        self.lighter_ws_task = self.lighter_market_ws_task

    async def handle_lighter_ws(self) -> None:
        """Compatibility entry point for pre-P2 subclasses."""
        await asyncio.gather(
            self.handle_lighter_market_ws(), self.handle_lighter_private_ws()
        )

    async def handle_lighter_market_ws(self) -> None:
        while not self.stop_flag:
            try:
                await self.reset_lighter_order_book()
                async with websockets.connect(
                    self.build_lighter_ws_url(),
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}
                        )
                    )
                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")
                        if msg_type == "subscribed/order_book":
                            async with self.lighter_order_book_lock:
                                self.lighter_order_book["bids"].clear()
                                self.lighter_order_book["asks"].clear()
                                self.lighter_order_book_ticks["bids"].clear()
                                self.lighter_order_book_ticks["asks"].clear()
                                self.lighter_vwap_cache.clear()
                                self.lighter_execution_tick_cache.clear()
                                order_book = data.get("order_book", {})
                                self.lighter_order_book_offset = int(order_book.get("offset", 0) or 0)
                                nonce = order_book.get("nonce")
                                self.lighter_order_book_nonce = int(nonce) if nonce is not None else None
                                self.update_lighter_order_book("bids", order_book.get("bids", []))
                                self.update_lighter_order_book("asks", order_book.get("asks", []))
                                self.lighter_snapshot_loaded = True
                                self.lighter_order_book_ready = True
                                self.refresh_lighter_best_prices_locked()
                                self.lighter_book_received_monotonic = time.monotonic()
                        elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                            order_book = data.get("order_book", {})
                            if "offset" not in order_book:
                                continue
                            async with self.lighter_order_book_lock:
                                if not self.validate_order_book_update(order_book):
                                    self.lighter_order_book_sequence_gap = True
                                else:
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_order_book_offset = int(order_book["offset"])
                                    nonce = order_book.get("nonce")
                                    if nonce is not None:
                                        self.lighter_order_book_nonce = int(nonce)
                                    self.refresh_lighter_best_prices_locked()
                                    self.lighter_book_received_monotonic = time.monotonic()
                        if msg_type in {"subscribed/order_book", "update/order_book"}:
                            self.notify_market_signal()
                        if self.lighter_order_book_sequence_gap:
                            await self.reset_lighter_order_book()
                            raise RuntimeError("Lighter order book nonce gap; reconnecting for snapshot")
                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning("Lighter market websocket reconnect after error: %s", exc)
                await asyncio.sleep(1)

    async def handle_lighter_private_ws(self) -> None:
        while not self.stop_flag:
            try:
                self.lighter_private_stream_ready = False
                async with websockets.connect(
                    self.build_lighter_ws_url(),
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                    async with self._lighter_signer_lock:
                        if not self.lighter_client:
                            self.initialize_lighter_client()
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                            api_key_index=self.api_key_index
                        )
                    if err is not None:
                        raise RuntimeError(f"Failed to create Lighter WS auth token: {err}")
                    await ws.send(
                        json.dumps(
                            {"type": "subscribe", "channel": account_orders_channel, "auth": auth_token}
                        )
                    )
                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")
                        if msg_type in {"subscribed/account_orders", "update/account_orders"}:
                            self.lighter_private_stream_ready = True
                            orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                            for order in orders:
                                await self.handle_lighter_fill_update(order)
                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.lighter_private_stream_ready = False
                self.logger.warning("Lighter private websocket reconnect after error: %s", exc)
                await asyncio.sleep(1)

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            return self.lighter_best_bid, self.lighter_best_ask

    async def get_lighter_quote_age_ms(self) -> int | None:
        async with self.lighter_order_book_lock:
            return quote_age_ms(self.lighter_book_received_monotonic)

    async def get_variational_best_bid_ask(self, preferred_asset: str | None):
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
                if quote is None:
                    return None, None, None
            elif self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)

            if quote is None:
                return None, None, None
            return to_decimal(quote.get("bid")), to_decimal(quote.get("ask")), str(quote.get("asset", ""))

    async def get_variational_quote_age_ms(self, preferred_asset: str | None = None) -> int | None:
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
                if quote is None:
                    return None
            elif self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)
            if not isinstance(quote, dict):
                return None
            received = quote.get("received_monotonic")
            age = quote_age_ms(float(received)) if received is not None else None
            captured_at = parse_iso_datetime(quote.get("captured_at"))
            if captured_at is not None:
                source_age = max(0, int((datetime.now(timezone.utc) - captured_at).total_seconds() * 1000))
                age = max(age or 0, source_age)
            return age

    def _strategy_identity(self) -> tuple[str, int, Decimal, Decimal] | None:
        asset = (self.variational_ticker or "").strip().upper()
        config = self.strategy_config
        if not asset or self.market_generation <= 0:
            return None
        return (
            asset,
            self.market_generation,
            config.reference_notional_usd,
            config.order_notional_usd,
        )

    def _lighter_execution_from_book_locked(
        self,
        *,
        lighter_side: str,
        qty: Decimal,
    ) -> tuple[Decimal, Decimal] | None:
        """Read/calculate one VWAP under the book lock with nonce-bound caches."""

        base_multiplier = int(self.base_amount_multiplier or 0)
        price_multiplier = int(self.price_multiplier or 0)
        base_amount = int((qty * base_multiplier).to_integral_value(rounding=ROUND_DOWN))
        cache_key = (lighter_side.strip().upper(), base_amount)
        execution = self.lighter_vwap_cache.get(cache_key)
        if execution is None and base_amount > 0:
            tick_execution = self.lighter_execution_tick_cache.get(cache_key)
            if tick_execution is None and (
                self.lighter_order_book_ticks["bids"]
                or self.lighter_order_book_ticks["asks"]
            ):
                tick_execution = calculate_lighter_execution_tick_values(
                    self.lighter_order_book_ticks,
                    lighter_side,
                    base_amount,
                )
                if tick_execution is not None:
                    self.lighter_execution_tick_cache[cache_key] = tick_execution
            if tick_execution is not None and price_multiplier > 0:
                quote_ticks, marginal_price_i = tick_execution
                execution = (
                    Decimal(quote_ticks) / Decimal(base_amount * price_multiplier),
                    Decimal(marginal_price_i) / Decimal(price_multiplier),
                )
                self.lighter_vwap_cache[cache_key] = execution
        if execution is None:
            execution = calculate_lighter_execution(
                self.lighter_order_book,
                lighter_side,
                qty,
            )
        return execution

    async def current_adaptive_market_frame(
        self,
        *,
        exact_actual_side: StrategySide | None = None,
        exact_actual_base_qty: Decimal | None = None,
        allow_stale_for_display: bool = False,
    ) -> tuple[MarketFrame | None, dict[str, Any]]:
        """Build one synchronized reference/execution frame.

        The normal open/sampling path uses the configured target quantity.  A
        close decision supplies both ``exact_actual_*`` arguments so the
        executable Lighter VWAP and current notional are derived from the
        actual frozen BTC position instead of a fresh synthetic target order.
        ``allow_stale_for_display`` may return the latest calculable frame with
        ``observation.valid == False``; trading callers never enable it.
        """

        if (exact_actual_side is None) != (exact_actual_base_qty is None):
            raise ValueError("exact close side and quantity must be provided together")
        if exact_actual_base_qty is not None and exact_actual_base_qty <= 0:
            raise ValueError("exact close quantity must be positive")

        identity = self._strategy_identity()
        observation: dict[str, Any] = {
            "version": STRATEGY_MARKET_SAMPLE_VERSION,
            "valid": False,
            "rejection_reason": "strategy_identity_unavailable",
        }
        if identity is None:
            return None, observation
        asset, market_generation, reference_notional, order_notional = identity
        observation.update(
            {
                "asset": asset,
                "market_generation": market_generation,
                "reference_notional_usd": decimal_to_str(reference_notional),
                "order_notional_usd": decimal_to_str(order_notional),
            }
        )
        async with self.runtime.monitor._lock:
            quote = self.runtime.monitor.quotes.get(asset)
            if not isinstance(quote, dict):
                observation["rejection_reason"] = "var_quote_unavailable"
                return None, observation
            var_bid = to_decimal(quote.get("bid"))
            var_ask = to_decimal(quote.get("ask"))
            var_received = quote.get("received_monotonic")
            var_captured_at = parse_iso_datetime(quote.get("captured_at"))
        if (
            var_bid is None
            or var_ask is None
            or var_bid <= 0
            or var_ask <= 0
            or not isinstance(var_received, (int, float))
        ):
            observation["rejection_reason"] = "var_quote_invalid"
            return None, observation

        reference_buy_qty = reference_notional / var_ask
        reference_sell_qty = reference_notional / var_bid
        actual_buy_qty = (
            exact_actual_base_qty
            if exact_actual_side is StrategySide.BUY
            else order_notional / var_ask
        )
        actual_sell_qty = (
            exact_actual_base_qty
            if exact_actual_side is StrategySide.SELL
            else order_notional / var_bid
        )
        actual_notional = (
            exact_actual_base_qty
            * (var_ask if exact_actual_side is StrategySide.BUY else var_bid)
            if exact_actual_side is not None and exact_actual_base_qty is not None
            else order_notional
        )
        async with self.lighter_order_book_lock:
            lighter_received = self.lighter_book_received_monotonic
            if (
                not self.lighter_order_book_ready
                or self.lighter_order_book_sequence_gap
                or self.lighter_order_book_nonce is None
                or lighter_received is None
            ):
                observation["rejection_reason"] = "lighter_book_not_ready"
                return None, observation
            lighter_nonce = self.lighter_order_book_nonce
            reference_lighter_sell = self._lighter_execution_from_book_locked(
                lighter_side="SELL",
                qty=reference_buy_qty,
            )
            reference_lighter_buy = self._lighter_execution_from_book_locked(
                lighter_side="BUY",
                qty=reference_sell_qty,
            )
            actual_lighter_sell = self._lighter_execution_from_book_locked(
                lighter_side="SELL",
                qty=actual_buy_qty,
            )
            actual_lighter_buy = self._lighter_execution_from_book_locked(
                lighter_side="BUY",
                qty=actual_sell_qty,
            )
        executions = (
            reference_lighter_sell,
            reference_lighter_buy,
            actual_lighter_sell,
            actual_lighter_buy,
        )
        if any(execution is None for execution in executions):
            observation["rejection_reason"] = "lighter_depth_unavailable"
            return None, observation
        assert reference_lighter_sell is not None
        assert reference_lighter_buy is not None
        assert actual_lighter_sell is not None
        assert actual_lighter_buy is not None

        now_monotonic = time.monotonic()
        var_age_ms = max(0, int((now_monotonic - float(var_received)) * 1000))
        if var_captured_at is not None:
            source_age_ms = max(
                0,
                int(
                    (datetime.now(timezone.utc) - var_captured_at).total_seconds()
                    * 1000
                ),
            )
            var_age_ms = max(var_age_ms, source_age_ms)
        lighter_age_ms = max(0, int((now_monotonic - lighter_received) * 1000))
        source_skew_ms = abs(int((float(var_received) - lighter_received) * 1000))
        observation.update(
            {
                "var_bid": decimal_to_str(var_bid),
                "var_ask": decimal_to_str(var_ask),
                "var_captured_at": (
                    var_captured_at.isoformat() if var_captured_at is not None else None
                ),
                "var_age_ms": var_age_ms,
                "lighter_age_ms": lighter_age_ms,
                "source_skew_ms": source_skew_ms,
                "lighter_book_nonce": lighter_nonce,
                "reference_buy_var_qty": decimal_to_str(reference_buy_qty),
                "reference_sell_var_qty": decimal_to_str(reference_sell_qty),
                "actual_buy_var_qty": decimal_to_str(actual_buy_qty),
                "actual_sell_var_qty": decimal_to_str(actual_sell_qty),
                "reference_lighter_sell_vwap": decimal_to_str(reference_lighter_sell[0]),
                "reference_lighter_buy_vwap": decimal_to_str(reference_lighter_buy[0]),
                "actual_lighter_sell_vwap": decimal_to_str(actual_lighter_sell[0]),
                "actual_lighter_buy_vwap": decimal_to_str(actual_lighter_buy[0]),
            }
        )
        display_rejection_reason: str | None = None
        if not market_data_fresh(
            var_age_ms,
            lighter_age_ms,
            self.strategy_config.max_quote_age_ms,
        ):
            observation["rejection_reason"] = "market_data_stale"
            display_rejection_reason = "market_data_stale"
            if not allow_stale_for_display:
                return None, observation
        if identity != self._strategy_identity():
            observation["rejection_reason"] = "strategy_identity_changed"
            return None, observation
        reference_rates = DirectionalRates(
            buy=(reference_lighter_sell[0] - var_ask) / var_ask,
            sell=(var_bid - reference_lighter_buy[0]) / var_bid,
        )
        actual_rates = DirectionalRates(
            buy=(actual_lighter_sell[0] - var_ask) / var_ask,
            sell=(var_bid - actual_lighter_buy[0]) / var_bid,
        )
        captured_ms = time.time_ns() // 1_000_000
        var_source_ms = (
            int(var_captured_at.timestamp() * 1_000)
            if var_captured_at is not None
            else captured_ms - var_age_ms
        )
        frame = MarketFrame(
            asset=asset,
            captured_at_ms=captured_ms,
            variational_clock=SourceClock(
                source_timestamp_ms=max(0, var_source_ms),
                received_timestamp_ms=max(0, captured_ms - var_age_ms),
                age_ms=var_age_ms,
            ),
            lighter_clock=SourceClock(
                source_timestamp_ms=max(0, captured_ms - lighter_age_ms),
                received_timestamp_ms=max(0, captured_ms - lighter_age_ms),
                age_ms=lighter_age_ms,
            ),
            source_skew_ms=source_skew_ms,
            var_bid=var_bid,
            var_ask=var_ask,
            lighter_reference_buy_vwap=reference_lighter_buy[0],
            lighter_reference_sell_vwap=reference_lighter_sell[0],
            lighter_actual_buy_vwap=actual_lighter_buy[0],
            lighter_actual_sell_vwap=actual_lighter_sell[0],
            reference_notional_usd=reference_notional,
            actual_notional_usd=actual_notional,
            reference_rates=reference_rates,
            actual_rates=actual_rates,
        )
        observation.update(
            {
                "valid": display_rejection_reason is None,
                "rejection_reason": display_rejection_reason,
                "reference_buy_rate": decimal_to_str(reference_rates.buy),
                "reference_sell_rate": decimal_to_str(reference_rates.sell),
                "actual_buy_rate": decimal_to_str(actual_rates.buy),
                "actual_sell_rate": decimal_to_str(actual_rates.sell),
            }
        )
        return frame, observation

    def _invalidate_adaptive_parameters(self, reason: str) -> None:
        """Fail new opens closed and discard every unconfirmed proposal."""

        had_state = (
            self.active_parameter_epoch is not None
            or self.strategy_epoch_activator.active is not None
        )
        self.active_parameter_epoch = None
        self.strategy_epoch_activator = EpochActivator(
            model=self.strategy_model,
            confirmations=self.strategy_config.parameter_confirmations,
        )
        self._strategy_parameter_block_reason = reason
        if had_state:
            self.trace_event("adaptive_parameters_invalidated", None, reason=reason)

    async def _refresh_adaptive_market_frame(
        self,
        *,
        record_sample: bool,
        sample_ms: int | None = None,
    ) -> bool:
        """Build the newest frame; optionally append one statistics sample.

        Frame construction is serialized between the event-driven decision
        adapter and the one-second statistics sampler.  Neither path performs
        network or disk waits; telemetry emission is queue-only.
        """

        requested_ms = time.time_ns() // 1_000_000 if sample_ms is None else sample_ms
        display_stats_input = None
        sample_generation = self.market_generation
        async with self._strategy_frame_build_lock:
            try:
                frame, observation = await self.current_adaptive_market_frame()
            except Exception:
                self.last_market_frame = None
                raise

            recorded_ms = frame.captured_at_ms if frame is not None and sample_ms is None else requested_ms
            if frame is None:
                self.last_market_frame = None
                if record_sample and self.strategy_market_sample_writer is not None:
                    self.strategy_market_sample_writer.emit(
                        {
                            **observation,
                            "sample_timestamp_ms": recorded_ms,
                            "sampled_at": utc_now(),
                            "runtime_build": RUNTIME_BUILD,
                        }
                    )
                return False

            previous_frame_ms = self._last_valid_strategy_frame_ms
            if previous_frame_ms is not None:
                if frame.captured_at_ms < previous_frame_ms:
                    self._invalidate_adaptive_parameters("strategy_frame_clock_regression")
                elif frame.captured_at_ms - previous_frame_ms >= STRATEGY_MAX_SAMPLE_GAP_MS:
                    self._invalidate_adaptive_parameters("strategy_market_frame_gap")
            self._last_valid_strategy_frame_ms = frame.captured_at_ms
            self.last_market_frame = frame

            if (
                record_sample
                and self._last_recorded_strategy_sample_ms is not None
                and 0
                <= recorded_ms - self._last_recorded_strategy_sample_ms
                < STRATEGY_MIN_SAMPLE_INTERVAL_MS
            ):
                # A Var event can arrive while the fallback sample is being
                # processed.  Keep the newest decision frame, but never count
                # that burst twice in the statistical windows.
                return True

            bridge_history_gap = False
            if record_sample and self._strategy_history_resume_pending:
                historical_ms = self._last_recorded_strategy_sample_ms
                resume_gap_ms = (
                    recorded_ms - historical_ms
                    if historical_ms is not None
                    else STRATEGY_HISTORY_RESUME_MAX_GAP_MS + 1
                )
                if resume_gap_ms > STRATEGY_HISTORY_RESUME_MAX_GAP_MS:
                    # The historical rates remain on disk for research, but a
                    # stale restart must rebuild all live windows from zero.
                    self.strategy_window_store = RollingWindowStore()
                    self._opportunity_samples = {
                        StrategySide.BUY: deque(),
                        StrategySide.SELL: deque(),
                    }
                    self._last_recorded_strategy_sample_ms = None
                    self._strategy_history_resume_pending = False
                    self._strategy_history_resume_state = "rejected_gap_over_5m"
                    self._strategy_history_resume_gap_ms = max(0, resume_gap_ms)
                    self._strategy_history_resume_samples = 0
                    self._strategy_history_resume_coverage_ms = 0
                    self._invalidate_adaptive_parameters(
                        "strategy_history_resume_gap_over_5m"
                    )
                elif resume_gap_ms >= 0:
                    bridge_history_gap = resume_gap_ms >= STRATEGY_MAX_SAMPLE_GAP_MS
                    self._strategy_history_resume_pending = False
                    self._strategy_history_resume_state = "resumed"
                    self._strategy_history_resume_gap_ms = resume_gap_ms
                    self.trace_event(
                        "adaptive_history_resumed",
                        None,
                        samples=self._strategy_history_resume_samples,
                        coverage_ms=self._strategy_history_resume_coverage_ms,
                        restart_gap_ms=resume_gap_ms,
                    )

            sample_clock_regression = bool(
                record_sample
                and self._last_recorded_strategy_sample_ms is not None
                and recorded_ms < self._last_recorded_strategy_sample_ms
            )
            if sample_clock_regression:
                self._invalidate_adaptive_parameters("strategy_sample_clock_regression")
                observation = {
                    **observation,
                    "valid": False,
                    "rejection_reason": "sample_clock_regression",
                }
            if record_sample and self.strategy_market_sample_writer is not None:
                self.strategy_market_sample_writer.emit(
                    {
                        **observation,
                        "sample_timestamp_ms": recorded_ms,
                        "sampled_at": utc_now(),
                        "runtime_build": RUNTIME_BUILD,
                    }
                )
            if not record_sample:
                return True
            if sample_clock_regression:
                return False

            previous_sample_ms = self._last_recorded_strategy_sample_ms
            if (
                previous_sample_ms is not None
                and recorded_ms - previous_sample_ms >= STRATEGY_MAX_SAMPLE_GAP_MS
                and not bridge_history_gap
            ):
                self._invalidate_adaptive_parameters("strategy_sample_gap")
            self._last_recorded_strategy_sample_ms = recorded_ms
            self.strategy_window_store.add(
                timestamp_ms=recorded_ms,
                rates=frame.reference_rates,
                bridges_previous=bridge_history_gap,
            )
            display_stats_input = self.strategy_window_store.frozen_copy()
            cutoff = recorded_ms - STRATEGY_STATISTICS_WINDOW_MS
            for side in StrategySide:
                samples = self._opportunity_samples[side]
                samples.append(
                    OpportunitySample(
                        recorded_ms,
                        frame.reference_rates.for_side(side),
                    )
                )
                while samples and samples[0].timestamp_ms < cutoff:
                    samples.popleft()
            if (
                recorded_ms - self._last_parameter_refresh_ms
                >= self.strategy_config.parameter_refresh_seconds * 1_000
            ):
                self._last_parameter_refresh_ms = recorded_ms
                refresh_parameter_ms: int | None = recorded_ms
            else:
                refresh_parameter_ms = None
        # Sorting the full one-hour window is a cold-path task.  Compile
        # from an immutable copy in a worker, after releasing the frame lock.
        if refresh_parameter_ms is not None:
            await self._refresh_parameter_epoch(refresh_parameter_ms)
        elif display_stats_input is not None:
            # Formal epochs compile once per configured interval. The panel's
            # medians, however, follow every valid one-second sample so a live
            # market never looks frozen between parameter compilations.
            display_stats = await asyncio.to_thread(
                display_stats_input.snapshot,
                now_ms=recorded_ms,
            )
            if (
                self.market_generation == sample_generation
                and self._last_recorded_strategy_sample_ms == recorded_ms
            ):
                self.strategy_window_stats = {
                    side: dict(windows)
                    for side, windows in display_stats.items()
                }
        return True

    async def capture_strategy_sample_once(
        self,
        *,
        now_ms: int | None = None,
    ) -> bool:
        """Append one frame sample and refresh parameters on the cold-path cadence."""

        if not self.strategy_config.sampling_enabled:
            self._invalidate_adaptive_parameters("strategy_sampling_disabled")
            return False
        return await self._refresh_adaptive_market_frame(
            record_sample=True,
            sample_ms=now_ms,
        )

    async def refresh_adaptive_market_frame_for_decision(self) -> bool:
        """Refresh the event-driven decision frame without changing windows."""

        return await self._refresh_adaptive_market_frame(record_sample=False)

    async def _refresh_parameter_epoch(self, now_ms: int) -> None:
        frozen_store = self.strategy_window_store.frozen_copy()
        opportunity_samples = {
            side: tuple(self._opportunity_samples[side]) for side in StrategySide
        }
        model = self.strategy_model
        config_hash = self.strategy_config_hash
        config = self.strategy_config
        reference_notional_usd = config.reference_notional_usd
        order_notional_usd = config.order_notional_usd
        reserve_bps_per_leg = config.provisional_reserve_bps_per_leg
        max_normal_round_wear_bps = config.max_normal_round_wear_bps
        def compile_candidate() -> tuple[
            Any,
            ParameterEpoch | None,
            str | None,
        ]:
            stats = frozen_store.snapshot(now_ms=now_ms)
            parameter_windows = (5, 30, 60)
            if not all(
                stats[side][minutes].ready
                for side in StrategySide
                for minutes in parameter_windows
            ):
                reasons = sorted(
                    {
                        stats[side][minutes].reason
                        for side in StrategySide
                        for minutes in parameter_windows
                        if not stats[side][minutes].ready
                    }
                )
                return (
                    stats,
                    None,
                    "strategy_windows_not_ready:"
                    + (",".join(reasons) if reasons else "unknown"),
                )
            preliminary = build_parameter_candidate(
                now_ms=now_ms,
                model=model,
                config_hash=config_hash,
                stats=stats,
                reference_notional_usd=reference_notional_usd,
                order_notional_usd=order_notional_usd,
                reserve_bps_per_leg=reserve_bps_per_leg,
                max_normal_round_wear_bps=max_normal_round_wear_bps,
            )
            raw = {
                side: preliminary.component(side).final for side in StrategySide
            }
            balance = {
                side: opportunity_balance_threshold(
                    own_samples=opportunity_samples[side],
                    other_samples=opportunity_samples[side.opposite],
                    raw_threshold=raw[side],
                    other_raw_threshold=raw[side.opposite],
                    model=model,
                )
                for side in StrategySide
            }
            proposal = build_parameter_candidate(
                now_ms=now_ms,
                model=model,
                config_hash=config_hash,
                stats=stats,
                reference_notional_usd=reference_notional_usd,
                order_notional_usd=order_notional_usd,
                reserve_bps_per_leg=reserve_bps_per_leg,
                max_normal_round_wear_bps=max_normal_round_wear_bps,
                balance_thresholds=balance,
            )
            return stats, proposal, None

        stats, proposal, block_reason = await asyncio.to_thread(compile_candidate)
        if config_hash != self.strategy_config_hash:
            self._invalidate_adaptive_parameters(
                "strategy_config_changed_during_parameter_compile"
            )
            return
        self.strategy_window_stats = {
            side: dict(windows) for side, windows in stats.items()
        }
        if proposal is None:
            self._invalidate_adaptive_parameters(block_reason or "strategy_windows_not_ready")
            return
        previous_active_epoch_id = (
            self.active_parameter_epoch.epoch_id
            if self.active_parameter_epoch is not None
            else None
        )
        self.active_parameter_epoch = self.strategy_epoch_activator.offer(
            proposal,
            now_ms=now_ms,
        )
        active_epoch = self.active_parameter_epoch
        self._strategy_parameter_block_reason = (
            None
            if self.active_parameter_epoch is not None
            else "parameter_epoch_pending_confirmation"
        )
        self.trace_event(
            "adaptive_parameter_candidate",
            None,
            proposal_epoch_id=proposal.epoch_id,
            active_epoch_id=(
                active_epoch.epoch_id if active_epoch is not None else None
            ),
            activated=(
                active_epoch is not None
                and active_epoch.epoch_id != previous_active_epoch_id
            ),
            parameter_window_source=proposal.window_source,
            proposal_buy_threshold=proposal.thresholds.buy.final,
            proposal_sell_threshold=proposal.thresholds.sell.final,
            active_buy_threshold=(
                active_epoch.thresholds.buy.final if active_epoch is not None else None
            ),
            active_sell_threshold=(
                active_epoch.thresholds.sell.final if active_epoch is not None else None
            ),
            refresh_interval_ms=self.strategy_config.parameter_refresh_seconds * 1_000,
        )

    async def strategy_sample_loop(self) -> None:
        """Sample on fresh Var quotes, with a one-second health fallback.

        Sampling remains isolated from the decision task, so rolling-window
        maintenance and its worker-thread statistics never lengthen the order
        signal path.
        """

        while not self.stop_flag:
            self._strategy_sample_event.clear()
            started = time.monotonic()
            try:
                await self.capture_strategy_sample_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_market_frame = None
                self._invalidate_adaptive_parameters("strategy_sampler_exception")
                self.logger.warning("Adaptive strategy sampler skipped: %s", exc)
            if self.stop_flag:
                break
            elapsed = time.monotonic() - started
            timeout = max(0, STRATEGY_SAMPLE_SECONDS - elapsed)
            try:
                await asyncio.wait_for(
                    self._strategy_sample_event.wait(),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                pass

    def evaluate_adaptive_open(self) -> StrategyDecision:
        now_ms = time.time_ns() // 1_000_000
        frame = self.last_market_frame
        if self._strategy_parameter_block_reason is not None:
            decision = StrategyDecision(
                StrategyAction.PAUSE,
                self._strategy_parameter_block_reason,
            )
        elif frame is None:
            decision = StrategyDecision(
                StrategyAction.PAUSE,
                "market_frame_unavailable",
            )
        else:
            decision = self.strategy_engine.evaluate_open(
                frame=frame,
                epoch=self.active_parameter_epoch,
                now_ms=now_ms,
            )
        self.last_strategy_decision = decision
        self.last_strategy_decision_at_ms = now_ms
        self._selected_open_candidate = decision.open_candidate
        epoch_id = (
            decision.open_candidate.epoch.epoch_id
            if decision.open_candidate is not None
            else None
        )
        direction = (
            decision.open_candidate.direction.value
            if decision.open_candidate is not None
            else None
        )
        trace_signature = (
            self.strategy_config.execution_mode,
            decision.action.value,
            decision.reason,
            epoch_id,
            direction,
        )
        # Market events can arrive tens of times per second.  Keep immediate
        # state transitions plus a low-rate heartbeat, without enqueueing identical
        # disk rows on every hot-path evaluation.
        if (
            trace_signature != self._last_open_decision_trace_signature
            or now_ms - self._last_open_decision_trace_ms
            >= OPEN_DECISION_TRACE_HEARTBEAT_MS
        ):
            self._last_open_decision_trace_signature = trace_signature
            self._last_open_decision_trace_ms = now_ms
            self.trace_event(
                "adaptive_open_decision",
                None,
                mode=self.strategy_config.execution_mode,
                action=decision.action.value,
                reason=decision.reason,
                epoch_id=epoch_id,
                direction=direction,
            )
        return decision

    def recent_directional_rate_range(
        self,
        side: StrategySide,
        *,
        now_ms: int,
        current_rate: Decimal | None = None,
    ) -> Decimal | None:
        """Return a fully covered five-second pre-trade rate range.

        The one-hour opportunity deque is maintained by the isolated sampler.
        A missing or gapped five-second window fails closed for a new entry;
        close callers may apply their bounded safety deferral instead.
        """

        cutoff_ms = now_ms - V5_RATE_RANGE_WINDOW_MS
        recent: list[OpportunitySample] = []
        for sample in reversed(self._opportunity_samples[side]):
            if sample.timestamp_ms > now_ms:
                continue
            if sample.timestamp_ms < cutoff_ms:
                break
            recent.append(sample)
        recent.reverse()
        if (
            len(recent) < 2
            or recent[0].timestamp_ms
            > cutoff_ms + int(STRATEGY_SAMPLE_SECONDS * 1_500)
            or recent[-1].timestamp_ms
            < now_ms - int(STRATEGY_SAMPLE_SECONDS * 1_500)
        ):
            return None
        values = [sample.rate for sample in recent]
        if current_rate is not None:
            values.append(current_rate)
        return max(values) - min(values)

    @staticmethod
    def trade_key(event: dict[str, Any]) -> str:
        trade_id = str(event.get("trade_id", "")).strip()
        if trade_id:
            return f"id:{trade_id}"
        event_seq = str(event.get("event_seq", "")).strip()
        return f"seq:{event_seq}"

    async def append_order_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.order_log_writer is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            "logged_monotonic_ns": time.monotonic_ns(),
            **payload,
        }
        self.order_log_writer.emit(row)

    def trace_event(self, event: str, trace_id: str | None, **fields: Any) -> None:
        """Record execution timing without awaiting disk I/O or exposing credentials."""
        if self.trace_writer is None:
            return
        row = {
            "event": event,
            "trace_id": trace_id,
            "utc_time": utc_now(),
            "monotonic_ns": time.monotonic_ns(),
            "runtime_build": RUNTIME_BUILD,
            **{key: trace_value(value) for key, value in fields.items()},
        }
        self.trace_writer.emit(row)

    async def fail_lighter_hedge(self, record: OrderLifecycle, message: str) -> bool:
        async with self._record_lock:
            filled_qty = record.lighter_filled_qty or Decimal("0")
            if (
                record.hedge_status in {"filled", "overfilled"}
                or lighter_order_target_matches(
                    record,
                    filled_qty,
                    self.base_amount_multiplier,
                )
            ):
                if record.hedge_status != "overfilled":
                    record.hedge_status = "filled"
                    record.hedge_error = None
                return False
            record.hedge_status = "error"
            record.execution_state = EXECUTION_STATE_HEDGE_ERROR
            record.hedge_error = message
            payload = record.to_payload()
        await self.append_order_log("lighter_error", payload)
        self.trace_event(
            "lighter_order_error",
            record.trace_id,
            trade_key=record.trade_key,
            error=message,
        )
        self.pause_for_reconciliation(f"Lighter hedge failed: {message}")
        await self.persist_runtime_state()
        if not record.lighter_reduce_only:
            await self.emergency_flatten_var(record)
        return True

    async def place_lighter_order(self, record: OrderLifecycle) -> None:
        if not self.args.auto_hedge:
            async with self._record_lock:
                record.hedge_status = "disabled"
            return

        side = "SELL" if record.side == "buy" else "BUY"
        generation = self.market_generation
        market_index = self.lighter_market_index
        base_multiplier = self.base_amount_multiplier
        lighter_target = lighter_order_target_qty(record, base_multiplier)
        if lighter_target is None:
            await self.fail_lighter_hedge(
                record,
                f"Invalid Lighter base amount multiplier/quantity ({base_multiplier}/{record.qty})",
            )
            return
        async with self._record_lock:
            if record.trace_id is None:
                record.trace_id = new_trace_id()
            reduce_only = record.lighter_reduce_only
            already_filled = record.lighter_filled_qty or Decimal("0")
            if record.hedge_status == "overfilled":
                return
            if already_filled == lighter_target:
                record.hedge_status = "filled"
                record.execution_state = EXECUTION_STATE_HEDGED
                record.hedge_error = None
                return
            record.lighter_side = side
            record.hedge_status = "submitting"
            record.execution_state = EXECUTION_STATE_HEDGE_SUBMITTING
            record.hedge_error = None
            trace_id = record.trace_id

        is_ask = side == "SELL"
        submission_error: str | None = None
        client_order_id: int | None = None
        base_amount = 0
        price_i = 0
        prepared_primary_order = False
        async with self._record_lock:
            already_filled = record.lighter_filled_qty or Decimal("0")
            if record.hedge_status == "overfilled":
                return
            if already_filled == lighter_target:
                record.hedge_status = "filled"
                record.execution_state = EXECUTION_STATE_HEDGED
                record.hedge_error = None
                return
            if generation != self.market_generation or market_index != self.lighter_market_index:
                submission_error = "Market changed before Lighter hedge submission"
            else:
                remaining_qty = max(Decimal("0"), lighter_target - already_filled)
                base_amount = int(remaining_qty * int(base_multiplier))
                if base_amount <= 0:
                    submission_error = f"Hedge base amount rounds to zero ({record.qty})"
                else:
                    attempt = len(record.lighter_client_order_ids)
                    if attempt == 0 and record.lighter_reserved_client_order_id is not None:
                        client_order_id = record.lighter_reserved_client_order_id
                        current_intent = self.pending_var_intent
                        occupied = self._occupied_lighter_client_order_ids_locked(
                            exclude_trade_key=record.trade_key,
                            exclude_intent=(
                                current_intent
                                if current_intent is not None
                                and current_intent.lighter_client_order_index == client_order_id
                                and current_intent.firm_quote_id == record.firm_quote_id
                                else None
                            ),
                        )
                        if client_order_id in occupied:
                            submission_error = (
                                "Reserved deterministic Lighter client order index is already in use"
                            )
                        elif (
                            current_intent is not None
                            and current_intent.state
                            in {
                                VAR_INTENT_PREPARED,
                                VAR_INTENT_COMMITTING,
                                VAR_INTENT_COMMIT_AMBIGUOUS,
                                VAR_INTENT_COMMITTED,
                            }
                            and current_intent.lighter_client_order_index == client_order_id
                            and current_intent.firm_quote_id == record.firm_quote_id
                        ):
                            prepared_primary_order = True
                    else:
                        identity = record.firm_quote_id or record.trace_id or record.trade_key
                        client_order_id, _collision = self._reserve_lighter_client_order_index_locked(
                            firm_quote_id=identity,
                            phase=record.strategy_phase or "recovery",
                            side=record.side,
                            attempt=attempt,
                            exclude_trade_key=record.trade_key,
                        )
                    if submission_error is not None:
                        client_order_id = None
                    else:
                        assert client_order_id is not None
                        record.lighter_side = side
                        record.lighter_client_order_id = client_order_id
                        record.lighter_client_order_ids.append(client_order_id)
                        record.lighter_outcome_final = False
                        record.lighter_submitted_at_iso = utc_now()
                        record.hedge_status = "submitting"
                        self.lighter_order_terminal_ids.discard(client_order_id)
                        self.lighter_retry_pending_keys.discard(record.trade_key)
                        self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
        if submission_error is not None:
            await self.fail_lighter_hedge(record, submission_error)
            return
        if client_order_id is None:
            return

        try:
            dispatch_snapshot: LighterHedgeDispatchSnapshot | None = None
            revalidation_error: str | None = None
            async with self._record_lock:
                filled_qty = record.lighter_filled_qty or Decimal("0")
                latest_remaining_qty = max(Decimal("0"), lighter_target - filled_qty)
                latest_base_amount = int(latest_remaining_qty * int(base_multiplier))
                submission_superseded = (
                    record.hedge_status in {"filled", "overfilled"}
                    or filled_qty == lighter_target
                    or latest_base_amount <= 0
                )
                if submission_superseded:
                    if client_order_id in record.lighter_client_order_ids:
                        record.lighter_client_order_ids.remove(client_order_id)
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
                    self.lighter_order_fill_totals.pop(client_order_id, None)
                    self.lighter_order_terminal_ids.discard(client_order_id)
                    record.lighter_client_order_id = (
                        record.lighter_client_order_ids[-1]
                        if record.lighter_client_order_ids
                        else None
                    )
                    if record.hedge_status != "overfilled":
                        record.hedge_status = "filled"
                        record.hedge_error = None
                else:
                    base_amount = latest_base_amount
                    # Fill handling owns _record_lock.  Holding it while the
                    # in-memory book snapshot is captured binds the residual
                    # amount and depth nonce to one pre-send observation.
                    dispatch_snapshot, revalidation_error = await self.capture_lighter_hedge_dispatch_snapshot(
                        record=record,
                        lighter_side=side,
                        base_amount=base_amount,
                        market_generation=generation,
                        market_index=market_index,
                    )
            if submission_superseded:
                tx_response = None
                error = None
            else:
                if revalidation_error is not None:
                    await self.fail_lighter_hedge(record, revalidation_error)
                    return
                assert dispatch_snapshot is not None
                price_i = dispatch_snapshot.price_i
                self.trace_event(
                    "lighter_order_prepared",
                    trace_id,
                    trade_key=record.trade_key,
                    client_order_id=client_order_id,
                    side=side,
                    base_amount=base_amount,
                    price=price_i,
                    reduce_only=reduce_only,
                    order_book_nonce=dispatch_snapshot.order_book_nonce,
                    quote_age_ms=dispatch_snapshot.quote_age_ms,
                )
                if prepared_primary_order:
                    self.trace_event(
                        "lighter_order_uses_prepared_intent",
                        trace_id,
                        trade_key=record.trade_key,
                        client_order_id=client_order_id,
                    )
                else:
                    await self.persist_runtime_state()
                if (
                    self.market_generation != dispatch_snapshot.market_generation
                    or self.lighter_market_index != dispatch_snapshot.market_index
                ):
                    await self.fail_lighter_hedge(
                        record, "Market changed after Lighter hedge snapshot"
                    )
                    return
                self.trace_event(
                    "lighter_sign_and_send_start",
                    trace_id,
                    trade_key=record.trade_key,
                    client_order_id=client_order_id,
                    transport=("websocket" if self.lighter_order_entry_enabled else "rest"),
                )
                tx_response, error = await self.submit_lighter_create_order(
                    market_index=market_index,
                    client_order_id=client_order_id,
                    base_amount=base_amount,
                    price=price_i,
                    is_ask=is_ask,
                    reduce_only=reduce_only,
                    trace_id=trace_id,
                )

            if submission_superseded:
                await self.persist_runtime_state()
                return

            if error is not None:
                self.trace_event(
                    "lighter_sign_and_send_result",
                    trace_id,
                    trade_key=record.trade_key,
                    client_order_id=client_order_id,
                    ok=False,
                    error=str(error),
                )
                raise RuntimeError(f"Sign error: {error}")
            response_code = getattr(tx_response, "code", 0)
            self.trace_event(
                "lighter_sign_and_send_result",
                trace_id,
                trade_key=record.trade_key,
                client_order_id=client_order_id,
                ok=response_code in {0, 200, "0", "200", None},
                response_code=response_code,
            )
            if response_code not in {0, 200, "0", "200", None}:
                raise RuntimeError(
                    f"Lighter sendTx rejected: code={response_code} message={getattr(tx_response, 'message', None)}"
                )

            async with self._record_lock:
                record.lighter_side = side
                record.lighter_tx_hash = getattr(tx_response, "tx_hash", None)
                record.lighter_submitted_at_iso = utc_now()
                if record.hedge_status == "submitting":
                    record.hedge_status = "submitted"
                    record.execution_state = EXECUTION_STATE_HEDGE_SUBMITTED
                    record.hedge_error = None
                payload = record.to_payload()
            await self.append_order_log("lighter_submitted", payload)
            self.trace_event(
                "lighter_order_ack",
                trace_id,
                trade_key=record.trade_key,
                client_order_id=client_order_id,
                tx_hash=record.lighter_tx_hash,
            )
            await self.persist_runtime_state()
        except Exception as exc:
            error_message = str(exc)
            self.trace_event(
                "lighter_sign_and_send_error",
                trace_id,
                trade_key=record.trade_key,
                client_order_id=client_order_id,
                error=error_message,
            )
            async with self._record_lock:
                response_was_superseded = record.hedge_status not in {
                    "submitting",
                    "submitted",
                }
            if response_was_superseded:
                return
            if lighter_error_is_definitive(error_message):
                await self.fail_lighter_hedge(record, error_message)
                return
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_status = "uncertain"
                record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                record.hedge_error = (
                    f"{error_message}; waiting {LIGHTER_ERROR_CONFIRM_SECONDS:.0f}s "
                    "for possible Lighter fill"
                )
                payload = record.to_payload()
            await self.append_order_log("lighter_uncertain", payload)
            await self.persist_runtime_state()
            await asyncio.sleep(LIGHTER_ERROR_CONFIRM_SECONDS)
            reconciliation = await self.reconcile_lighter_client_order(client_order_id)
            async with self._record_lock:
                if (
                    record.hedge_status
                    in {"filled", "queued", "submitting", "submitted", "retrying"}
                    or (
                        record.hedge_status == "partial"
                        and not record.lighter_outcome_final
                    )
                ):
                    return
                if reconciliation is LighterOrderReconcileOutcome.UNKNOWN:
                    record.hedge_status = "uncertain"
                    record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                    record.hedge_error = (
                        f"{error_message}; Lighter order outcome remains unknown after reconciliation"
                    )
                    payload = record.to_payload()
                    attempts_left = False
                else:
                    attempts_left = (
                        len(record.lighter_client_order_ids)
                        < self.strategy_config.lighter_hedge_max_attempts
                    )
                    record.hedge_status = "retrying" if attempts_left else "error"
                    record.hedge_error = error_message
                    payload = record.to_payload()
            if reconciliation is LighterOrderReconcileOutcome.UNKNOWN:
                await self.append_order_log("lighter_reconciliation_unknown", payload)
                self.pause_automation(record.hedge_error or "Lighter order outcome unknown")
                await self.persist_runtime_state()
                return
            if attempts_left:
                await self.append_order_log("lighter_retry", payload)
                self.queue_lighter_retry_after_current(record)
                return
            await self.fail_lighter_hedge(record, error_message)

    def schedule_lighter_order(self, record: OrderLifecycle) -> bool:
        existing = self.lighter_order_tasks_by_trade_key.get(record.trade_key)
        if existing is not None and not existing.done():
            return False
        record.hedge_status = "queued"
        record.hedge_error = None
        task = asyncio.create_task(self._run_lighter_order_task(record))
        self.lighter_order_tasks_by_trade_key[record.trade_key] = task
        self.hedge_tasks.add(task)
        task.add_done_callback(
            lambda completed, trade_key=record.trade_key: self._lighter_order_task_done(
                trade_key,
                completed,
            )
        )
        return True

    def queue_lighter_retry_after_current(self, record: OrderLifecycle) -> bool:
        self.lighter_retry_pending_keys.add(record.trade_key)
        existing = self.lighter_order_tasks_by_trade_key.get(record.trade_key)
        if existing is not None and not existing.done():
            self.lighter_requeue_after_task_keys.add(record.trade_key)
            return False
        return self.schedule_lighter_order(record)

    def _lighter_order_task_done(
        self,
        trade_key: str,
        task: asyncio.Task[None],
    ) -> None:
        self.hedge_tasks.discard(task)
        if self.lighter_order_tasks_by_trade_key.get(trade_key) is task:
            self.lighter_order_tasks_by_trade_key.pop(trade_key, None)

    async def _run_lighter_order_task(self, record: OrderLifecycle) -> None:
        try:
            while True:
                self.lighter_requeue_after_task_keys.discard(record.trade_key)
                await self.place_lighter_order(record)
                if record.trade_key not in self.lighter_requeue_after_task_keys:
                    break
                async with self._record_lock:
                    self.lighter_requeue_after_task_keys.discard(record.trade_key)
                    filled_qty = record.lighter_filled_qty or Decimal("0")
                    lighter_target = lighter_order_target_qty(
                        record,
                        self.base_amount_multiplier,
                    )
                    retry_needed = (
                        record.hedge_status != "overfilled"
                        and lighter_target is not None
                        and filled_qty < lighter_target
                    )
                    if retry_needed:
                        record.hedge_status = "retrying"
                        record.hedge_error = None
                    else:
                        self.lighter_retry_pending_keys.discard(record.trade_key)
                if not retry_needed:
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with self._record_lock:
                record.hedge_status = "error"
                record.hedge_error = str(exc)
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            self.pause_for_reconciliation(f"Lighter hedge failed: task error: {exc}")
            self.logger.exception("Unhandled Lighter hedge task error: %s", exc)
        finally:
            self.lighter_retry_pending_keys.discard(record.trade_key)
            self.lighter_requeue_after_task_keys.discard(record.trade_key)

    async def _current_open_record(
        self,
        *,
        exclude_trade_key: str | None = None,
    ) -> OrderLifecycle | None:
        async with self._record_lock:
            ordered_keys = list(self.record_order)
            ordered_records = [
                self.records[key]
                for key in ordered_keys
                if key in self.records and key != exclude_trade_key
            ]
        current_open, _ = build_trade_rounds(ordered_records)
        return current_open

    async def _auto_var_signal_for_current_open(
        self,
        current_open: OrderLifecycle | None,
    ) -> tuple[str, Decimal] | None:
        self._selected_open_candidate = None
        if current_open is not None:
            return None
        decision = self.evaluate_adaptive_open()
        candidate = decision.open_candidate
        if candidate is None:
            self.last_auto_var_order_status = f"{decision.action.value}: {decision.reason}"
            return None
        if candidate.epoch.model_version == "adaptive-median-v5":
            now_ms = time.time_ns() // 1_000_000
            rate_range = self.recent_directional_rate_range(
                candidate.direction,
                now_ms=now_ms,
                current_rate=candidate.reference_rate,
            )
            if rate_range is None:
                self._selected_open_candidate = None
                self.last_auto_var_order_status = (
                    "NO_ACTION: opening five-second rate range is not ready"
                )
                return None
            maximum_range = V5_OPEN_RATE_RANGE_BPS / Decimal("10000")
            if rate_range > maximum_range:
                self._selected_open_candidate = None
                self.last_auto_var_order_status = (
                    "NO_ACTION: opening five-second rate range "
                    f"{rate_range * Decimal('10000'):.2f}bps exceeds "
                    f"{V5_OPEN_RATE_RANGE_BPS}bps"
                )
                return None
        headroom_bps = self.effective_open_execution_headroom_bps(
            candidate.direction.value,
            candidate.order_notional_usd,
        )
        execution_threshold = (
            candidate.threshold + headroom_bps / Decimal("10000")
        )
        if (
            candidate.reference_rate < execution_threshold
            or candidate.actual_rate < execution_threshold
        ):
            # evaluate_adaptive_open() publishes its raw strategy candidate for
            # the dashboard.  Once the execution-layer headroom rejects it,
            # callers must not observe that object as the selected executable
            # candidate for this decision cycle.
            self._selected_open_candidate = None
            self.last_auto_var_order_status = (
                "NO_ACTION: opening rate has not covered execution headroom "
                f"({headroom_bps:.2f}bps)"
            )
            return None
        self._selected_open_candidate = candidate
        if self.strategy_config.execution_mode == "observe":
            self.last_auto_var_order_status = (
                f"observe candidate {candidate.direction.value}; no order"
            )
            return None
        return candidate.direction.value, candidate.actual_open_pnl_usd

    async def get_fresh_lighter_vwap(
        self,
        *,
        var_side: str,
        qty: Decimal,
    ) -> tuple[Decimal | None, int | None]:
        lighter_side = "SELL" if var_side.strip().upper() == "BUY" else "BUY"
        vwap, _marginal, _nonce, age_ms = await self.get_lighter_execution_snapshot(
            lighter_side=lighter_side,
            qty=qty,
        )
        if age_ms is None:
            return None, None
        if age_ms > self.strategy_config.max_quote_age_ms:
            return None, age_ms
        return vwap, age_ms

    async def get_fresh_lighter_open_vwaps(
        self,
        *,
        var_side: str,
        firm_price: Decimal,
        firm_qty: Decimal,
        reference_notional_usd: Decimal,
    ) -> tuple[Decimal | None, Decimal | None, int | None]:
        """Read actual and reference opening VWAPs from one book snapshot.

        The preliminary signal requires both target-size and reference-size
        depth to clear the frozen gate.  Repeating that same dual check after
        Firm Quote must use one Lighter nonce; otherwise a fast book change can
        combine two different states or silently retain the pre-quote 500U
        reference rate.
        """

        if (
            firm_price <= 0
            or firm_qty <= 0
            or reference_notional_usd <= 0
        ):
            return None, None, None
        lighter_side = "SELL" if var_side.strip().upper() == "BUY" else "BUY"
        reference_qty = reference_notional_usd / firm_price
        async with self.lighter_order_book_lock:
            received_at = self.lighter_book_received_monotonic
            if (
                not self.lighter_order_book_ready
                or self.lighter_order_book_sequence_gap
                or self.lighter_order_book_nonce is None
                or received_at is None
            ):
                return None, None, None
            age_ms = max(0, int((time.monotonic() - received_at) * 1000))
            actual_execution = self._lighter_execution_from_book_locked(
                lighter_side=lighter_side,
                qty=firm_qty,
            )
            reference_execution = self._lighter_execution_from_book_locked(
                lighter_side=lighter_side,
                qty=reference_qty,
            )
        if (
            age_ms > self.strategy_config.max_quote_age_ms
            or actual_execution is None
            or reference_execution is None
        ):
            return None, None, age_ms
        return actual_execution[0], reference_execution[0], age_ms

    async def get_lighter_execution_snapshot(
        self,
        *,
        lighter_side: str,
        qty: Decimal,
    ) -> tuple[Decimal | None, Decimal | None, int | None, int | None]:
        """Atomically read the newest executable depth, nonce, and age."""
        async with self.lighter_order_book_lock:
            received_at = self.lighter_book_received_monotonic
            if (
                not self.lighter_order_book_ready
                or self.lighter_order_book_sequence_gap
                or self.lighter_order_book_nonce is None
                or received_at is None
            ):
                return None, None, self.lighter_order_book_nonce, None
            age_ms = max(0, int((time.monotonic() - received_at) * 1000))
            execution = self._lighter_execution_from_book_locked(
                lighter_side=lighter_side,
                qty=qty,
            )
            nonce = self.lighter_order_book_nonce
        if execution is None:
            return None, None, nonce, age_ms
        vwap, marginal_price = execution
        return vwap, marginal_price, nonce, age_ms

    async def capture_lighter_hedge_dispatch_snapshot(
        self,
        *,
        record: OrderLifecycle,
        lighter_side: str,
        base_amount: int,
        market_generation: int,
        market_index: int,
    ) -> tuple[LighterHedgeDispatchSnapshot | None, str | None]:
        """Capture the only post-Commit pre-send snapshot with integer prices.

        Firm Guard is the pre-Commit admission decision.  Once Variational has
        committed, refusing the Lighter hedge cannot preserve that theoretical
        PnL; it only creates one-sided exposure.  New-opening hedges retain the
        fresh-depth and configured IOC checks.  A reduce-only close bypasses
        those local economic/slippage vetoes and uses a practically unbounded
        market price because restoring a flat account is mandatory.
        """
        trace_id = record.trace_id
        async with self.lighter_order_book_lock:
            book_ready = self.lighter_order_book_ready
            sequence_gap = self.lighter_order_book_sequence_gap
            received_at = self.lighter_book_received_monotonic
            age_ms = (
                max(0, int((time.monotonic() - received_at) * 1000))
                if self.lighter_order_book_ready and received_at is not None
                else None
            )
            nonce = self.lighter_order_book_nonce
            tick_side = "asks" if lighter_side == "BUY" else "bids"
            best_price_i = (
                min(self.lighter_order_book_ticks["asks"], default=None)
                if tick_side == "asks"
                else max(self.lighter_order_book_ticks["bids"], default=None)
            )
            price_multiplier = int(self.price_multiplier or 0)
            cache_key = (lighter_side.strip().upper(), base_amount)
            tick_execution = self.lighter_execution_tick_cache.get(cache_key)
            if tick_execution is None and (
                self.lighter_order_book_ticks["bids"]
                or self.lighter_order_book_ticks["asks"]
            ):
                tick_execution = calculate_lighter_execution_tick_values(
                    self.lighter_order_book_ticks,
                    lighter_side,
                    base_amount,
                )
                if tick_execution is not None:
                    self.lighter_execution_tick_cache[cache_key] = tick_execution

            # Restored local state and small test fixtures may predate the
            # tick cache.  Running market feeds always populate tick data;
            # this compatibility path never changes the submitted IOC price.
            if tick_execution is None and best_price_i is None:
                raw_best = (
                    min(self.lighter_order_book["asks"], default=None)
                    if tick_side == "asks"
                    else max(self.lighter_order_book["bids"], default=None)
                )
                if raw_best is not None and price_multiplier > 0:
                    best_price_i = int(
                        (raw_best * price_multiplier).to_integral_value(
                            rounding=ROUND_UP if lighter_side == "BUY" else ROUND_DOWN
                        )
                    )
                if raw_best is not None and price_multiplier > 0:
                    execution_qty = Decimal(base_amount) / Decimal(
                        str(self.base_amount_multiplier or 1)
                    )
                    raw_execution = calculate_lighter_execution(
                        self.lighter_order_book,
                        lighter_side,
                        execution_qty,
                    )
                    if raw_execution is not None:
                        _vwap, raw_marginal = raw_execution
                        marginal_i = int(
                            (raw_marginal * price_multiplier).to_integral_value(
                                rounding=ROUND_UP if lighter_side == "BUY" else ROUND_DOWN
                            )
                        )
                        tick_execution = (0, marginal_i)

        if (
            self.market_generation != market_generation
            or self.lighter_market_index != market_index
        ):
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=False,
                reason="market_changed",
                client_order_id=record.lighter_client_order_id,
            )
            return None, "Market changed before Lighter hedge submission"
        is_reduce_only_recovery = record.lighter_reduce_only
        if is_reduce_only_recovery:
            marginal_i = tick_execution[1] if tick_execution is not None else None
            anchor_price_i = best_price_i or marginal_i
            fallback_anchor = record.firm_price or record.var_fill_price
            if (
                anchor_price_i is None
                and fallback_anchor is not None
                and fallback_anchor > 0
                and price_multiplier > 0
            ):
                anchor_price_i = int(
                    (fallback_anchor * price_multiplier).to_integral_value(
                        rounding=ROUND_UP if lighter_side == "BUY" else ROUND_DOWN
                    )
                )
            price_i = (
                lighter_reduce_only_market_price_tick(
                    anchor_price_i=anchor_price_i,
                    lighter_side=lighter_side,
                )
                if anchor_price_i is not None
                else None
            )
            if price_i is None or price_i <= 0:
                return None, "Unable to calculate mandatory Lighter reduce-only market price"
            snapshot = LighterHedgeDispatchSnapshot(
                market_generation=market_generation,
                market_index=market_index,
                base_amount=base_amount,
                price_i=price_i,
                marginal_price_i=marginal_i or anchor_price_i,
                economic_limit_price_i=None,
                order_book_nonce=nonce,
                quote_age_ms=age_ms if age_ms is not None else -1,
            )
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=True,
                reason="mandatory_reduce_only_market_sweep",
                client_order_id=record.lighter_client_order_id,
                quote_age_ms=age_ms,
                order_book_nonce=nonce,
                base_amount=base_amount,
                marginal_price_i=marginal_i,
                existing_limit_price_i=price_i,
                configured_limit_price_i=None,
                economic_limit_price_i=None,
                reduce_only_recovery=True,
                local_price_guard_bypassed=True,
            )
            return snapshot, None

        if not book_ready or sequence_gap or nonce is None:
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=False,
                reason="invalid_order_book_sequence",
                client_order_id=record.lighter_client_order_id,
                order_book_nonce=nonce,
            )
            return None, "Lighter order book sequence is not ready before submission"
        if age_ms is None or age_ms > self.strategy_config.max_quote_age_ms:
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=False,
                reason="stale_or_unavailable_depth",
                client_order_id=record.lighter_client_order_id,
                quote_age_ms=age_ms,
                order_book_nonce=nonce,
            )
            return None, f"Lighter depth stale or unavailable before submission ({age_ms}ms)"
        if best_price_i is None or tick_execution is None:
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=False,
                reason="insufficient_full_depth",
                client_order_id=record.lighter_client_order_id,
                quote_age_ms=age_ms,
                order_book_nonce=nonce,
                base_amount=base_amount,
            )
            return None, f"Lighter full depth unavailable for remaining hedge base amount {base_amount}"

        _quote_ticks, marginal_i = tick_execution
        effective_slippage_bps = self.strategy_config.hedge_slippage_bps
        configured_price_i = lighter_ioc_limit_price_tick(
            best_price_i=best_price_i,
            lighter_side=lighter_side,
            slippage_bps=effective_slippage_bps,
        )
        if configured_price_i is None or configured_price_i <= 0:
            return None, "Unable to calculate Lighter IOC limit price"
        economic_limit = (
            lighter_economic_limit_price(
                var_side=record.side,
                firm_price=record.firm_price,
                firm_qty=record.qty,
                required_pnl=record.firm_required_pnl,
            )
            if (
                record.firm_price is not None
                and record.firm_required_pnl is not None
            )
            else None
        )
        economic_limit_price_i: int | None = None
        if economic_limit is not None and price_multiplier > 0:
            economic_limit_price_i = int(
                (economic_limit * price_multiplier).to_integral_value(
                    rounding=ROUND_DOWN if lighter_side == "BUY" else ROUND_UP
                )
            )
            if economic_limit_price_i <= 0:
                return None, "Firm Guard produced an invalid Lighter economic limit"
        price_i = configured_price_i
        within_limit = marginal_i <= price_i if lighter_side == "BUY" else marginal_i >= price_i
        if not within_limit:
            self.trace_event(
                "lighter_pre_submit_revalidation",
                trace_id,
                allowed=False,
                reason="marginal_price_outside_existing_limit",
                client_order_id=record.lighter_client_order_id,
                quote_age_ms=age_ms,
                order_book_nonce=nonce,
                marginal_price_i=marginal_i,
                existing_limit_price_i=price_i,
                configured_limit_price_i=configured_price_i,
                economic_limit_price_i=economic_limit_price_i,
                effective_slippage_bps=effective_slippage_bps,
                reduce_only_recovery=is_reduce_only_recovery,
            )
            return None, "Lighter marginal depth price moved outside the configured IOC limit"

        snapshot = LighterHedgeDispatchSnapshot(
            market_generation=market_generation,
            market_index=market_index,
            base_amount=base_amount,
            price_i=price_i,
            marginal_price_i=marginal_i,
            economic_limit_price_i=economic_limit_price_i,
            order_book_nonce=nonce,
            quote_age_ms=age_ms,
        )
        self.trace_event(
            "lighter_pre_submit_revalidation",
            trace_id,
            allowed=True,
            client_order_id=record.lighter_client_order_id,
            quote_age_ms=age_ms,
            order_book_nonce=nonce,
            base_amount=base_amount,
            marginal_price_i=marginal_i,
            existing_limit_price_i=price_i,
            configured_limit_price_i=configured_price_i,
            economic_limit_price_i=economic_limit_price_i,
            effective_slippage_bps=effective_slippage_bps,
            reduce_only_recovery=is_reduce_only_recovery,
        )
        return snapshot, None

    def get_lighter_order_entry(self) -> LighterOrderEntry:
        if self.lighter_order_entry is None:
            self.lighter_order_entry = LighterOrderEntry(
                self.build_lighter_ws_url(),
                ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                response_timeout=LIGHTER_ORDER_ENTRY_RESPONSE_TIMEOUT_SECONDS,
                max_queue_size=LIGHTER_ORDER_ENTRY_QUEUE_SIZE,
            )
        return self.lighter_order_entry

    def lighter_order_entry_is_ready(self) -> bool:
        entry = self.lighter_order_entry
        return bool(
            self.lighter_order_entry_enabled
            and entry is not None
            and entry.is_ready
        )

    def log_lighter_order_entry_transition(self) -> None:
        if not self.lighter_order_entry_enabled:
            return
        ready = self.lighter_order_entry_is_ready()
        previous = self._lighter_order_entry_last_observed_ready
        if previous is ready:
            return
        self._lighter_order_entry_last_observed_ready = ready
        if ready:
            self.logger.info(
                "Lighter dedicated order-entry WebSocket recovered and is ready; low-latency live opens are enabled"
            )
        elif previous is True:
            self.logger.warning(
                "Lighter dedicated order-entry WebSocket disconnected; new live opens are blocked while close/recovery REST fallback remains available"
            )

    async def prewarm_lighter_order_entry(self) -> None:
        if not self.lighter_order_entry_enabled:
            self._lighter_order_entry_last_observed_ready = False
            return
        entry = self.get_lighter_order_entry()
        try:
            await asyncio.wait_for(entry.start(), timeout=1.0)
            self._lighter_order_entry_last_observed_ready = True
            self.logger.info("Lighter dedicated order-entry WebSocket is ready")
        except asyncio.TimeoutError:
            # Background reconnect remains active.  Only new strategy exposure
            # is gated; close and recovery hedges retain the safe REST fallback.
            self._lighter_order_entry_last_observed_ready = False
            self.logger.warning(
                "Lighter dedicated order-entry WebSocket did not become ready within 1.0s; background reconnect continues and new live opens remain blocked"
            )
        except LighterOrderEntryUnavailable as exc:
            self._lighter_order_entry_last_observed_ready = False
            self.logger.warning(
                "Lighter dedicated order-entry WebSocket not ready at startup (%s); background reconnect continues and new live opens remain blocked",
                exc,
            )

    async def submit_lighter_create_order(
        self,
        *,
        market_index: int,
        client_order_id: int,
        base_amount: int,
        price: int,
        is_ask: bool,
        reduce_only: bool,
        trace_id: str | None,
    ) -> tuple[Any | None, str | None]:
        """Sign and submit a Lighter IOC using the configured transport.

        A WebSocket timeout is intentionally returned as an *unknown* result.
        The caller's existing reconciliation path then checks the deterministic
        client order id before deciding whether another order is safe.
        """
        async with self._lighter_signer_lock:
            if not self.lighter_client:
                self.initialize_lighter_client()
            assert self.lighter_client is not None
            client = self.lighter_client
            create_kwargs = {
                "market_index": market_index,
                "client_order_index": client_order_id,
                "base_amount": base_amount,
                "price": price,
                "is_ask": is_ask,
                "order_type": client.ORDER_TYPE_MARKET,
                "time_in_force": client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                "reduce_only": reduce_only,
                "trigger_price": 0,
                "order_expiry": client.DEFAULT_IOC_EXPIRY,
            }
            if not self.lighter_order_entry_enabled:
                self.trace_event(
                    "lighter_order_transport",
                    trace_id,
                    transport="rest",
                    reason="websocket_disabled",
                    client_order_id=client_order_id,
                )
                _created, response, error = await client.create_order(**create_kwargs)
                return response, error

            entry = self.get_lighter_order_entry()
            if not entry.is_ready:
                if self.lighter_order_entry_rest_fallback:
                    self.trace_event(
                        "lighter_order_transport",
                        trace_id,
                        transport="rest",
                        reason="websocket_not_ready_before_signing",
                        client_order_id=client_order_id,
                    )
                    _created, response, error = await client.create_order(**create_kwargs)
                    return response, error
                return None, "Lighter order-entry WebSocket is not ready"

            api_key_index, nonce = client.nonce_manager.next_nonce()
            sign_started_ns = time.monotonic_ns()
            tx_type, tx_info, tx_hash, sign_error = client.sign_create_order(
                **create_kwargs,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            if sign_error is not None:
                client.nonce_manager.acknowledge_failure(api_key_index)
                return None, sign_error
            self.trace_event(
                "lighter_order_signed",
                trace_id,
                transport="websocket",
                client_order_id=client_order_id,
                signer_started_monotonic_ns=sign_started_ns,
                signer_elapsed_ns=time.monotonic_ns() - sign_started_ns,
            )
            try:
                receipt = await entry.submit(
                    tx_type=tx_type,
                    tx_info=tx_info,
                    tx_hash=tx_hash,
                    request_id=f"v-{client_order_id}-{(trace_id or 'order')[:12]}",
                )
            except LighterOrderEntryUnavailable as exc:
                # No frame left this process.  Release the reserved nonce and
                # use the proven REST path only when explicitly allowed.
                client.nonce_manager.acknowledge_failure(api_key_index)
                if self.lighter_order_entry_rest_fallback:
                    self.trace_event(
                        "lighter_order_entry_rest_fallback",
                        trace_id,
                        client_order_id=client_order_id,
                        reason=str(exc),
                    )
                    _created, response, error = await client.create_order(**create_kwargs)
                    return response, error
                return None, str(exc)
            except LighterOrderEntryUnknown as exc:
                # Do not roll nonce back and do not submit again: the exchange
                # may have accepted the signed frame.
                return None, str(exc)

            response_code = receipt.code
            self.trace_event(
                "lighter_order_entry_receipt",
                trace_id,
                transport="websocket",
                client_order_id=client_order_id,
                queue_wait_ns=receipt.queue_wait_ns,
                round_trip_ns=receipt.round_trip_ns,
                send_monotonic_ns=receipt.send_monotonic_ns,
                response_monotonic_ns=receipt.response_monotonic_ns,
                response_code=response_code,
            )
            if response_code not in {0, 200, "0", "200", None}:
                client.nonce_manager.acknowledge_failure(api_key_index)
            return receipt, None

    def _execution_loss_record_snapshot(self) -> list[ExecutionLossSample]:
        return [
            sample
            for values in self.execution_loss_sample_records.values()
            for sample in values
        ]

    def provisional_phase_reserve_usd(self, notional_usd: Decimal) -> Decimal:
        """Fixed v1 reserve for both legs of one open or close phase."""

        return self.phase_reserve_usd(
            notional_usd,
            self.strategy_config.provisional_reserve_bps_per_leg,
        )

    def effective_open_execution_headroom_bps(
        self,
        side: str,
        notional_usd: Decimal,
        *,
        reserve_bps_per_leg: Decimal | None = None,
        sample_notional_usd: Decimal | None = None,
    ) -> Decimal:
        """Keep post-fill loss samples diagnostic; never raise the signal gate.

        Historical adverse fills describe execution quality, not the current
        opportunity. Feeding them back into the signal threshold made the
        strategy wait for extreme, short-lived dislocations. Bad fills are
        therefore not allowed to grow the rolling signal threshold dynamically.
        A small fixed reserve is applied later to the already-fetched Firm
        Quote, so it adds no network round trip and does not make the visible
        5m/30m/1h trigger harder to reach.
        """

        del side, notional_usd, reserve_bps_per_leg, sample_notional_usd
        return Decimal("0")

    def firm_open_execution_reserve_bps(
        self,
        *,
        reserve_bps_per_leg: Decimal | None = None,
    ) -> Decimal:
        """Return the fixed final-commit margin for the Lighter opening leg."""

        reserve = (
            reserve_bps_per_leg
            if reserve_bps_per_leg is not None
            else self.strategy_config.provisional_reserve_bps_per_leg
        )
        if not reserve.is_finite() or reserve <= 0:
            return Decimal("0")
        return reserve

    def firm_open_execution_reserve_usd(
        self,
        notional_usd: Decimal,
        *,
        reserve_bps_per_leg: Decimal | None = None,
    ) -> Decimal:
        return (
            notional_usd
            * self.firm_open_execution_reserve_bps(
                reserve_bps_per_leg=reserve_bps_per_leg,
            )
            / Decimal("10000")
        )

    @staticmethod
    def phase_reserve_usd(
        notional_usd: Decimal,
        reserve_bps_per_leg: Decimal,
    ) -> Decimal:
        """Return one two-leg phase reserve from an explicit frozen rate."""

        return (
            notional_usd
            * Decimal("2")
            * reserve_bps_per_leg
            / Decimal("10000")
        )

    def _capture_execution_loss_locked(self, record: OrderLifecycle) -> None:
        if (
            record.execution_loss_recorded
            or record.var_fill_source not in {"event", "portfolio"}
            or record.firm_guard_pnl is None
            or record.lighter_fill_price is None
            or record.hedge_status != "filled"
            or not record.strategy_phase
        ):
            return
        actual_pnl = leg_result_by_direction(record).pnl
        if actual_pnl is None:
            return
        loss = record.firm_guard_pnl - actual_pnl
        record.execution_loss_usd = loss
        record.execution_loss_recorded = True
        matched_qty = min(record.qty, record.lighter_filled_qty or record.qty)
        sample_notional = (
            matched_qty * record.var_fill_price
            if record.var_fill_price is not None
            else None
        )
        if sample_notional is not None and sample_notional > 0:
            sample = ExecutionLossSample.from_loss(
                timestamp=record.lighter_fill_ts_iso or utc_now(),
                asset=record.asset,
                phase=record.strategy_phase,
                side=record.side,
                notional_usd=sample_notional,
                loss_usd=loss,
            )
            assert sample.notional_bucket is not None
            self.execution_loss_sample_records[
                (sample.phase, sample.side, sample.notional_bucket)
            ].append(sample)
        if loss > 0:
            self.execution_loss_samples[
                (record.strategy_phase, record.side.upper())
            ].append(loss)
        self._execution_samples_revision += 1

    async def request_guarded_var_order(
        self,
        *,
        phase: str,
        side: str,
        amount: Decimal,
        base_qty: Decimal | None,
        open_candidate: OpenCandidate | None = None,
        close_candidate: CloseCandidate | None = None,
        expected_intent: VarOrderIntent | None = None,
    ) -> dict[str, Any]:
        config = self.strategy_config
        trace_id = new_trace_id()
        initial_open = await self._current_open_record()
        expected_open_trade_key = (
            initial_open.trade_key if initial_open is not None else None
        )
        common = {
            "side": side,
            "amount": decimal_to_str(amount) or "0",
            "base_qty": decimal_to_str(base_qty),
            "market": self.variational_ticker,
            "timeout_ms": config.var_order_result_timeout_ms,
            "phase": phase,
            "trace_id": trace_id,
        }
        self.trace_event(
            "variational_quote_dispatch",
            trace_id,
            phase=phase,
            side=side,
            amount=amount,
            base_qty=base_qty,
            market=self.variational_ticker,
        )
        quote_result = {
            **await self.runtime.command_broker.request_place_order(
                **common,
                fetch_stage="quote",
                guard={"required": False},
            ),
            "trace_id": trace_id,
        }
        quote_result_detail = (
            quote_result.get("detail") if isinstance(quote_result.get("detail"), dict) else {}
        )
        quote_detail = (
            quote_result_detail.get("quote")
            if isinstance(quote_result_detail.get("quote"), dict)
            else {}
        )
        self.trace_event(
            "variational_quote_result",
            trace_id,
            ok=bool(quote_result.get("ok")),
            request_id=quote_result.get("requestId"),
            error=quote_result.get("error"),
            browser_elapsed_ms=quote_result_detail.get("elapsedMs"),
            browser_quote_elapsed_ms=quote_detail.get("elapsedMs"),
            extension_trace_id=quote_result.get("traceId"),
        )
        if not quote_result.get("ok"):
            return quote_result

        quote_id = str(quote_detail.get("quoteId") or "").strip()
        firm_price = to_decimal(quote_detail.get("firmPrice"))
        firm_qty = to_decimal(quote_detail.get("firmQty"))
        if not quote_id or firm_price is None or firm_price <= 0 or firm_qty is None or firm_qty <= 0:
            self.trace_event(
                "firm_quote_guard",
                trace_id,
                allowed=False,
                reason="firm_quote_missing_required_fields",
            )
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": "fresh firm quote is missing quote_id, price, or quantity",
                "detail": quote_result_detail,
                "trace_id": trace_id,
            }

        if (open_candidate is None) == (close_candidate is None):
            raise ValueError("exactly one frozen adaptive candidate is required")
        firm_reference_vwap: Decimal | None = None
        if open_candidate is not None:
            (
                lighter_vwap,
                firm_reference_vwap,
                lighter_age_ms,
            ) = await self.get_fresh_lighter_open_vwaps(
                var_side=side,
                firm_price=firm_price,
                firm_qty=firm_qty,
                reference_notional_usd=open_candidate.epoch.reference_notional_usd,
            )
        else:
            lighter_vwap, lighter_age_ms = await self.get_fresh_lighter_vwap(
                var_side=side,
                qty=firm_qty,
            )
        if lighter_vwap is None or (
            open_candidate is not None and firm_reference_vwap is None
        ):
            reason = (
                f"Lighter depth is stale ({lighter_age_ms}ms)"
                if lighter_age_ms is not None
                else "Lighter depth is unavailable"
            )
            self.trace_event(
                "firm_quote_guard",
                trace_id,
                allowed=False,
                reason="lighter_depth_unavailable_or_stale",
                quote_id=quote_id,
                lighter_quote_age_ms=lighter_age_ms,
            )
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": f"fresh firm quote rejected: {reason}",
                "detail": {**quote_result_detail, "quote": quote_detail},
                "trace_id": trace_id,
            }

        now_ms = time.time_ns() // 1_000_000
        firm_notional = firm_price * firm_qty
        firm_target_notional = (
            to_decimal(quote_detail.get("targetNotionalUsd"))
            or to_decimal(quote_detail.get("firmNotionalUsd"))
            or amount
        )
        if firm_target_notional <= 0:
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": "fresh firm quote rejected: invalid template target amount",
                "detail": {**quote_result_detail, "quote": quote_detail},
                "trace_id": trace_id,
            }
        strategy_payload: dict[str, Any]
        firm_regression_required = False
        if open_candidate is not None:
            assert firm_reference_vwap is not None
            epoch = open_candidate.epoch
            if (
                phase.strip().lower() != "open"
                or open_candidate.direction.value != side.strip().upper()
                or epoch.model_hash != self.strategy_model.model_hash
                or epoch.config_hash != self.strategy_config_hash
            ):
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": "fresh firm quote rejected: frozen open context mismatch",
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            base_frame = self.last_market_frame
            if base_frame is None:
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": "fresh firm quote rejected: market frame unavailable",
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            actual_rate = (
                (lighter_vwap - firm_price) / firm_price
                if open_candidate.direction is StrategySide.BUY
                else (firm_price - lighter_vwap) / firm_price
            )
            firm_reference_rate = (
                (firm_reference_vwap - firm_price) / firm_price
                if open_candidate.direction is StrategySide.BUY
                else (firm_price - firm_reference_vwap) / firm_price
            )
            reference_rates = DirectionalRates(
                buy=(
                    firm_reference_rate
                    if open_candidate.direction is StrategySide.BUY
                    else base_frame.reference_rates.buy
                ),
                sell=(
                    firm_reference_rate
                    if open_candidate.direction is StrategySide.SELL
                    else base_frame.reference_rates.sell
                ),
            )
            actual_rates = DirectionalRates(
                buy=(
                    actual_rate
                    if open_candidate.direction is StrategySide.BUY
                    else base_frame.actual_rates.buy
                ),
                sell=(
                    actual_rate
                    if open_candidate.direction is StrategySide.SELL
                    else base_frame.actual_rates.sell
                ),
            )
            firm_frame = MarketFrame(
                asset=base_frame.asset,
                captured_at_ms=now_ms,
                variational_clock=SourceClock(now_ms, now_ms, 0),
                lighter_clock=SourceClock(
                    max(0, now_ms - (lighter_age_ms or 0)),
                    max(0, now_ms - (lighter_age_ms or 0)),
                    lighter_age_ms or 0,
                ),
                source_skew_ms=lighter_age_ms or 0,
                var_bid=(
                    firm_price
                    if open_candidate.direction is StrategySide.SELL
                    else min(base_frame.var_bid, firm_price)
                ),
                var_ask=(
                    firm_price
                    if open_candidate.direction is StrategySide.BUY
                    else max(base_frame.var_ask, firm_price)
                ),
                lighter_reference_buy_vwap=base_frame.lighter_reference_buy_vwap,
                lighter_reference_sell_vwap=base_frame.lighter_reference_sell_vwap,
                lighter_actual_buy_vwap=(
                    lighter_vwap
                    if open_candidate.direction is StrategySide.SELL
                    else base_frame.lighter_actual_buy_vwap
                ),
                lighter_actual_sell_vwap=(
                    lighter_vwap
                    if open_candidate.direction is StrategySide.BUY
                    else base_frame.lighter_actual_sell_vwap
                ),
                reference_notional_usd=base_frame.reference_notional_usd,
                actual_notional_usd=firm_notional,
                reference_rates=reference_rates,
                actual_rates=actual_rates,
            )
            confirmation = self.strategy_engine.confirm_open(
                candidate=open_candidate,
                firm_frame=firm_frame,
                firm_notional_usd=firm_notional,
                target_notional_usd=firm_target_notional,
                now_ms=now_ms,
            )
            if confirmation.action is not StrategyAction.OPEN:
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": f"fresh firm quote rejected: {confirmation.reason}",
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            confirmed = confirmation.open_candidate
            assert confirmed is not None
            opposite_component = epoch.component(confirmed.direction.opposite)
            projected_exit_rate = (
                opposite_component.exit_opportunity
                if epoch.model_version in {
                    "adaptive-median-v2",
                    "adaptive-median-v3",
                    "adaptive-median-v4",
                    "adaptive-median-v5",
                }
                else opposite_component.baseline
            )
            close_credit = firm_notional * projected_exit_rate
            wear = firm_notional * epoch.max_normal_round_wear_bps / Decimal("10000")
            execution_headroom_bps = self.firm_open_execution_reserve_bps(
                reserve_bps_per_leg=epoch.reserve_bps_per_leg,
            )
            execution_reserve = self.firm_open_execution_reserve_usd(
                firm_notional,
                reserve_bps_per_leg=epoch.reserve_bps_per_leg,
            )
            firm_reference_threshold = confirmed.threshold
            if firm_reference_rate < firm_reference_threshold:
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": (
                        "fresh firm quote rejected: reference depth has not "
                        "covered the frozen threshold"
                    ),
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            close_reserve = self.phase_reserve_usd(
                firm_notional,
                epoch.reserve_bps_per_leg,
            )
            # V3/V4/V5 previously subtracted this reserve here and added it back
            # inside evaluate_firm_quote_guard(), cancelling the protection.
            # The dynamic threshold is the true minimum; the direction-specific
            # execution headroom is now added exactly once by the Firm Guard,
            # matching the latest legacy execution-layer semantics.
            minimum_pnl = (
                confirmed.threshold * firm_notional
                if epoch.model_version
                in {
                    "adaptive-median-v3",
                    "adaptive-median-v4",
                    "adaptive-median-v5",
                }
                else -wear - close_credit + close_reserve
            )
            strategy_payload = open_candidate_to_payload(confirmed)
            strategy_payload.update(
                {
                    "firmNotionalUsd": decimal_to_str(firm_notional),
                    "firmCloseCreditUsd": decimal_to_str(close_credit),
                    "firmOpenReserveUsd": decimal_to_str(execution_reserve),
                    "firmOpenExecutionHeadroomBps": decimal_to_str(
                        execution_headroom_bps
                    ),
                    "firmReferenceRate": decimal_to_str(firm_reference_rate),
                    "firmReferenceVwap": decimal_to_str(firm_reference_vwap),
                    "firmCloseReserveUsd": decimal_to_str(close_reserve),
                    "firmWearUsd": decimal_to_str(wear),
                }
            )
        else:
            assert close_candidate is not None
            current_open = await self._current_open_record()
            frozen_open = (
                open_candidate_from_payload(current_open.adaptive_strategy_context)
                if current_open is not None
                else None
            )
            context_age_ms = now_ms - close_candidate.frame_captured_at_ms
            firm_lighter_target = lighter_hedge_target_qty(
                firm_qty,
                self.base_amount_multiplier,
            )
            frozen_lighter_target = (
                lighter_hedge_target_qty(base_qty, self.base_amount_multiplier)
                if base_qty is not None
                else None
            )
            if (
                phase.strip().lower() != "close"
                or close_candidate.close_direction.value != side.strip().upper()
                or base_qty is None
                or abs(firm_qty - base_qty) > VAR_BASE_QTY_TICK
                or firm_lighter_target is None
                or frozen_lighter_target is None
                or firm_lighter_target != frozen_lighter_target
                or current_open is None
                or frozen_open is None
                or frozen_open.epoch.epoch_id != close_candidate.frozen_epoch_id
                or context_age_ms < 0
            ):
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": "fresh firm quote rejected: frozen close context mismatch",
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            open_pnl = leg_result_by_direction(current_open).pnl
            open_notional = var_open_notional_usd(current_open)
            if open_pnl is None or open_notional is None:
                return {
                    "type": "ORDER_RESULT",
                    "requestId": quote_result.get("requestId"),
                    "ok": False,
                    "error": "fresh firm quote rejected: actual open economics unavailable",
                    "detail": {**quote_result_detail, "quote": quote_detail},
                    "trace_id": trace_id,
                }
            # The wear floor remains tied to the actual opening amount, while
            # this phase reserve scales with the exact current Firm notional.
            execution_reserve = (
                self.phase_reserve_usd(
                    firm_notional,
                    frozen_open.epoch.reserve_bps_per_leg,
                )
                * CLOSE_RESERVE_MULTIPLIER
            )
            firm_regression_required = (
                frozen_open.epoch.model_version == "adaptive-median-v1"
            )
            if close_candidate.zero_wear_stability_passed:
                # Stability is an alternative only while the gross round is
                # above zero.  Subtracting the reserve here cancels the guard's
                # normal reserve addition, so the fresh Firm check still
                # requires actual_open + firm_close >= 0, never a negative
                # zero-wear result.
                minimum_pnl = -open_pnl - execution_reserve
            else:
                minimum_pnl = close_candidate.required_floor_usd - open_pnl
            strategy_payload = {
                "schema": "adaptive-close-context-v1",
                "strategyTag": ADAPTIVE_MODEL_VERSION,
                "epochId": close_candidate.frozen_epoch_id,
                "heldSeconds": close_candidate.held_seconds,
                "requiredFloorUsd": decimal_to_str(close_candidate.required_floor_usd),
                "preliminaryRoundLowerBoundUsd": decimal_to_str(
                    close_candidate.round_lower_bound_usd
                ),
                "preliminaryGrossRoundPnlUsd": decimal_to_str(
                    close_candidate.round_lower_bound_usd
                    + close_candidate.close_reserve_usd
                ),
                "firmNotionalUsd": decimal_to_str(firm_notional),
                "firmCloseReserveUsd": decimal_to_str(execution_reserve),
                "zeroWearStabilityPassed": (
                    close_candidate.zero_wear_stability_passed
                ),
                "zeroWearContinuousMs": close_candidate.zero_wear_continuous_ms,
                "zeroWearAccumulatedMs": close_candidate.zero_wear_accumulated_ms,
            }
        decision = evaluate_firm_quote_guard(
            var_side=side,
            firm_price=firm_price,
            firm_qty=firm_qty,
            lighter_vwap=lighter_vwap,
            minimum_pnl=minimum_pnl,
            execution_reserve=execution_reserve,
        )
        firm_regression_passed = True
        if close_candidate is not None:
            firm_close_rate = decision.expected_pnl / firm_notional
            firm_regression_passed = (
                firm_close_rate >= close_candidate.regression_target_rate
            )
            strategy_payload.update(
                {
                    "firmCloseRate": decimal_to_str(firm_close_rate),
                    "regressionTargetRate": decimal_to_str(
                        close_candidate.regression_target_rate
                    ),
                    "regressionPassed": firm_regression_passed,
                    "regressionRequired": firm_regression_required,
                }
            )
        guarded_quote = {
            **quote_detail,
            "quoteId": quote_id,
            "firmPrice": decimal_to_str(firm_price),
            "firmQty": decimal_to_str(firm_qty),
            "targetNotionalUsd": decimal_to_str(firm_target_notional),
            "guardPnl": decimal_to_str(decision.expected_pnl),
            "guardMinPnl": decimal_to_str(decision.required_pnl),
            "executionReserveUsd": decimal_to_str(execution_reserve),
            "lighterVwap": decimal_to_str(lighter_vwap),
            "lighterQuoteAgeMs": lighter_age_ms,
            "adaptiveStrategy": strategy_payload,
            "strategyTag": ADAPTIVE_MODEL_VERSION,
        }
        self.trace_event(
            "firm_quote_guard",
            trace_id,
            allowed=(
                decision.allowed
                and (firm_regression_passed or not firm_regression_required)
            ),
            quote_id=quote_id,
            firm_price=firm_price,
            firm_qty=firm_qty,
            lighter_vwap=lighter_vwap,
            required_pnl=decision.required_pnl,
            expected_pnl=decision.expected_pnl,
            lighter_quote_age_ms=lighter_age_ms,
            adaptive_strategy=strategy_payload,
        )
        if firm_regression_required and not firm_regression_passed:
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": "fresh firm quote rejected: close baseline regression not met",
                "detail": {**quote_result_detail, "quote": guarded_quote},
                "trace_id": trace_id,
            }
        if not decision.allowed:
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": (
                    "fresh firm quote rejected: expected "
                    f"{decision.expected_pnl:.4f}U < required {decision.required_pnl:.4f}U"
                ),
                "detail": {**quote_result_detail, "quote": guarded_quote},
                "trace_id": trace_id,
            }

        prepared_intent = await self.prepare_pending_var_intent(
            phase=phase,
            side=side,
            amount=amount,
            trace_id=trace_id,
            firm_quote=guarded_quote,
            expected_intent=expected_intent,
        )
        if prepared_intent is None:
            self.trace_event(
                "execution_intent_prepare_failed",
                trace_id,
                phase=phase,
                side=side,
            )
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": "unable to prepare a durable Var execution intent",
                "detail": {**quote_result_detail, "quote": guarded_quote},
                "trace_id": trace_id,
            }
        self.trace_event(
            "execution_intent_prepared",
            trace_id,
            phase=phase,
            side=side,
            quote_id=prepared_intent.firm_quote_id,
            lighter_client_order_index=prepared_intent.lighter_client_order_index,
            collision=prepared_intent.lighter_client_order_collision,
        )
        precondition_error = await self.pending_var_commit_precondition_error(
            intent=prepared_intent,
            phase=phase,
            side=side,
            amount=amount,
            trace_id=trace_id,
            expected_open_trade_key=expected_open_trade_key,
            base_qty=base_qty,
            require_live_ready=(expected_intent is not None),
        )
        if precondition_error is not None:
            cleared = self.clear_matching_var_intent(
                side,
                force=True,
                expected_intent=prepared_intent,
            )
            if cleared:
                await self.persist_runtime_state()
            self.trace_event(
                "execution_intent_commit_precondition_failed",
                trace_id,
                reason=precondition_error,
                prepared_intent_cleared=cleared,
            )
            return {
                "type": "ORDER_RESULT",
                "requestId": quote_result.get("requestId"),
                "ok": False,
                "error": precondition_error,
                "detail": {**quote_result_detail, "quote": guarded_quote},
                "trace_id": trace_id,
            }

        self.trace_event("execution_intent_committing", trace_id)
        self.trace_event(
            "variational_commit_dispatch",
            trace_id,
            quote_id=quote_id,
            phase=phase,
            side=side,
        )
        try:
            commit_response = await self.runtime.command_broker.request_place_order(
                **common,
                fetch_stage="commit",
                firm_quote=guarded_quote,
                guard={"required": False},
            )
        except asyncio.CancelledError:
            await self.mark_pending_var_intent_commit_ambiguous(
                phase=phase,
                side=side,
                trace_id=trace_id,
                expected_intent=prepared_intent,
            )
            raise
        except Exception:
            await self.mark_pending_var_intent_commit_ambiguous(
                phase=phase,
                side=side,
                trace_id=trace_id,
                expected_intent=prepared_intent,
            )
            raise
        commit_result = {**commit_response, "trace_id": trace_id}
        if commit_result.get("ok"):
            async with self._record_lock:
                if self.pending_var_intent is prepared_intent:
                    prepared_intent.commit_accepted_monotonic = time.monotonic()
        elif var_result_is_ambiguous(commit_result):
            await self.mark_pending_var_intent_commit_ambiguous(
                phase=phase,
                side=side,
                trace_id=trace_id,
                expected_intent=prepared_intent,
            )
        commit_result_detail = (
            commit_result.get("detail") if isinstance(commit_result.get("detail"), dict) else {}
        )
        self.trace_event(
            "variational_commit_result",
            trace_id,
            ok=bool(commit_result.get("ok")),
            request_id=commit_result.get("requestId"),
            error=commit_result.get("error"),
            browser_elapsed_ms=commit_result_detail.get("elapsedMs"),
            extension_trace_id=commit_result.get("traceId"),
        )
        commit_detail = (
            dict(commit_result_detail)
            if commit_result_detail
            else {}
        )
        commit_detail["quote"] = guarded_quote
        return {**commit_result, "detail": commit_detail, "trace_id": trace_id}

    async def start_committed_var_hedge(
        self,
        *,
        phase: str,
        side: str,
        result: dict[str, Any],
    ) -> OrderLifecycle | None:
        side_n = side.strip().upper()
        if not result.get("ok"):
            return None

        detail = result.get("detail") if isinstance(result.get("detail"), dict) else {}
        quote = detail.get("quote") if isinstance(detail.get("quote"), dict) else {}
        firm_price = to_decimal(quote.get("firmPrice"))
        firm_qty = to_decimal(quote.get("firmQty"))
        if firm_price is None or firm_price <= 0 or firm_qty is None or firm_qty <= 0:
            self.pause_automation("Var commit succeeded without a usable firm price/quantity")
            return None

        request_id = str(result.get("requestId") or "").strip() or f"commit-{int(time.time() * 1000)}"
        trade_key = f"commit:{request_id}"
        trace_id = str(result.get("trace_id") or "").strip() or new_trace_id()
        firm_quote_id = str(quote.get("quoteId") or "").strip() or None
        commit_rfq_id = commit_rfq_id_from_result(result)
        merged = False
        merged_with_intent = False
        correlation_error: str | None = None
        async with self._record_lock:
            if trade_key in self.records:
                return self.records[trade_key]
            intent = self.pending_var_intent
            if intent is not None:
                intent.commit_rfq_id = commit_rfq_id
            record = None
            if intent is not None and intent.confirmed_trade_key:
                record = self.records.get(intent.confirmed_trade_key)
                if (
                    record is not None
                    and commit_rfq_id is not None
                    and record.var_source_rfq is not None
                    and record.var_source_rfq != commit_rfq_id
                ):
                    correlation_error = (
                        "Var Commit RFQ does not match the pre-observed fill: "
                        f"commit={commit_rfq_id}, fill={record.var_source_rfq}"
                    )
                    record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                    record.hedge_status = "recovery_required"
                    record.hedge_error = correlation_error
            for candidate_key in reversed(self.record_order):
                if record is not None or correlation_error is not None:
                    break
                candidate = self.records.get(candidate_key)
                if (
                    candidate is not None
                    and candidate.side.upper() == side_n
                    and candidate.asset == (self.variational_ticker or candidate.asset).upper()
                    and candidate.var_fill_source in {"event", "portfolio"}
                    and candidate.last_variational_status == "filled"
                    and candidate.var_fill_price is not None
                    and abs(candidate.qty - firm_qty)
                    <= max(VAR_POSITION_TOLERANCE, firm_qty * Decimal("0.10"))
                    and candidate.strategy_phase == phase
                    and candidate.firm_quote_id in {None, firm_quote_id}
                    and (
                        commit_rfq_id is None
                        or candidate.var_source_rfq is None
                        or candidate.var_source_rfq == commit_rfq_id
                    )
                ):
                    record = candidate
                    break
            if correlation_error is not None:
                payload = record.to_payload() if record is not None else {}
            elif record is not None:
                if intent is not None and intent.phase == phase and intent.side == side_n:
                    self._apply_intent_metadata_locked(record, intent)
                    self.pending_var_intent = None
                    merged_with_intent = True
                else:
                    record.trace_id = record.trace_id or trace_id
                    record.firm_quote_id = firm_quote_id
                    record.firm_price = firm_price
                    record.firm_guard_pnl = to_decimal(quote.get("guardPnl"))
                    record.firm_required_pnl = to_decimal(quote.get("guardMinPnl"))
                    record.execution_reserve_usd = to_decimal(quote.get("executionReserveUsd"))
                    dynamic_context = quote.get("adaptiveStrategy")
                    record.adaptive_strategy_context = (
                        dict(dynamic_context) if isinstance(dynamic_context, dict) else None
                    )
                    strategy_tag = str(
                        quote.get("strategyTag") or ADAPTIVE_MODEL_VERSION
                    ).strip()
                    record.strategy_tag = strategy_tag or ADAPTIVE_MODEL_VERSION
                    if phase == "open" and record.open_notional_usd is None:
                        record.open_notional_usd = record.qty * record.var_fill_price
                if record.hedge_status == "waiting_commit":
                    record.hedge_status = "not_started"
                    record.hedge_error = None
                record.execution_state = EXECUTION_STATE_VAR_COMMITTED
                record.var_event_origin = VarEventOrigin.AUTO_INTENT.value
                record.var_source_rfq = record.var_source_rfq or commit_rfq_id
                self._capture_execution_loss_locked(record)
                merged = True
            elif correlation_error is None:
                if intent is None or intent.phase != phase or intent.side != side_n:
                    return None
                record = OrderLifecycle(
                    trade_key=trade_key,
                    trade_id=str(detail.get("orderId") or detail.get("tradeId") or request_id),
                    side=side_n.lower(),
                    qty=firm_qty,
                    asset=(self.variational_ticker or intent.market).upper(),
                    auto_hedge_enabled=self.args.auto_hedge,
                    last_variational_status="accepted",
                    trace_id=trace_id,
                    var_fill_price=firm_price,
                    var_fill_ts_iso=str(result.get("timestamp") or utc_now()),
                    var_fill_source="http_commit",
                    var_event_origin=VarEventOrigin.AUTO_INTENT.value,
                    var_source_rfq=commit_rfq_id,
                    firm_quote_id=firm_quote_id,
                    firm_price=firm_price,
                    firm_guard_pnl=to_decimal(quote.get("guardPnl")),
                    firm_required_pnl=to_decimal(quote.get("guardMinPnl")),
                    execution_reserve_usd=to_decimal(quote.get("executionReserveUsd")),
                    adaptive_strategy_context=(
                        dict(quote.get("adaptiveStrategy"))
                        if isinstance(quote.get("adaptiveStrategy"), dict)
                        else (
                            dict(intent.adaptive_strategy_context)
                            if intent.adaptive_strategy_context is not None
                            else None
                        )
                    ),
                    strategy_tag=(
                        str(
                            quote.get("strategyTag")
                            or intent.strategy_tag
                            or ADAPTIVE_MODEL_VERSION
                        ).strip()
                        or ADAPTIVE_MODEL_VERSION
                    ),
                    strategy_phase=phase,
                    lighter_reduce_only=phase != "open",
                    lighter_reserved_client_order_id=intent.lighter_client_order_index,
                    execution_state=EXECUTION_STATE_VAR_COMMITTED,
                    open_notional_usd=(firm_qty * firm_price if phase == "open" else None),
                )
                self.records[trade_key] = record
                self.record_order.append(trade_key)
                intent.state = VAR_INTENT_COMMITTED
                intent.request_id = request_id
                raw_order_id = detail.get("orderId") or detail.get("tradeId")
                intent.order_id = (
                    str(raw_order_id).strip() if raw_order_id is not None else None
                ) or None
                intent.provisional_trade_key = trade_key
                intent.commit_accepted_monotonic = time.monotonic()
            payload = record.to_payload()

        if correlation_error is not None:
            self.pause_automation(correlation_error)
            await self.append_order_log("variational_commit_correlation_failed", payload)
            await self.persist_runtime_state()
            return None

        self.trace_event(
            "variational_commit_accepted",
            record.trace_id,
            request_id=request_id,
            trade_key=record.trade_key,
            quote_id=record.firm_quote_id,
            merged=merged,
            execution_state=record.execution_state,
            lighter_reserved_client_order_id=record.lighter_reserved_client_order_id,
        )
        async with self._record_lock:
            should_schedule_hedge = (
                record.hedge_status == "not_started"
                and (not merged or merged_with_intent)
            )
        if should_schedule_hedge:
            scheduled = self.schedule_lighter_order(record)
            if scheduled:
                await asyncio.sleep(0)
        await self.append_order_log(
            "variational_commit_metadata_merged" if merged else "variational_commit_accepted",
            payload,
        )
        return record

    def _build_lighter_rollback_locked(
        self,
        record: OrderLifecycle,
    ) -> OrderLifecycle | None:
        filled_qty = record.lighter_filled_qty or Decimal("0")
        rollback_qty = filled_qty - record.lighter_rollback_scheduled_qty
        if rollback_qty <= 0:
            return None
        rollback_side = opposite_var_order_side(record.side)
        if rollback_side is None:
            return None

        rollback_index = len(
            [key for key in self.record_order if key.startswith(f"{record.trade_key}:rollback:")]
        ) + 1
        rollback_key = f"{record.trade_key}:rollback:{rollback_index}"
        rollback = OrderLifecycle(
            trade_key=rollback_key,
            trade_id=rollback_key,
            side=rollback_side.lower(),
            qty=rollback_qty,
            asset=record.asset,
            auto_hedge_enabled=self.args.auto_hedge,
            last_variational_status="rollback",
            var_fill_source="lighter_rollback",
            lighter_reduce_only=record.strategy_phase != "close",
        )
        self.records[rollback_key] = rollback
        self.record_order.append(rollback_key)
        record.lighter_rollback_scheduled_qty += rollback_qty
        return rollback

    def _build_lighter_qty_correction_locked(
        self,
        record: OrderLifecycle,
        qty: Decimal,
    ) -> OrderLifecycle | None:
        correction_side = opposite_var_order_side(record.side)
        lighter_tick = lighter_base_qty_tick(self.base_amount_multiplier)
        remaining_qty = qty - record.lighter_qty_correction_scheduled_qty
        if (
            lighter_tick is None
            or remaining_qty < lighter_tick
            or correction_side is None
        ):
            return None
        correction_index = len(
            [key for key in self.record_order if key.startswith(f"{record.trade_key}:qty-correction:")]
        ) + 1
        correction_key = f"{record.trade_key}:qty-correction:{correction_index}"
        correction = OrderLifecycle(
            trade_key=correction_key,
            trade_id=correction_key,
            side=correction_side.lower(),
            qty=remaining_qty,
            asset=record.asset,
            auto_hedge_enabled=self.args.auto_hedge,
            last_variational_status="qty_correction",
            var_fill_source="lighter_qty_correction",
            lighter_reduce_only=True,
        )
        self.records[correction_key] = correction
        self.record_order.append(correction_key)
        record.lighter_qty_correction_scheduled_qty += remaining_qty
        return correction

    async def mark_unconfirmed_var_commit_ambiguous(
        self,
        expected_intent: VarOrderIntent,
    ) -> bool:
        """Stop a timed-out accepted Commit from spinning or being retried."""

        async with self._record_lock:
            intent = self.pending_var_intent
            if (
                intent is not expected_intent
                or intent.commit_accepted_monotonic is None
            ):
                return False
            intent.state = VAR_INTENT_COMMIT_AMBIGUOUS
            intent.commit_accepted_monotonic = None
            payload = {
                "phase": intent.phase,
                "side": intent.side,
                "amount": decimal_to_str(intent.amount),
                "market": intent.market,
                "request_id": intent.request_id,
                "order_id": intent.order_id,
                "trace_id": intent.trace_id,
                "firm_quote_id": intent.firm_quote_id,
                "provisional_trade_key": intent.provisional_trade_key,
                "state": intent.state,
            }

        reason = (
            "Var commit was accepted but its position/fill could not be confirmed; "
            "manual account reconciliation is required"
        )
        # This is an execution-ambiguity pause, not a manual/operator halt.
        # Give ownership to reconciliation so an authoritative portfolio
        # recovery followed by an exact two-account match can resume safely.
        # Any different pre-existing safety pause keeps ownership and remains
        # latched.
        self.pause_for_reconciliation(reason)
        await self.append_order_log(
            "variational_commit_confirmation_unresolved",
            payload,
        )
        await self.persist_runtime_state()
        return True

    async def rollback_unconfirmed_var_commit(
        self,
        *,
        expected_intent: VarOrderIntent | None = None,
    ) -> bool:
        intent = self.pending_var_intent
        if (
            intent is None
            or (expected_intent is not None and intent is not expected_intent)
            or not intent.provisional_trade_key
        ):
            return False

        async with self._record_lock:
            if self.pending_var_intent is not intent:
                return False
            record = self.records.get(intent.provisional_trade_key)
            if record is None or record.var_fill_source != "http_commit":
                return False
            record.var_fill_price = None
            record.var_fill_ts_iso = None
            record.var_fill_source = "unconfirmed_commit"
            record.last_variational_status = "unconfirmed"
            rollback = self._build_lighter_rollback_locked(record)
            record_payload = record.to_payload()
            rollback_payload = rollback.to_payload() if rollback is not None else None
            self.pending_var_intent = None

        reason = (
            "Var commit returned HTTP success but no position/fill confirmation within "
            f"{VAR_COMMIT_CONFIRM_TIMEOUT_SECONDS:.0f}s"
        )
        self.pause_automation(reason)
        await self.append_order_log("variational_commit_unconfirmed", record_payload)
        if rollback is not None:
            self.schedule_lighter_order(rollback)
            await self.append_order_log("lighter_rollback_queued", rollback_payload or {})
        await self.persist_runtime_state()
        return True

    async def append_auto_var_result_log(
        self,
        *,
        phase: str,
        side: str,
        amount: Decimal,
        expected_pnl: Decimal | None,
        result: dict[str, Any],
        base_qty: Decimal | None = None,
    ) -> None:
        detail = result.get("detail") if isinstance(result.get("detail"), dict) else {}
        quote_detail = detail.get("quote") if isinstance(detail.get("quote"), dict) else {}
        payload = {
            "phase": phase,
            "side": side,
            "amount": decimal_to_str(amount),
            "base_qty": decimal_to_str(base_qty),
            "expected_pnl": decimal_to_str(expected_pnl),
            "ok": bool(result.get("ok")),
            "error": result.get("error"),
            "request_id": result.get("requestId"),
            "trace_id": result.get("trace_id"),
            "result_timestamp": result.get("timestamp"),
            "method": detail.get("method"),
            "url_path": detail.get("urlPath"),
            "status": detail.get("status"),
            "elapsed_ms": detail.get("elapsedMs"),
            "body_format": detail.get("bodyFormat"),
            "side_patched": detail.get("sidePatched"),
            "amount_patched": detail.get("amountPatched"),
            "base_qty_patched": detail.get("baseQtyPatched"),
            "leg_ratio_patched": detail.get("legRatioPatched"),
            "quote_patched": detail.get("quotePatched"),
            "quote_status": quote_detail.get("status"),
            "quote_elapsed_ms": quote_detail.get("elapsedMs"),
            "quote_url_path": quote_detail.get("urlPath"),
            "firm_price": quote_detail.get("firmPrice"),
            "firm_guard_pnl": quote_detail.get("guardPnl"),
            "firm_guard_min_pnl": quote_detail.get("guardMinPnl"),
            "lighter_guard_quote_age_ms": quote_detail.get("lighterQuoteAgeMs"),
            "response_preview": detail.get("responsePreview"),
        }
        await self.append_order_log("auto_var_order_result", payload)

    def _reset_close_zero_wear_stability(self, trade_key: str | None) -> None:
        self._close_stability_trade_key = trade_key
        self._close_zero_wear_started_ms = None
        self._close_zero_wear_last_sample_ms = None
        self._close_zero_wear_last_above = False
        self._close_zero_wear_intervals.clear()

    def _update_close_zero_wear_stability(
        self,
        *,
        trade_key: str,
        above_zero_wear: bool,
        now_ms: int,
    ) -> tuple[bool, int, int]:
        """Track recent positive gross-round time without touching I/O paths.

        Continuous time and the union of positive intervals in the latest ten
        seconds are both measured.  A stale/missing evaluation ends the active
        interval, so a delayed loop cannot manufacture stability evidence.
        """

        if self._close_stability_trade_key != trade_key:
            self._reset_close_zero_wear_stability(trade_key)
        last_ms = self._close_zero_wear_last_sample_ms
        if last_ms is not None and now_ms < last_ms:
            self._reset_close_zero_wear_stability(trade_key)
            last_ms = None

        gap_ms = now_ms - last_ms if last_ms is not None else None
        previous_interval_ended = (
            self._close_zero_wear_last_above
            and (
                not above_zero_wear
                or gap_ms is None
                or gap_ms > CLOSE_ZERO_WEAR_MAX_SAMPLE_GAP_MS
            )
        )
        if previous_interval_ended:
            started_ms = self._close_zero_wear_started_ms
            if started_ms is not None and last_ms is not None and last_ms > started_ms:
                self._close_zero_wear_intervals.append((started_ms, last_ms))
            self._close_zero_wear_started_ms = None

        if above_zero_wear and self._close_zero_wear_started_ms is None:
            self._close_zero_wear_started_ms = now_ms

        self._close_zero_wear_last_sample_ms = now_ms
        self._close_zero_wear_last_above = above_zero_wear

        cutoff_ms = now_ms - CLOSE_ZERO_WEAR_ACCUMULATION_WINDOW_MS
        while (
            self._close_zero_wear_intervals
            and self._close_zero_wear_intervals[0][1] <= cutoff_ms
        ):
            self._close_zero_wear_intervals.popleft()
        accumulated_ms = sum(
            max(0, end_ms - max(start_ms, cutoff_ms))
            for start_ms, end_ms in self._close_zero_wear_intervals
        )
        continuous_ms = 0
        if above_zero_wear and self._close_zero_wear_started_ms is not None:
            continuous_ms = max(0, now_ms - self._close_zero_wear_started_ms)
            accumulated_ms += max(
                0,
                now_ms - max(self._close_zero_wear_started_ms, cutoff_ms),
            )
        confirmed = above_zero_wear and (
            continuous_ms >= CLOSE_ZERO_WEAR_STABILITY_MS
            or accumulated_ms >= CLOSE_ZERO_WEAR_STABILITY_MS
        )
        return confirmed, continuous_ms, accumulated_ms

    async def _auto_var_close_signal_for_current_open(
        self,
        current_open: OrderLifecycle,
    ) -> tuple[str, Decimal, Decimal, Decimal, CloseCandidate] | None:
        if not self.automation_can_submit_var_order(
            "last_auto_var_close_status",
            allow_reconcile_degraded=True,
        ):
            return None
        hedge_age = record_hold_seconds(current_open)
        if current_open.hedge_status == "error" or current_open.hedge_error:
            self.pause_for_reconciliation(
                f"Lighter hedge failed: "
                f"{current_open.hedge_error or current_open.hedge_status}"
            )
            return None
        if current_open.lighter_fill_price is None:
            if hedge_age is not None and hedge_age > LIGHTER_FILL_TIMEOUT_SECONDS:
                self.pause_for_reconciliation(
                    "Lighter hedge failed: "
                    f"no fill after {LIGHTER_FILL_TIMEOUT_SECONDS:.0f}s"
                )
            else:
                self.last_auto_var_close_status = "waiting Lighter fill"
            return None
        lighter_filled_qty = current_open.lighter_filled_qty
        lighter_target = lighter_order_target_qty(
            current_open,
            self.base_amount_multiplier,
        )
        if lighter_target is None:
            reason = (
                "invalid Lighter quantity precision; automatic close fails closed "
                f"({self.base_amount_multiplier}/{current_open.qty})"
            )
            self.last_auto_var_close_status = reason
            self.pause_automation(reason)
            return None
        if (
            current_open.hedge_status != "filled"
            or lighter_filled_qty is None
            or lighter_filled_qty != lighter_target
        ):
            self.last_auto_var_close_status = (
                "Lighter quantity mismatch: "
                f"filled {lighter_filled_qty or Decimal('0')} / expected {lighter_target} "
                f"(Var {current_open.qty})"
            )
            return None
        if current_open.strategy_tag == MANUAL_STRATEGY_TAG:
            self.last_auto_var_close_status = "manual position: strategy close disabled"
            return None
        if current_open.strategy_tag != ADAPTIVE_MODEL_VERSION:
            self.pause_automation("Unknown strategy tag; automatic close fails closed")
            return None
        frozen_open = open_candidate_from_payload(current_open.adaptive_strategy_context)
        opened_at = parse_iso_datetime(current_open.var_fill_ts_iso)
        open_notional = current_open.open_notional_usd or var_open_notional_usd(current_open)
        open_pnl = leg_result_by_direction(current_open).pnl
        close_qty = normalize_var_base_qty(current_open.qty)
        if (
            frozen_open is None
            or opened_at is None
            or open_notional is None
            or open_pnl is None
            or close_qty is None
        ):
            self.pause_automation("Adaptive close is missing frozen or actual fill context")
            return None
        close_side = StrategySide(current_open.side.strip().upper()).opposite
        try:
            frame, _observation = await self.current_adaptive_market_frame(
                exact_actual_side=close_side,
                exact_actual_base_qty=close_qty,
            )
        except Exception as exc:
            self._update_close_zero_wear_stability(
                trade_key=current_open.trade_key,
                above_zero_wear=False,
                now_ms=time.time_ns() // 1_000_000,
            )
            self.last_auto_var_close_status = f"PAUSE: exact_close_frame_error ({exc})"
            return None
        if frame is None:
            self._update_close_zero_wear_stability(
                trade_key=current_open.trade_key,
                above_zero_wear=False,
                now_ms=time.time_ns() // 1_000_000,
            )
            self.last_auto_var_close_status = "PAUSE: exact_close_market_frame_unavailable"
            return None
        now_ms = time.time_ns() // 1_000_000
        position = PositionContext(
            strategy_tag=ADAPTIVE_MODEL_VERSION,
            open_direction=StrategySide(current_open.side.strip().upper()),
            opened_at_ms=int(opened_at.timestamp() * 1_000),
            actual_base_qty=close_qty,
            actual_notional_usd=open_notional,
            actual_open_pnl_usd=open_pnl,
            epoch=frozen_open.epoch,
        )
        decision = self.strategy_engine.evaluate_close(
            frame=frame,
            position=position,
            now_ms=now_ms,
        )
        close_candidate = decision.close_candidate
        if close_candidate is not None:
            gross_round_pnl = open_pnl + close_candidate.expected_close_pnl_usd
            stability_passed, continuous_ms, accumulated_ms = (
                self._update_close_zero_wear_stability(
                    trade_key=current_open.trade_key,
                    above_zero_wear=gross_round_pnl > Decimal("0"),
                    now_ms=now_ms,
                )
            )
            close_candidate = replace(
                close_candidate,
                zero_wear_continuous_ms=continuous_ms,
                zero_wear_accumulated_ms=accumulated_ms,
            )
            if (
                decision.action is not StrategyAction.CLOSE
                and gross_round_pnl > Decimal("0")
                and stability_passed
            ):
                close_candidate = replace(
                    close_candidate,
                    zero_wear_stability_passed=True,
                )
                decision = StrategyDecision(
                    StrategyAction.CLOSE,
                    "close_zero_wear_stability_passed",
                    close_candidate=close_candidate,
                )
            elif (
                decision.action is not StrategyAction.CLOSE
                and gross_round_pnl > Decimal("0")
            ):
                decision = StrategyDecision(
                    StrategyAction.NO_ACTION,
                    "close_zero_wear_stability_pending",
                    close_candidate=close_candidate,
                )
            else:
                decision = StrategyDecision(
                    decision.action,
                    decision.reason,
                    close_candidate=close_candidate,
                )
        if (
            frozen_open.epoch.model_version == "adaptive-median-v5"
            and decision.action is StrategyAction.CLOSE
            and close_candidate is not None
            and close_candidate.held_seconds < self.strategy_engine.early_exit_seconds
        ):
            close_rate_range = self.recent_directional_rate_range(
                close_side,
                now_ms=now_ms,
                current_rate=frame.reference_rates.for_side(close_side),
            )
            maximum_range = V5_CLOSE_RATE_RANGE_BPS / Decimal("10000")
            if close_rate_range is None or close_rate_range > maximum_range:
                started_ms = self._close_range_deferral_started_ms.setdefault(
                    current_open.trade_key,
                    now_ms,
                )
                if now_ms - started_ms < V5_CLOSE_RANGE_MAX_DEFERRAL_MS:
                    decision = StrategyDecision(
                        StrategyAction.NO_ACTION,
                        "close_five_second_rate_range_deferral",
                        close_candidate=close_candidate,
                    )
                else:
                    self._close_range_deferral_started_ms.pop(
                        current_open.trade_key,
                        None,
                    )
            else:
                self._close_range_deferral_started_ms.pop(
                    current_open.trade_key,
                    None,
                )
        else:
            self._close_range_deferral_started_ms.pop(current_open.trade_key, None)
        self.last_strategy_decision = decision
        self.last_strategy_decision_at_ms = now_ms
        if close_candidate is not None and close_candidate.max_hold_alert:
            if current_open.trade_key not in self._max_hold_alerted_trade_keys:
                self._max_hold_alerted_trade_keys.add(current_open.trade_key)
                self.logger.error(
                    "Maximum hold exceeded for %s; controlled floor remains active",
                    current_open.trade_key,
                )
        if decision.action is not StrategyAction.CLOSE or close_candidate is None:
            self.last_auto_var_close_status = f"{decision.action.value}: {decision.reason}"
            return None
        return (
            close_candidate.close_direction.value,
            close_candidate.round_lower_bound_usd,
            open_notional,
            close_qty,
            close_candidate,
        )

    async def _evaluate_auto_close_once(self, current_open: OrderLifecycle | None) -> None:
        if self._auto_var_order_inflight:
            return
        if time.time() - self._last_auto_var_order_at < AUTO_VAR_ORDER_COOLDOWN_SECONDS:
            return
        if not self.automation_can_submit_var_order(
            "last_auto_var_close_status",
            allow_reconcile_degraded=True,
        ):
            return
        if not await self.runtime.command_broker.extension_connected():
            self.last_auto_var_close_status = "command disconnected"
            return

        if current_open is None:
            return
        signal = await self._auto_var_close_signal_for_current_open(current_open)
        if signal is None or not self.automation_can_submit_var_order(
            "last_auto_var_close_status",
            allow_reconcile_degraded=True,
        ):
            return

        side, expected_pnl, close_notional, close_qty, close_candidate = signal
        self._auto_var_order_inflight = True
        self._last_auto_var_order_at = time.time()
        self.last_auto_var_close_status = f"sending {side} {self._fmt_notional(close_notional)}"
        expected_intent = await self.mark_and_persist_var_intent(
            "close",
            side,
            close_notional,
        )
        try:
            result = await self.request_guarded_var_order(
                phase="close",
                side=side,
                amount=close_notional,
                base_qty=close_qty,
                close_candidate=close_candidate,
                expected_intent=expected_intent,
            )
            committed_record = None
            if result.get("ok"):
                committed_record = await self.start_committed_var_hedge(
                    phase="close", side=side, result=result
                )
            await self.append_auto_var_result_log(
                phase="close",
                side=side,
                amount=close_notional,
                base_qty=close_qty,
                expected_pnl=expected_pnl,
                result=result,
            )
            if result.get("ok"):
                firm_estimate = firm_guard_pnl_from_result(result) or expected_pnl
                self.last_auto_var_close_status = (
                    f"已提交 {side}，对冲中，firm估算 {self._fmt_colored_money(firm_estimate)}"
                )
                self.logger.info("Auto Var close accepted: %s", result)
            else:
                commit_is_uncertain = (
                    var_result_is_ambiguous(result)
                    and var_intent_crossed_commit_boundary(expected_intent)
                )
                if commit_is_uncertain:
                    self.pause_automation(f"Ambiguous Var close result: {result.get('error') or 'unknown'}")
                else:
                    self.clear_matching_var_intent(
                        side,
                        force=True,
                        expected_intent=expected_intent,
                    )
                error = result.get("error") or "unknown"
                prefix = "跳过" if str(error).startswith("fresh firm quote rejected") else "failed"
                self.last_auto_var_close_status = f"{prefix}: {error}"
                self.logger.warning("Auto Var close failed: %s", result)
            if not result.get("ok") or committed_record is None:
                await self.persist_runtime_state()
        except Exception as exc:
            if var_intent_crossed_commit_boundary(expected_intent):
                self.pause_automation(f"Ambiguous Var close exception: {exc}")
            else:
                self.clear_matching_var_intent(
                    side,
                    force=True,
                    expected_intent=expected_intent,
                )
                await self.persist_runtime_state()
            self.last_auto_var_close_status = f"failed: {exc}"
            self.logger.warning("Auto Var close exception: %s", exc)
        finally:
            self._auto_var_order_inflight = False

    async def _evaluate_auto_open_once(self, current_open: OrderLifecycle | None) -> None:
        config = self.strategy_config
        if current_open is not None or self._auto_var_order_inflight:
            return
        if time.time() - self._last_auto_var_order_at < AUTO_VAR_ORDER_COOLDOWN_SECONDS:
            return
        cooldown_remaining = self.round_cooldown_remaining_seconds()
        if cooldown_remaining > 0:
            self.last_auto_var_order_status = f"cooldown {self._fmt_duration(cooldown_remaining)}"
            return
        self._selected_open_candidate = None
        signal = await self._auto_var_signal_for_current_open(current_open)
        if signal is None:
            return
        side, expected_pnl = signal
        candidate = self._selected_open_candidate
        if candidate is None or candidate.direction.value != side:
            self.last_auto_var_order_status = "frozen candidate mismatch"
            return
        live_block = self.live_open_block_reason()
        if live_block is not None:
            self.last_auto_var_order_status = f"live blocked: {live_block}"
            return
        if not self.automation_can_submit_var_order("last_auto_var_order_status"):
            return
        if not await self.runtime.command_broker.extension_connected():
            self.last_auto_var_order_status = "command disconnected"
            return
        self._auto_var_order_inflight = True
        self._last_auto_var_order_at = time.time()
        self.last_auto_var_order_status = f"sending {side} {self._fmt_notional(config.order_notional_usd)}"
        expected_intent = await self.mark_and_persist_var_intent(
            "open",
            side,
            config.order_notional_usd,
        )
        try:
            result = await self.request_guarded_var_order(
                phase="open",
                side=side,
                amount=config.order_notional_usd,
                base_qty=None,
                open_candidate=candidate,
                expected_intent=expected_intent,
            )
            committed_record = None
            if result.get("ok"):
                committed_record = await self.start_committed_var_hedge(
                    phase="open", side=side, result=result
                )
            await self.append_auto_var_result_log(
                phase="open",
                side=side,
                amount=config.order_notional_usd,
                expected_pnl=expected_pnl,
                result=result,
            )
            if result.get("ok"):
                firm_estimate = firm_guard_pnl_from_result(result) or expected_pnl
                self.last_auto_var_order_status = (
                    f"已提交 {side}，对冲中，firm估算 {self._fmt_colored_money(firm_estimate)}"
                )
                self.logger.info("Auto Var order accepted: %s", result)
            else:
                commit_is_uncertain = (
                    var_result_is_ambiguous(result)
                    and var_intent_crossed_commit_boundary(expected_intent)
                )
                if commit_is_uncertain:
                    self.pause_automation(f"Ambiguous Var open result: {result.get('error') or 'unknown'}")
                else:
                    self.clear_matching_var_intent(
                        side,
                        force=True,
                        expected_intent=expected_intent,
                    )
                error = result.get("error") or "unknown"
                prefix = "跳过" if str(error).startswith("fresh firm quote rejected") else "failed"
                self.last_auto_var_order_status = f"{prefix}: {error}"
                self.logger.warning("Auto Var order failed: %s", result)
            if not result.get("ok") or committed_record is None:
                await self.persist_runtime_state()
        except Exception as exc:
            if var_intent_crossed_commit_boundary(expected_intent):
                self.pause_automation(f"Ambiguous Var open exception: {exc}")
            else:
                self.clear_matching_var_intent(
                    side,
                    force=True,
                    expected_intent=expected_intent,
                )
                await self.persist_runtime_state()
            self.last_auto_var_order_status = f"failed: {exc}"
            self.logger.warning("Auto Var open exception: %s", exc)
        finally:
            self._auto_var_order_inflight = False

    async def strategy_signal_loop(self) -> None:
        """Run exactly one strategy branch for every coalesced market batch."""
        signal_revision = self._market_signal_revision
        trade_signal_revision = self._trade_signal_revision
        while not self.stop_flag:
            signal_revision = await self.wait_for_market_signal(signal_revision)
            if self._trade_signal_revision != trade_signal_revision:
                await self.drain_pending_trade_events()
                trade_signal_revision = self._trade_signal_revision
            try:
                await self.refresh_adaptive_market_frame_for_decision()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_market_frame = None
                self._invalidate_adaptive_parameters("strategy_frame_adapter_exception")
                self.logger.warning("Adaptive decision frame skipped: %s", exc)
            current_open = await self._current_open_record()
            if current_open is None:
                await self._evaluate_auto_open_once(None)
            else:
                await self._evaluate_auto_close_once(current_open)

    def should_track_variational_event(self, event: dict[str, Any]) -> bool:
        side = str(event.get("side", "")).strip().lower()
        if side not in {"buy", "sell"}:
            return False

        qty = to_decimal(event.get("qty"))
        if qty is None or qty <= 0:
            return False

        asset = str(event.get("asset", "")).strip().upper()
        if not asset:
            return False
        return asset in self.accepted_assets

    async def process_variational_trade_event(self, event: dict[str, Any]) -> None:
        if not self.should_track_variational_event(event):
            return

        exchange_event_time = parse_iso_datetime(str(event.get("timestamp") or ""))
        if (
            self.var_event_accept_after is not None
            and exchange_event_time is not None
            and exchange_event_time < self.var_event_accept_after - timedelta(seconds=2)
        ):
            # Event WebSockets may replay the most recent trade when Chrome or
            # the extension reconnects.  The wrapper capture timestamp is new,
            # but the exchange trade timestamp is not.  Never let that replay
            # create a fresh hedge.
            await self.append_order_log("historical_variational_event_ignored", event)
            return

        captured_at = parse_iso_datetime(str(event.get("captured_at") or ""))
        if captured_at is not None:
            event_age = (datetime.now(timezone.utc) - captured_at).total_seconds()
            if event_age > MAX_FORWARDED_EVENT_AGE_SECONDS:
                self.pause_automation(f"Dropped stale Var trade event ({event_age:.1f}s old); reconcile accounts")
                await self.append_order_log("stale_variational_event", event)
                return

        raw_event_seq = event.get("event_seq")
        event_seq = (
            None
            if isinstance(raw_event_seq, bool)
            else to_int(raw_event_seq)
        )
        if not str(event.get("trade_id") or "").strip() and (
            event_seq is None or event_seq <= 0
        ):
            reason = (
                "Var trade event has no stable trade_id/event_seq; automatic "
                "hedging is unsafe and account reconciliation is required"
            )
            self.pause_for_reconciliation(reason)
            await self.append_order_log("unidentified_variational_event", event)
            return

        key = self.trade_key(event)
        side = str(event.get("side", "")).strip().lower()
        qty = to_decimal(event.get("qty"))
        if qty is None:
            return

        status = normalize_variational_status(str(event.get("status", "")))
        asset = str(event.get("asset", "")).strip().upper() or self.variational_ticker
        trade_id = str(event.get("trade_id", "")).strip()
        source_rfq = variational_event_source_id(event, "source_rfq")
        source_quote = variational_event_source_id(event, "source_quote")

        now_iso = utc_now()
        fill_iso = str(event.get("timestamp") or now_iso)
        cancelled_precommit_intent = False

        # Exchange timestamps reject historical replays above.  A remaining
        # fresh fill can be correlated to an automatic intent, update an
        # existing trade, or represent a live manual action.  Manual actions
        # are only safe to automate when they are a complete open from flat or
        # an exact close of the currently tracked position.
        async with self._record_lock:
            pending_for_event = self.pending_var_intent
            pending_matches_event = (
                pending_for_event is not None
                and self.var_event_matches_intent(pending_for_event, event)
            )
            pending_waits_for_commit = bool(
                pending_matches_event
                and pending_for_event is not None
                and pending_for_event.state == VAR_INTENT_COMMITTING
                and pending_for_event.commit_accepted_monotonic is None
            )
            pending_is_precommit = bool(
                pending_for_event is not None
                and pending_for_event.state
                in {VAR_INTENT_QUOTING, VAR_INTENT_PREPARED}
            )
            pending_blocks_manual_classification = bool(
                pending_for_event is not None and not pending_is_precommit
            )
            known_event = key in self.records or (
                bool(trade_id)
                and any(
                    candidate.trade_id == trade_id
                    and candidate.asset == asset
                    and candidate.side == side
                    for candidate in self.records.values()
                )
            )
            ordered_records = [
                self.records[record_key]
                for record_key in self.record_order
                if record_key in self.records
            ]
            event_time = parse_iso_datetime(fill_iso) or datetime.now(timezone.utc)
            is_late_portfolio_fill = any(
                candidate.var_fill_source == "portfolio"
                and candidate.asset == asset
                and candidate.side == side
                and abs(candidate.qty - qty) <= VAR_POSITION_TOLERANCE
                and (candidate_time := parse_iso_datetime(candidate.var_fill_ts_iso))
                is not None
                and abs((event_time - candidate_time).total_seconds())
                <= RECOVERED_FILL_DEDUP_SECONDS
                for candidate in ordered_records
            )
            known_event = known_event or is_late_portfolio_fill
            current_open_for_event, _ = build_trade_rounds(ordered_records)
            is_live_manual_close = (
                status == "filled"
                and self.args.auto_hedge
                and not pending_blocks_manual_classification
                and current_open_for_event is not None
                and current_open_for_event.asset == asset
                and current_open_for_event.side != side
                and abs(current_open_for_event.qty - qty) <= VAR_POSITION_TOLERANCE
            )
            is_live_manual_open = (
                status == "filled"
                and self.args.auto_hedge
                and not pending_blocks_manual_classification
                and current_open_for_event is None
                and not known_event
                and not event.get("recovered_from_portfolio")
            )
            is_unsupported_manual_change = (
                status == "filled"
                and not event.get("recovered_from_portfolio")
                and not known_event
                and not pending_matches_event
                and not is_live_manual_open
                and not is_live_manual_close
            )
            if (
                pending_is_precommit
                and (is_live_manual_open or is_live_manual_close)
                and self.pending_var_intent is pending_for_event
            ):
                # Quote/PREPARED has not crossed the Commit boundary.  Cancel
                # that exact candidate and protect the real manual fill.  A
                # concurrently returning quote will fail its expected-intent
                # identity check and therefore cannot Commit afterwards.
                self.pending_var_intent = None
                cancelled_precommit_intent = True
        if is_unsupported_manual_change:
            if pending_for_event is not None:
                reason = (
                    "Manual Var fill arrived while an automatic intent was active; "
                    "order relationship is ambiguous and requires reconciliation"
                )
            elif current_open_for_event is None:
                reason = "Manual Var fill could not be classified; account reconciliation required"
            elif current_open_for_event.asset != asset:
                reason = "Manual Var fill changed a different market; account reconciliation required"
            elif current_open_for_event.side == side:
                reason = "Manual Var add-on is not auto-hedged; account reconciliation required"
            elif qty < current_open_for_event.qty - VAR_POSITION_TOLERANCE:
                reason = "Manual Var partial close is not auto-hedged; account reconciliation required"
            else:
                reason = "Manual Var reversal is not auto-hedged; account reconciliation required"

            recovery_record = OrderLifecycle(
                trade_key=key,
                trade_id=trade_id,
                side=side,
                qty=qty,
                asset=asset if asset else "UNKNOWN",
                auto_hedge_enabled=self.args.auto_hedge,
                last_variational_status=status,
                var_fill_price=to_decimal(event.get("price")),
                var_fill_ts_iso=fill_iso,
                var_fill_source="event",
                var_event_origin=VarEventOrigin.MANUAL_LIVE.value,
                var_source_rfq=source_rfq,
                var_source_quote=source_quote,
                strategy_phase="manual_recovery",
                hedge_status="recovery_required",
                hedge_error=reason,
                execution_state=EXECUTION_STATE_RECOVERY_REQUIRED,
            )
            async with self._record_lock:
                if key not in self.records:
                    self.records[key] = recovery_record
                    self.record_order.append(key)
                    recovery_payload = recovery_record.to_payload()
                else:
                    recovery_payload = None
            if recovery_payload is not None:
                await self.append_order_log(
                    "manual_variational_change_requires_recovery",
                    recovery_payload,
                )
                await self.persist_runtime_state()
            self.pause_for_reconciliation(reason)
            return
        if (
            self.var_event_accept_after is not None
            and self.args.auto_hedge
            and not event.get("recovered_from_portfolio")
            and not known_event
            and not pending_matches_event
            and not is_live_manual_open
            and not is_live_manual_close
        ):
            reason = (
                "Ignored uncorrelated Var trade; no Lighter order sent. "
                "Account reconciliation required"
            )
            self.pause_for_reconciliation(reason)
            await self.append_order_log("uncorrelated_variational_event_ignored", event)
            return

        created = False
        filled_record: OrderLifecycle | None = None
        qty_topup_record: OrderLifecycle | None = None
        qty_correction_record: OrderLifecycle | None = None
        matching_open_before_create: OrderLifecycle | None = None
        recovered_match = False
        recovered_prepared_hedge = False

        async with self._record_lock:
            provisional_key: str | None = None
            pending_intent = self.pending_var_intent
            if (
                status == "filled"
                and pending_intent is not None
                and self.var_event_matches_intent(pending_intent, event)
                and pending_intent.provisional_trade_key
                and pending_intent.side == side.upper()
                and (not pending_intent.market or pending_intent.market == asset)
            ):
                provisional_key = pending_intent.provisional_trade_key
            ordered_keys_before = list(self.record_order)
            ordered_records_before = [
                self.records[record_key]
                for record_key in ordered_keys_before
                if record_key in self.records
            ]
            current_open_before, _ = build_trade_rounds(ordered_records_before)
            if current_open_before is not None and current_open_before.side != side:
                matching_open_before_create = current_open_before

            record = self.records.get(key)
            if record is None and provisional_key is not None:
                record = self.records.get(provisional_key)
                if record is not None:
                    key = provisional_key
            if record is None and trade_id:
                for candidate_key in reversed(ordered_keys_before):
                    candidate = self.records.get(candidate_key)
                    if (
                        candidate is not None
                        and candidate.var_fill_source not in {"unconfirmed_commit", "lighter_rollback"}
                        and candidate.trade_id == trade_id
                        and candidate.asset == asset
                        and candidate.side == side
                    ):
                        record = candidate
                        key = candidate_key
                        break
            if record is None and status == "filled" and not event.get("recovered_from_portfolio"):
                event_time = parse_iso_datetime(fill_iso) or datetime.now(timezone.utc)
                for candidate_key in reversed(ordered_keys_before):
                    candidate = self.records.get(candidate_key)
                    if candidate is None or candidate.var_fill_source != "portfolio":
                        continue
                    candidate_time = parse_iso_datetime(candidate.var_fill_ts_iso)
                    if (
                        candidate.asset == asset
                        and candidate.side == side
                        and abs(candidate.qty - qty) <= VAR_POSITION_TOLERANCE
                        and candidate_time is not None
                        and abs((event_time - candidate_time).total_seconds()) <= RECOVERED_FILL_DEDUP_SECONDS
                    ):
                        record = candidate
                        key = candidate.trade_key
                        recovered_match = True
                        break
            if record is None:
                record = OrderLifecycle(
                    trade_key=key,
                    trade_id=trade_id,
                    side=side,
                    qty=qty,
                    asset=asset if asset else "UNKNOWN",
                    auto_hedge_enabled=self.args.auto_hedge,
                    last_variational_status=status,
                    var_event_origin=(
                        VarEventOrigin.PORTFOLIO_RECOVERY.value
                        if event.get("recovered_from_portfolio")
                        else (
                            VarEventOrigin.AUTO_INTENT.value
                            if pending_intent is not None
                            and self.var_event_matches_intent(pending_intent, event)
                            else (
                                VarEventOrigin.MANUAL_LIVE.value
                                if is_live_manual_open or is_live_manual_close
                                else VarEventOrigin.UNKNOWN.value
                            )
                        )
                    ),
                    var_source_rfq=source_rfq,
                    var_source_quote=source_quote,
                )
                self.records[key] = record
                self.record_order.append(key)
                created = True
                if matching_open_before_create is not None:
                    record.lighter_reduce_only = True
                if is_live_manual_open:
                    record.strategy_phase = "open"
                    record.strategy_tag = MANUAL_STRATEGY_TAG
                elif is_live_manual_close:
                    record.strategy_phase = "close"
            else:
                previous_status = record.last_variational_status
                record.last_variational_status = status

            if (
                status == "filled"
                and pending_intent is not None
                and self.var_event_matches_intent(pending_intent, event)
                and pending_intent.side == side.upper()
                and (not pending_intent.market or pending_intent.market == asset)
            ):
                self._apply_intent_metadata_locked(record, pending_intent)
                record.var_event_origin = VarEventOrigin.AUTO_INTENT.value
                record.var_source_rfq = source_rfq or record.var_source_rfq
                record.var_source_quote = source_quote or record.var_source_quote
                if (
                    pending_intent.state == VAR_INTENT_COMMITTING
                    and pending_intent.commit_accepted_monotonic is None
                ):
                    pending_intent.confirmed_trade_key = record.trade_key
                    record.hedge_status = "waiting_commit"
                    record.execution_state = VAR_INTENT_COMMITTING
                if (
                    event.get("recovered_from_portfolio")
                    and pending_intent.phase
                    not in {"emergency_close", "operator_var_only_close"}
                    and record.lighter_reserved_client_order_id is not None
                ):
                    if not self._terminal_lighter_hedge_is_complete_locked(record):
                        record.hedge_status = "recovery_check"
                        record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                        record.hedge_error = "Checking deterministic Lighter order before recovery hedge"
                        self.lighter_client_order_to_trade_key[
                            record.lighter_reserved_client_order_id
                        ] = record.trade_key
                        recovered_prepared_hedge = True
                    else:
                        self._normalize_terminal_lighter_hedge_locked(record)

            if created:
                previous_status = ""

            should_set_fill = False
            if status == "filled":
                if recovered_match or record.var_fill_ts_iso is None:
                    should_set_fill = True
                elif previous_status != "filled":
                    should_set_fill = True

            if should_set_fill:
                was_http_commit = record.var_fill_source == "http_commit"
                record.qty = qty
                record.side = side
                record.asset = asset if asset else record.asset
                record.var_fill_ts_iso = fill_iso
                record.var_fill_price = to_decimal(event.get("price"))
                record.var_fill_source = (
                    "portfolio" if event.get("recovered_from_portfolio") else "event"
                )
                record.var_source_rfq = source_rfq or record.var_source_rfq
                record.var_source_quote = source_quote or record.var_source_quote
                if (
                    pending_intent is not None
                    and pending_intent.side == side.upper()
                    and (not pending_intent.market or pending_intent.market == asset)
                ):
                    record.strategy_phase = record.strategy_phase or pending_intent.phase
                if trade_id:
                    record.trade_id = trade_id
                if matching_open_before_create is not None:
                    record.lighter_reduce_only = True
                if (
                    record.strategy_phase == "open"
                    and not record.lighter_reduce_only
                    and record.var_fill_price is not None
                    and record.var_fill_price > 0
                ):
                    record.open_notional_usd = qty * record.var_fill_price
                if (
                    was_http_commit
                    and record.hedge_status == "filled"
                    and record.lighter_filled_qty is not None
                ):
                    lighter_target = lighter_order_target_qty(
                        record,
                        self.base_amount_multiplier,
                    )
                    filled_lighter_qty = record.lighter_filled_qty
                    if lighter_target is None:
                        record.hedge_status = "error"
                        record.hedge_error = (
                            "Invalid Lighter quantity precision after Var fill"
                        )
                    elif filled_lighter_qty < lighter_target:
                        record.hedge_status = "retrying"
                        record.hedge_error = (
                            f"Var actual qty {qty} exceeds early Lighter hedge "
                            f"{filled_lighter_qty}; topping up to {lighter_target}"
                        )
                        qty_topup_record = record
                    elif filled_lighter_qty > lighter_target:
                        qty_correction_record = self._build_lighter_qty_correction_locked(
                            record,
                            filled_lighter_qty - lighter_target,
                        )
                if qty_correction_record is None:
                    self._capture_execution_loss_locked(record)
                filled_payload = record.to_payload()
                filled_record = record
            else:
                filled_payload = None

        if filled_payload is not None:
            cleared_intent = False
            if not pending_waits_for_commit:
                cleared_intent = self.clear_matching_var_intent(side, event=event)
            if (
                not pending_waits_for_commit
                and not cleared_intent
                and self.pending_var_intent is not None
            ):
                intent = self.pending_var_intent
                expected_side = intent.side
                expected_market = intent.market
                self.pending_var_intent = None
                if expected_side != side.upper() or (expected_market and expected_market != asset):
                    self.pause_for_reconciliation(
                        f"Account reconciliation required: Var fill direction/market mismatch: "
                        f"expected {expected_market or '-'} "
                        f"{expected_side}, got {asset} {side.upper()}; "
                        "hedging the confirmed fill"
                    )
                else:
                    self.pause_for_reconciliation(
                        "Account reconciliation required: Var fill matched the pending side/market "
                        "but not its exact order metadata; hedging the confirmed fill"
                    )
            if qty_topup_record is not None:
                self.queue_lighter_retry_after_current(qty_topup_record)
            if qty_correction_record is not None:
                self.schedule_lighter_order(qty_correction_record)
            if cancelled_precommit_intent:
                await self.append_order_log(
                    "precommit_intent_cancelled_by_manual_fill",
                    {
                        "trade_key": filled_record.trade_key if filled_record else key,
                        "side": side,
                        "asset": asset,
                    },
                )
            await self.append_order_log("variational_fill", filled_payload)
            if recovered_prepared_hedge:
                await self.append_order_log("lighter_recovery_check", filled_payload)
            if qty_correction_record is not None:
                await self.append_order_log(
                    "lighter_qty_correction_queued",
                    qty_correction_record.to_payload(),
                )
            if filled_record is not None:
                await self.record_completed_canary_round_for_leg(filled_record)
            await self.persist_runtime_state()

        if (
            filled_record is not None
            and self.args.auto_hedge
            and filled_record.auto_hedge_enabled
            and filled_record.hedge_status == "error"
            and not filled_record.lighter_reduce_only
        ):
            await self.emergency_flatten_var(filled_record)
            return

        if (
            filled_record is not None
            and self.args.auto_hedge
            and filled_record.auto_hedge_enabled
            and filled_record.hedge_status == "not_started"
        ):
            if (
                matching_open_before_create is not None
                and not lighter_hedge_filled(matching_open_before_create)
            ):
                async with self._record_lock:
                    open_hedge_may_still_fill = lighter_order_may_still_fill(
                        matching_open_before_create
                    )
                    partial_open_hedge_qty = (
                        matching_open_before_create.lighter_filled_qty
                        or Decimal("0")
                    )
                    open_hedge_status = matching_open_before_create.hedge_status
                if open_hedge_may_still_fill:
                    reason = (
                        "Waiting Lighter close hedge: matching open Lighter "
                        "hedge may still fill."
                    )
                    async with self._record_lock:
                        filled_record.hedge_status = "waiting_open_hedge"
                        filled_record.hedge_error = reason
                        payload = filled_record.to_payload()
                    await self.append_order_log("lighter_waiting_close", payload)
                    self.pause_automation(reason)
                    await self.persist_runtime_state()
                    return
                if partial_open_hedge_qty > 0:
                    executable_partial_qty = lighter_hedge_target_qty(
                        partial_open_hedge_qty,
                        self.base_amount_multiplier,
                    )
                    if executable_partial_qty != partial_open_hedge_qty:
                        reason = (
                            "Protective Lighter close refused: confirmed partial fill "
                            "is not an exact executable Lighter quantity."
                        )
                        async with self._record_lock:
                            filled_record.hedge_status = "recovery_required"
                            filled_record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                            filled_record.hedge_error = reason
                            payload = filled_record.to_payload()
                        await self.append_order_log("lighter_protective_close_refused", payload)
                        self.pause_automation(reason)
                        await self.persist_runtime_state()
                        return
                    reason = (
                        "Protective Lighter close hedge: matching open Lighter "
                        f"hedge filled only {partial_open_hedge_qty}; closing that exact residual."
                    )
                    async with self._record_lock:
                        filled_record.lighter_target_qty_override = partial_open_hedge_qty
                        filled_record.hedge_status = "protective_close"
                        filled_record.hedge_error = reason
                        payload = filled_record.to_payload()
                    await self.append_order_log("lighter_protective_close", payload)
                    self.pause_automation(reason)
                    self.schedule_lighter_order(filled_record)
                    await self.persist_runtime_state()
                    return
                if open_hedge_status in {"submitted", "uncertain", "partial"}:
                    reason = (
                        "Protective Lighter close not submitted: prior hedge outcome "
                        "is unresolved and no confirmed filled quantity is available."
                    )
                    async with self._record_lock:
                        filled_record.hedge_status = "recovery_required"
                        filled_record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                        filled_record.hedge_error = reason
                        payload = filled_record.to_payload()
                    await self.append_order_log("lighter_protective_close_deferred", payload)
                    self.pause_automation(reason)
                    await self.persist_runtime_state()
                    return
                reason = "Skipped Lighter close hedge: matching open Lighter hedge was not filled."
                async with self._record_lock:
                    filled_record.hedge_status = "skipped"
                    filled_record.hedge_error = reason
                    payload = filled_record.to_payload()
                await self.append_order_log("lighter_skip", payload)
                self.pause_automation(reason)
                await self.persist_runtime_state()
                return
            self.schedule_lighter_order(filled_record)
            await self.persist_runtime_state()

    async def drain_pending_trade_events(self) -> int:
        async with self._trade_event_drain_lock:
            processed = 0
            while True:
                events = await self.runtime.monitor.get_trade_events_since(
                    self.trade_event_cursor,
                    limit=500,
                )
                if not events:
                    return processed
                for event in events:
                    event_seq = int(event.get("event_seq", 0) or 0)
                    if event_seq <= self.trade_event_cursor:
                        continue
                    await self.process_variational_trade_event(event)
                    self.trade_event_cursor = event_seq
                    processed += 1
                if len(events) < 500:
                    return processed

    async def trade_loop(self) -> None:
        signal_revision = self._market_signal_revision
        while not self.stop_flag:
            signal_revision = await self.wait_for_market_signal(signal_revision)
            current_asset = await self.detect_current_variational_asset()
            if current_asset:
                if current_asset == self.variational_ticker:
                    self._asset_switch_candidate = None
                    self._asset_switch_candidate_hits = 0
                else:
                    if current_asset == self._asset_switch_candidate:
                        self._asset_switch_candidate_hits += 1
                    else:
                        self._asset_switch_candidate = current_asset
                        self._asset_switch_candidate_hits = 1

                    if self._asset_switch_candidate_hits >= ASSET_SWITCH_CONFIRM_TICKS:
                        await self.activate_asset(current_asset, reason="quote_stream_debounced")
                        self._asset_switch_candidate = None
                        self._asset_switch_candidate_hits = 0
            else:
                self._asset_switch_candidate = None
                self._asset_switch_candidate_hits = 0
            await self.drain_pending_trade_events()

    def _fmt_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return format(value, "f")

    def _fmt_fill_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.2f}"

    @staticmethod
    def _direction_labels(side: str) -> tuple[str, str]:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "做多 Var / 做空 Lighter", "Long Var / Short Lighter"
        if side_n == "sell":
            return "做空 Var / 做多 Lighter", "Short Var / Long Lighter"
        side_u = side_n.upper() if side_n else "-"
        return side_u, side_u

    def _fmt_signed_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:+.4f}%"

    def _fmt_money(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:+.4f}U"

    def _fmt_notional(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.2f}U"

    @staticmethod
    def _fmt_rate_percent(value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value * Decimal('100'):+.5f}%"

    def adaptive_strategy_status_snapshot(self) -> dict[str, Any]:
        """Return immutable strategy state already computed off the hot path."""

        now_ms = time.time_ns() // 1_000_000
        epoch = self.active_parameter_epoch
        frame = self.last_market_frame
        decision = self.last_strategy_decision
        return {
            "execution_mode": self.strategy_config.execution_mode,
            "session_state": self._canary_session_state,
            "model_version": self.strategy_model.model_version,
            "model_hash": self.strategy_model.model_hash,
            "config_hash": self.strategy_config_hash,
            "epoch_id": epoch.epoch_id if epoch is not None else None,
            "epoch_age_ms": max(0, now_ms - epoch.valid_from_ms) if epoch else None,
            "epoch_expires_in_ms": max(0, epoch.expires_at_ms - now_ms) if epoch else None,
            "parameter_window_source": epoch.window_source if epoch else "live",
            "frame_age_ms": max(0, now_ms - frame.captured_at_ms) if frame else None,
            "decision_action": decision.action.value if decision else "NOT_EVALUATED",
            "decision_reason": decision.reason if decision else "not_evaluated",
            "decision_age_ms": (
                max(0, now_ms - self.last_strategy_decision_at_ms)
                if self.last_strategy_decision_at_ms is not None
                else None
            ),
        }

    def _fmt_colored_money(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        style = "green" if value >= 0 else "red"
        return f"[{style}]{self._fmt_money(value)}[/{style}]"

    @staticmethod
    def _fmt_duration(seconds: int | None) -> str:
        if seconds is None:
            return "-"
        minutes, sec = divmod(max(0, seconds), 60)
        hours, minute = divmod(minutes, 60)
        return f"{hours}h{minute:02d}m" if hours else f"{minute}m{sec:02d}s"

    @staticmethod
    def _current_round_status_text(record: OrderLifecycle, is_zh: bool) -> str:
        if record.hedge_status in {"retrying", "partial", "protective_close"}:
            return "Lighter 对冲补单中" if is_zh else "Lighter hedge retrying"
        if record.hedge_status == "error" or record.hedge_error:
            return "Lighter 对冲失败" if is_zh else "Lighter hedge failed"
        if record.hedge_status in {"queued", "submitting", "submitted"}:
            return "等待 Lighter 对冲" if is_zh else "waiting for Lighter hedge"
        if record.lighter_fill_price is None:
            return "等待 Lighter 成交" if is_zh else "waiting for Lighter fill"
        return "等待受控平仓" if is_zh else "waiting for controlled close"

    @staticmethod
    def _dashboard_window_reason(reason: str, is_zh: bool) -> str:
        if not is_zh:
            return reason.replace("_", " ")
        return {
            "ready": "已就绪",
            "empty_window": "暂无数据",
            "insufficient_span": "数据时长不足",
            "insufficient_density": "有效数据不足",
            "data_gap": "数据有缺口",
            "latest_sample_stale": "最新数据已过期",
            "sealed_calibration_prior": "历史基准",
        }.get(reason, "等待有效数据")

    @classmethod
    def _dashboard_reason_text(cls, reason: str | None, is_zh: bool) -> str:
        raw = str(reason or "-").strip()
        if not raw or raw == "-":
            return "-"
        if not is_zh:
            return raw.replace("_", " ")
        if raw.startswith("strategy_windows_not_ready"):
            _, _, detail = raw.partition(":")
            details = [
                cls._dashboard_window_reason(item, True)
                for item in detail.split(",")
                if item
            ]
            suffix = f"（{'、'.join(details)}）" if details else ""
            return f"采样窗口未就绪{suffix}"
        return {
            "not_evaluated": "尚未评估",
            "market_frame_unavailable": "等待有效行情",
            "market_frame_from_future": "行情时间异常",
            "market_frame_stale": "行情已过期",
            "market_sources_skewed": "两端行情不同步",
            "variational_quote_stale": "Variational 行情已过期",
            "lighter_quote_stale": "Lighter 行情已过期",
            "parameter_epoch_unavailable": "参数尚未生成",
            "parameter_epoch_expired": "参数已过期",
            "parameter_epoch_pending_confirmation": "参数等待确认",
            "strategy_sampling_disabled": "策略采样已关闭",
            "strategy_sampler_exception": "策略采样异常",
            "strategy_market_frame_gap": "行情数据中断",
            "strategy_sample_gap": "采样数据中断",
            "strategy_frame_clock_regression": "行情时间倒退",
            "strategy_sample_clock_regression": "采样时间倒退",
            "strategy_config_changed_during_parameter_compile": "参数生成期间配置发生变化",
            "unsupported_asset": "当前资产不受支持",
            "reference_notional_mismatch": "参考金额不一致",
            "order_notional_mismatch": "实盘金额不一致",
            "buy_dynamic_threshold_not_above_hard_limit": "BUY动态门槛未严格高于配置下限，禁止开仓",
            "sell_dynamic_threshold_below_hard_limit": "SELL动态门槛低于配置下限，禁止开仓",
            "both_dynamic_threshold_hard_limits_blocked": "BUY和SELL动态门槛均被配置下限限制",
            "no_direction_passed_frozen_threshold": "暂无方向达到开仓阈值",
            "frozen_threshold_passed": "已达到开仓阈值",
            "firm_rate_below_frozen_threshold": "最终报价低于开仓阈值",
            "firm_round_lower_bound_below_wear_floor": "预期收益不足",
            "firm_guard_passed_frozen_context": "最终报价校验通过",
            "manual_position_not_strategy_managed": "手动持仓等待手动平仓",
            "missing_frozen_position_epoch": "持仓参数缺失",
            "close_notional_mismatch": "平仓金额不一致",
            "close_floor_passed": "已达到直接平仓条件，等待Firm确认",
            "close_floor_not_met": "尚未达到平仓条件",
            "close_zero_wear_stability_pending": "零磨损线上稳定性累计中",
            "close_zero_wear_stability_passed": "零磨损线上稳定性已确认，等待Firm确认",
            "close_baseline_regression_not_met": "价差尚未回归平仓基准",
            "max_hold_alert_floor_passed": "持仓超时且已达到平仓条件",
            "max_hold_alert_waiting_controlled_floor": "持仓超时，等待安全平仓条件",
            "max_hold_alert_waiting_baseline_regression": "持仓超时，等待价差回归",
        }.get(raw, "状态异常，详情见日志")

    @staticmethod
    def _dashboard_action_text(action: str | None, is_zh: bool) -> str:
        raw = str(action or "NOT_EVALUATED")
        if not is_zh:
            return raw.replace("_", " ").title()
        return {
            "PAUSE": "暂停",
            "NO_ACTION": "等待",
            "OPEN": "开仓",
            "CLOSE": "平仓",
            "NOT_EVALUATED": "未评估",
        }.get(raw, "未知")

    @staticmethod
    def _dashboard_mode_text(mode: str, is_zh: bool) -> str:
        if not is_zh:
            return "Observe" if mode == "observe" else "Live"
        return "观察模式" if mode == "observe" else "连续实盘"

    @staticmethod
    def _dashboard_open_gate_text(reason: str, is_zh: bool) -> str:
        if not is_zh:
            return reason
        return {
            "observe mode never opens a position": "观察模式不自动开仓",
            "automatic Lighter hedge is disabled": "Lighter自动对冲未开启",
            "new opens are paused by operator": "操作员已暂停新开仓",
            "automation is paused": "自动化处于安全暂停",
            "parameter epoch is not yet available": "动态参数尚未就绪",
            "one-hour observe warmup is not complete": "一小时实时窗口尚未就绪",
            "dedicated Lighter order-entry WebSocket is not ready": "Lighter低延迟下单通道未就绪",
            "fresh account reconciliation is required": "等待最新账户对账",
            "account reconciliation snapshot is stale": "账户对账数据已过期",
            "account must be flat with no active Lighter orders": "账户非空仓或存在活动委托",
            "an execution intent or hedge is still active": "仍有订单或对冲正在执行",
        }.get(reason, reason)

    @staticmethod
    def _dashboard_session_text(state: str, is_zh: bool) -> str:
        if not is_zh:
            return {
                CANARY_SESSION_OBSERVING: "Preparing",
                CANARY_SESSION_ARMED: "Running",
                CANARY_SESSION_REVIEW_REQUIRED: "Preparing",
                CANARY_SESSION_HALTED: "Safety Paused",
            }.get(state, "Unknown")
        return {
            CANARY_SESSION_OBSERVING: "准备中",
            CANARY_SESSION_ARMED: "运行中",
            CANARY_SESSION_REVIEW_REQUIRED: "准备中",
            CANARY_SESSION_HALTED: "安全暂停",
        }.get(state, "未知")

    @staticmethod
    def _fmt_dashboard_age(age_ms: int | None, is_zh: bool) -> str:
        if age_ms is None:
            return "-"
        value = max(0, age_ms)
        if value < 1_000:
            return f"{value}ms"
        if value < 60_000:
            return f"{value / 1_000:.1f}s"
        return f"{value // 60_000}m"

    @staticmethod
    def _fmt_dashboard_duration(seconds: int | None, is_zh: bool) -> str:
        if seconds is None:
            return "-"
        minutes, sec = divmod(max(0, seconds), 60)
        hours, minute = divmod(minutes, 60)
        return f"{hours}h{minute:02d}m" if hours else f"{minute}m{sec:02d}s"

    @staticmethod
    def _fmt_policy_duration(seconds: int) -> str:
        value = max(0, seconds)
        if value % 3_600 == 0:
            return f"{value // 3_600}h"
        if value % 60 == 0:
            return f"{value // 60}m"
        return f"{value}s"

    @staticmethod
    def _dashboard_reconcile_text(outcome: AccountReconcileOutcome, is_zh: bool) -> str:
        if not is_zh:
            return {
                AccountReconcileOutcome.FRESH_MATCH: "Matched",
                AccountReconcileOutcome.FRESH_MISMATCH: "Mismatch",
                AccountReconcileOutcome.STALE: "Data stale",
                AccountReconcileOutcome.UNKNOWN: "Not checked",
            }[outcome]
        return {
            AccountReconcileOutcome.FRESH_MATCH: "正常",
            AccountReconcileOutcome.FRESH_MISMATCH: "仓位不一致",
            AccountReconcileOutcome.STALE: "账户数据已过期",
            AccountReconcileOutcome.UNKNOWN: "尚未检查",
        }[outcome]

    @classmethod
    def _dashboard_pause_text(cls, reason: str | None, is_zh: bool) -> str:
        raw = str(reason or "-").strip()
        if not raw or raw == "-":
            return "-"
        translated = cls._dashboard_reason_text(raw, is_zh)
        if not is_zh or translated != "状态异常，详情见日志":
            return translated
        if any("\u4e00" <= char <= "\u9fff" for char in raw):
            return raw
        return "检测到安全异常，详情见日志"

    @staticmethod
    def _fmt_signal_dot(allowed: bool) -> str:
        color = "green" if allowed else "red"
        return f"[bold {color}]●[/bold {color}]"

    @classmethod
    def _fmt_colored_rate(cls, value: Decimal | None) -> str:
        text = cls._fmt_rate_percent(value)
        if value is None:
            return text
        color = "green" if value >= 0 else "red"
        return f"[{color}]{text}[/{color}]"

    def _fmt_colored_leg_result(self, result: LegResult | None) -> str:
        if result is None or result.pnl is None or result.pct is None:
            return "-"
        color = "green" if result.pnl >= 0 else "red"
        return (
            f"[{color}]{self._fmt_money(result.pnl)}"
            f" / {self._fmt_signed_pct(result.pct)}[/{color}]"
        )

    def _fmt_colored_pnl_rate(
        self,
        pnl: Decimal | None,
        rate: Decimal | None,
    ) -> str:
        if pnl is None or rate is None:
            return "-"
        color = "green" if pnl >= 0 else "red"
        return (
            f"[{color}]{self._fmt_money(pnl)}"
            f" / {self._fmt_rate_percent(rate)}[/{color}]"
        )

    def _dashboard_window(self, side: StrategySide, minutes: int) -> Any | None:
        return self.strategy_window_stats.get(side, {}).get(minutes)

    def _dashboard_window_median(
        self,
        side: StrategySide,
        minutes: int,
    ) -> Decimal | None:
        window = self._dashboard_window(side, minutes)
        if window is None or not window.ready:
            return None
        return window.median

    def _dashboard_direction_preview(
        self,
        *,
        side: StrategySide,
        frame: MarketFrame | None,
        epoch: ParameterEpoch | None,
        current_open: OrderLifecycle | None,
        live_frame: bool,
        frame_rejection_reason: str | None,
        submission_block_reason: str | None,
        command_connected: bool,
        is_zh: bool,
    ) -> dict[str, Any]:
        reference_rate = frame.reference_rates.for_side(side) if frame else None
        actual_rate = frame.actual_rates.for_side(side) if frame else None
        open_pnl = (
            frame.actual_notional_usd * actual_rate
            if frame is not None and actual_rate is not None
            else None
        )
        component = epoch.component(side) if epoch else None
        dynamic_threshold_block = (
            self.strategy_engine.open_dynamic_threshold_block_reason(
                model_version=epoch.model_version,
                side=side,
                threshold=component.final,
            )
            if epoch is not None and component is not None
            else None
        )
        execution_headroom_bps = self.effective_open_execution_headroom_bps(
            side.value,
            self.strategy_config.order_notional_usd,
        )
        execution_threshold = (
            component.final + execution_headroom_bps / Decimal("10000")
            if component is not None
            else None
        )
        deviation = (
            reference_rate - component.baseline
            if reference_rate is not None and component is not None
            else None
        )
        round_lower_bound: Decimal | None = None
        wear_floor: Decimal | None = None
        if frame is not None and epoch is not None and open_pnl is not None:
            notional = frame.actual_notional_usd
            opposite_component = epoch.component(side.opposite)
            projected_exit_rate = (
                opposite_component.exit_opportunity
                if epoch.model_version in {
                    "adaptive-median-v2",
                    "adaptive-median-v3",
                    "adaptive-median-v4",
                    "adaptive-median-v5",
                }
                else opposite_component.baseline
            )
            phase_reserve = (
                notional
                * Decimal("2")
                * epoch.reserve_bps_per_leg
                / Decimal("10000")
            )
            round_lower_bound = (
                open_pnl
                + notional * projected_exit_rate
                - Decimal("2") * phase_reserve
            )
            wear_floor = (
                -notional
                * epoch.max_normal_round_wear_bps
                / Decimal("10000")
            )

        allowed = False
        if current_open is not None:
            reason = "已有持仓" if is_zh else "position already open"
        elif not live_frame or frame is None:
            reason = {
                "market_data_stale": (
                    "仅显示：行情已过期" if is_zh else "display only: market data stale"
                ),
                "source_skew_exceeded": (
                    "仅显示：两端行情不同步"
                    if is_zh
                    else "display only: sources out of sync"
                ),
            }.get(
                str(frame_rejection_reason or ""),
                "行情暂不可用" if is_zh else "market unavailable",
            )
        elif component is None:
            reason = "动态参数预热中" if is_zh else "parameters warming up"
        elif dynamic_threshold_block == "buy_dynamic_threshold_not_above_hard_limit":
            configured = self.strategy_config.buy_dynamic_threshold_min_pct
            reason = (
                f"BUY动态门槛必须严格高于{configured}%"
                if is_zh
                else f"BUY dynamic threshold must be above {configured}%"
            )
        elif dynamic_threshold_block == "sell_dynamic_threshold_below_hard_limit":
            configured = self.strategy_config.sell_dynamic_threshold_min_pct
            reason = (
                f"SELL动态门槛低于{configured}%，禁止开仓"
                if is_zh
                else f"SELL dynamic threshold below {configured}%; opening blocked"
            )
        elif reference_rate is None or reference_rate < execution_threshold:
            reason = "500U偏差未过开仓门槛" if is_zh else "500U rate below live entry gate"
        elif actual_rate is None or actual_rate < execution_threshold:
            target = self._fmt_notional(self.strategy_config.order_notional_usd)
            reason = (
                f"{target}瞬时价差未覆盖执行余量"
                if is_zh
                else f"{target} live rate below execution headroom"
            )
        elif epoch.model_version not in {
            "adaptive-median-v3",
            "adaptive-median-v4",
            "adaptive-median-v5",
        } and (
            round_lower_bound is None
            or wear_floor is None
            or round_lower_bound < wear_floor
        ):
            reason = "预计整轮下界不足" if is_zh else "round lower bound too low"
        elif self.strategy_config.execution_mode == "live" and submission_block_reason:
            reason = (
                f"全局条件未就绪：{self._dashboard_open_gate_text(submission_block_reason, True)}"
                if is_zh
                else f"global gate: {submission_block_reason}"
            )
        elif self.strategy_config.execution_mode == "live" and not command_connected:
            reason = "命令通道未连接" if is_zh else "command channel disconnected"
        else:
            allowed = True
            reason = (
                "通过（observe仅记录）"
                if is_zh and self.strategy_config.execution_mode == "observe"
                else (
                    "passed (observe only)"
                    if self.strategy_config.execution_mode == "observe"
                    else ("通过，待Firm Guard" if is_zh else "passed; Firm Guard pending")
                )
            )
        return {
            "reference_rate": reference_rate,
            "actual_rate": actual_rate,
            "open_pnl": open_pnl,
            "deviation": deviation,
            "round_lower_bound": round_lower_bound,
            "execution_threshold": execution_threshold,
            "execution_headroom_bps": execution_headroom_bps,
            "allowed": allowed,
            "reason": reason,
        }

    async def operations_dashboard_snapshot(self) -> dict[str, Any]:
        """Return one JSON-safe, presentation-only operations snapshot."""

        self._operations_dashboard_sequence += 1
        var_age = await self.get_variational_quote_age_ms(self.variational_ticker)
        lighter_age = await self.get_lighter_quote_age_ms()
        command_connected = await self.runtime.command_broker.extension_connected()
        current_open = await self._current_open_record()
        async with self._record_lock:
            ordered_records = [
                self.records[key]
                for key in self.record_order
                if key in self.records
            ]
        _current, completed_rounds = build_trade_rounds(ordered_records)
        recent_rounds = completed_rounds[-10:]
        normal_wear_usd = (
            self.strategy_config.order_notional_usd
            * self.strategy_config.max_normal_round_wear_bps
            / Decimal("10000")
        )
        basis_medians: dict[str, dict[str, Any]] = {}
        for minutes, label in ((5, "5m"), (30, "30m"), (60, "1h")):
            buy = self._dashboard_window(StrategySide.BUY, minutes)
            sell = self._dashboard_window(StrategySide.SELL, minutes)
            windows = (buy, sell)
            basis_medians[label] = {
                "longVar": decimal_to_str(
                    buy.median if buy is not None and buy.sample_count else None
                ),
                "shortVar": decimal_to_str(
                    sell.median if sell is not None and sell.sample_count else None
                ),
                "ready": all(window is not None and window.ready for window in windows),
                "sampleCount": min(
                    (window.sample_count for window in windows if window is not None),
                    default=0,
                ),
            }
        round_rows: list[dict[str, Any]] = []
        total_open = Decimal("0")
        total_close = Decimal("0")
        positive_rounds = 0
        negative_rounds = 0
        first_number = len(completed_rounds) - len(recent_rounds) + 1
        for offset, trade_round in enumerate(recent_rounds):
            open_pnl = trade_round.open_result.pnl
            close_pnl = trade_round.close_result.pnl
            round_pnl = trade_round.round_pnl
            if open_pnl is not None:
                total_open += open_pnl
            if close_pnl is not None:
                total_close += close_pnl
            if round_pnl is not None and round_pnl >= 0:
                positive_rounds += 1
            elif round_pnl is not None:
                negative_rounds += 1
            long_var = trade_round.open_record.side.strip().lower() == "buy"
            round_rows.append(
                {
                    "number": first_number + offset,
                    "directionKey": "long_var" if long_var else "short_var",
                    "direction": (
                        "多 Var / 空 Lighter"
                        if long_var
                        else "空 Var / 多 Lighter"
                    ),
                    "openWear": decimal_to_str(open_pnl),
                    "closeWear": decimal_to_str(close_pnl),
                    "roundWear": decimal_to_str(round_pnl),
                    "withinLimit": bool(
                        round_pnl is not None and round_pnl >= -normal_wear_usd
                    ),
                }
            )
        total_round = sum(
            (
                trade_round.round_pnl
                for trade_round in recent_rounds
                if trade_round.round_pnl is not None
            ),
            Decimal("0"),
        )
        completed_with_pnl = sum(
            1 for trade_round in recent_rounds if trade_round.round_pnl is not None
        )
        average_round = (
            total_round / Decimal(completed_with_pnl)
            if completed_with_pnl
            else None
        )
        decision = self.last_strategy_decision
        close_candidate = decision.close_candidate if decision is not None else None
        current_open_pnl = (
            leg_result_by_direction(current_open).pnl
            if current_open is not None
            else None
        )
        current_close_estimate = (
            close_candidate.expected_close_pnl_usd
            if current_open is not None and close_candidate is not None
            else None
        )
        current_round_estimate = (
            close_candidate.round_lower_bound_usd
            if current_open is not None and close_candidate is not None
            else None
        )
        account = self.last_account_snapshot
        var_position = account.var_position if account is not None else None
        lighter_position = account.lighter_position if account is not None else None
        positions_match = self.last_reconcile_outcome is AccountReconcileOutcome.FRESH_MATCH
        direction = None
        held_seconds = None
        if current_open is not None:
            long_var = current_open.side.strip().lower() == "buy"
            direction = "多 Var / 空 Lighter" if long_var else "空 Var / 多 Lighter"
            held_seconds = record_hold_seconds(current_open)
        ages = [age for age in (var_age, lighter_age) if age is not None]
        data_age = max(ages) if len(ages) == 2 else None
        if self.automation_paused:
            headline = f"安全暂停：{self._dashboard_pause_text(self.automation_pause_reason, True)}"
            level = "error"
            risk = "需要人工检查"
        elif self.operator_open_paused:
            headline = "操作员已暂停新开仓"
            level = "warning"
            risk = "新开仓已暂停"
        elif self.last_reconcile_outcome is AccountReconcileOutcome.FRESH_MISMATCH:
            headline = "权威账户仓位不一致"
            level = "error"
            risk = "单边暴露风险"
        elif not command_connected:
            headline = "Var 命令通道未连接"
            level = "warning"
            risk = "新开仓被阻止"
        else:
            headline = "策略与风控运行正常"
            level = "normal"
            risk = "策略运行稳定"
        strategy_status = (
            "安全暂停"
            if self.automation_paused
            else ("新开仓暂停" if self.operator_open_paused else "运行中")
        )
        return {
            "schema": "var-lit-v1-operations-state-v1",
            "environment": "runtime",
            "sequence": self._operations_dashboard_sequence,
            "generatedAt": utc_now(),
            "dataAgeMs": data_age,
            "health": {
                "runtimeActive": not self.stop_flag,
                "headline": headline,
                "level": level,
                "risk": risk,
                "actionBusy": self._operator_action_inflight,
            },
            "connections": {
                "command": command_connected,
                "privateStream": self.lighter_private_stream_ready,
                "lighterOrderEntry": self.lighter_order_entry_is_ready(),
                "varAgeMs": var_age,
                "lighterAgeMs": lighter_age,
            },
            "positions": {
                "var": decimal_to_str(var_position),
                "lighter": decimal_to_str(lighter_position),
                "activeOrders": (
                    account.lighter_active_orders if account is not None else None
                ),
                "capturedAt": account.captured_at if account is not None else None,
                "matched": positions_match,
                "reconcile": self._dashboard_reconcile_text(
                    self.last_reconcile_outcome, True
                ),
                "direction": direction,
                "heldSeconds": held_seconds,
            },
            "strategy": {
                "mode": self.strategy_config.execution_mode,
                "status": strategy_status,
                "openPaused": self.operator_open_paused,
                "automationPaused": self.automation_paused,
                "pauseReason": self.automation_pause_reason,
                "automationReady": self.automation_ready,
            },
            "config": {
                "executionMode": self.strategy_config.execution_mode,
                "orderNotionalUsd": decimal_to_str(
                    self.strategy_config.order_notional_usd
                ),
                "maxNormalRoundWearUsd": decimal_to_str(-normal_wear_usd),
                "buyThresholdMinPct": decimal_to_str(
                    self.strategy_config.buy_dynamic_threshold_min_pct
                ),
                "sellThresholdMinPct": decimal_to_str(
                    self.strategy_config.sell_dynamic_threshold_min_pct
                ),
                "maxQuoteAgeMs": self.strategy_config.max_quote_age_ms,
                "earlyExitMinutes": decimal_to_str(
                    Decimal(self.strategy_config.early_exit_seconds) / Decimal("60")
                ),
            },
            "metrics": {
                "currentRoundEstimate": decimal_to_str(current_round_estimate),
                "currentRoundNote": (
                    "当前可执行平仓估值；最终轮次仍以双边成交价结算"
                    if current_open is not None
                    else "当前无持仓"
                ),
                "totalOpenWear": decimal_to_str(total_open),
                "totalCloseWear": decimal_to_str(total_close),
                "totalWear": decimal_to_str(total_round),
                "averageWear": decimal_to_str(average_round),
                "positiveRounds": positive_rounds,
                "negativeRounds": negative_rounds,
                "basisMedians": basis_medians,
                "currentPositionPnl": {
                    "active": current_open is not None,
                    "open": decimal_to_str(current_open_pnl),
                    "closeEstimate": decimal_to_str(current_close_estimate),
                },
            },
            "recentRounds": round_rows,
        }

    async def _operator_state_guard(self) -> str:
        current_open = await self._current_open_record()
        account = self.last_account_snapshot
        payload = {
            "account_captured_at": account.captured_at if account else None,
            "var_position": decimal_to_str(account.var_position) if account else None,
            "lighter_position": (
                decimal_to_str(account.lighter_position) if account else None
            ),
            "active_orders": account.lighter_active_orders if account else None,
            "current_open": current_open.trade_key if current_open else None,
            "pending_intent": (
                self.pending_var_intent.state if self.pending_var_intent else None
            ),
            "transition": self.transition_in_progress(),
            "operator_open_paused": self.operator_open_paused,
            "automation_paused": self.automation_paused,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _authoritative_account_error(self) -> str | None:
        account = self.last_account_snapshot
        if account is None:
            return "尚无权威账户快照"
        captured = parse_iso_datetime(account.captured_at)
        maximum_age = max(10.0, self.strategy_config.reconcile_interval_seconds * 2)
        if (
            captured is None
            or (datetime.now(timezone.utc) - captured).total_seconds() > maximum_age
        ):
            return "权威账户快照已过期，请先执行账户对账"
        if account.lighter_active_orders != 0:
            return f"Lighter 仍有 {account.lighter_active_orders} 个活动委托"
        return None

    def _config_updates_from_dashboard(
        self,
        payload: dict[str, Any],
    ) -> tuple[dict[str, str] | None, str | None, dict[str, str]]:
        desired_order = to_decimal(payload.get("orderNotionalUsd"))
        desired_wear = to_decimal(payload.get("maxNormalRoundWearUsd"))
        desired_buy = to_decimal(payload.get("buyThresholdMinPct"))
        desired_sell = to_decimal(payload.get("sellThresholdMinPct"))
        desired_age = to_int(payload.get("maxQuoteAgeMs"))
        desired_early = to_decimal(payload.get("earlyExitMinutes"))
        desired_mode = str(payload.get("executionMode") or "").strip().lower()
        facts = {
            "执行模式": desired_mode or "-",
            "单边金额": f"{desired_order} U" if desired_order is not None else "-",
            "磨损下限": f"{desired_wear} U" if desired_wear is not None else "-",
            "做多硬门槛": f"{desired_buy}%" if desired_buy is not None else "-",
            "做空硬门槛": f"{desired_sell}%" if desired_sell is not None else "-",
        }
        if desired_order is None or desired_order <= 0:
            return None, "单边开仓金额必须大于 0", facts
        if desired_wear is None or desired_wear >= 0:
            return None, "允许磨损下限必须是负数", facts
        wear_bps = -desired_wear * Decimal("10000") / desired_order
        candidate_payload = {
            "executionMode": desired_mode,
            "orderNotionalUsd": decimal_to_str(desired_order),
            "buyDynamicThresholdMinPct": decimal_to_str(desired_buy),
            "sellDynamicThresholdMinPct": decimal_to_str(desired_sell),
            "maxNormalRoundWearBps": decimal_to_str(wear_bps),
            "maxQuoteAgeMs": desired_age,
            "earlyExitMinutes": decimal_to_str(desired_early),
        }
        candidate = strategy_config_from_payload(
            candidate_payload,
            current=self.strategy_config,
        )
        expected = (
            desired_mode in STRATEGY_EXECUTION_MODES
            and candidate.order_notional_usd == desired_order
            and desired_buy is not None
            and candidate.buy_dynamic_threshold_min_pct == desired_buy
            and desired_sell is not None
            and candidate.sell_dynamic_threshold_min_pct == desired_sell
            and candidate.max_normal_round_wear_bps == wear_bps
            and desired_age is not None
            and candidate.max_quote_age_ms == desired_age
            and desired_early is not None
            and candidate.early_exit_seconds == int(desired_early * Decimal("60"))
        )
        if not expected:
            return None, "参数超出策略允许范围或格式无效", facts
        updates = {
            "STRATEGY_EXECUTION_MODE": candidate.execution_mode,
            "STRATEGY_ORDER_NOTIONAL_USD": decimal_to_str(
                candidate.order_notional_usd
            ) or "0",
            "STRATEGY_BUY_DYNAMIC_THRESHOLD_MIN_PCT": decimal_to_str(
                candidate.buy_dynamic_threshold_min_pct
            ) or "0",
            "STRATEGY_SELL_DYNAMIC_THRESHOLD_MIN_PCT": decimal_to_str(
                candidate.sell_dynamic_threshold_min_pct
            ) or "0",
            "STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS": decimal_to_str(
                candidate.max_normal_round_wear_bps
            ) or "0",
            "STRATEGY_MAX_QUOTE_AGE_MS": str(candidate.max_quote_age_ms),
            "STRATEGY_EARLY_EXIT_MINUTES": decimal_to_str(
                Decimal(candidate.early_exit_seconds) / Decimal("60")
            ) or "0",
        }
        facts["折算磨损"] = f"{updates['STRATEGY_MAX_NORMAL_ROUND_WEAR_BPS']} bps/round"
        return updates, None, facts

    async def prepare_operations_action(
        self,
        action: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        supported = {
            "pause_open",
            "force_round_close",
            "close_var_residual",
            "close_lighter_residual",
            "refresh_var",
            "reconcile",
            "stage_config",
        }
        if action not in supported:
            return {"allowed": False, "reason": "未知操作"}
        guard = await self._operator_state_guard()
        account = self.last_account_snapshot
        current_open = await self._current_open_record()
        facts = {
            "Var 权威仓位": (
                f"{account.var_position} BTC" if account is not None else "-"
            ),
            "Lighter 权威仓位": (
                f"{account.lighter_position} BTC" if account is not None else "-"
            ),
            "活动委托": (
                str(account.lighter_active_orders) if account is not None else "-"
            ),
            "执行中转换": "是" if self.transition_in_progress() else "否",
        }
        if self._operator_action_inflight:
            return {"allowed": False, "reason": "另一个操作正在执行", "facts": facts}
        if action == "pause_open":
            return {
                "allowed": True,
                "guard": guard,
                "message": (
                    "确认恢复策略新开仓；其他所有风控门槛仍会继续生效。"
                    if self.operator_open_paused
                    else "确认暂停新的策略开仓；现有持仓仍会继续自动平仓和对账。"
                ),
                "facts": {
                    **facts,
                    "目标状态": "恢复新开仓" if self.operator_open_paused else "暂停新开仓",
                },
            }
        if action == "stage_config":
            updates, error, config_facts = self._config_updates_from_dashboard(payload)
            if error is not None or updates is None:
                return {"allowed": False, "reason": error, "facts": config_facts}
            account_error = self._authoritative_account_error()
            if account_error is not None:
                return {"allowed": False, "reason": account_error, "facts": config_facts}
            assert account is not None
            if (
                abs(account.var_position) > VAR_POSITION_TOLERANCE
                or abs(account.lighter_position) > VAR_POSITION_TOLERANCE
                or self.transition_in_progress()
            ):
                return {
                    "allowed": False,
                    "reason": "只允许在双方空仓且没有执行中订单时保存参数",
                    "facts": config_facts,
                }
            return {
                "allowed": True,
                "guard": guard,
                "message": "确认写入 .env；当前进程不热更新，重启 Runtime 后生效。",
                "facts": config_facts,
                "updates": updates,
            }
        account_error = self._authoritative_account_error()
        if action not in {"refresh_var", "reconcile"} and account_error is not None:
            return {"allowed": False, "reason": account_error, "facts": facts}
        if action in {
            "force_round_close",
            "close_var_residual",
            "close_lighter_residual",
        } and self.transition_in_progress():
            return {"allowed": False, "reason": "仍有订单或对冲正在执行", "facts": facts}
        command_connected = await self.runtime.command_broker.extension_connected()
        if action == "force_round_close":
            if current_open is None:
                return {"allowed": False, "reason": "当前没有可追踪的双边持仓", "facts": facts}
            if self.last_reconcile_outcome is not AccountReconcileOutcome.FRESH_MATCH:
                return {"allowed": False, "reason": "强制整轮平仓前必须精确对账一致", "facts": facts}
            if not command_connected:
                return {"allowed": False, "reason": "Var 命令通道未连接", "facts": facts}
            return {
                "allowed": True,
                "guard": guard,
                "message": "将忽略策略收益阈值，先平 Var，再按实际成交量 reduce-only 平 Lighter。",
                "facts": {**facts, "当前轮次": current_open.trade_key},
            }
        if action == "close_var_residual":
            assert account is not None
            if abs(account.var_position) <= VAR_POSITION_TOLERANCE:
                return {"allowed": False, "reason": "Var 当前没有残仓", "facts": facts}
            if abs(account.lighter_position) > VAR_POSITION_TOLERANCE:
                return {"allowed": False, "reason": "Lighter 并非空仓，请使用强制整轮平仓", "facts": facts}
            if current_open is None:
                return {"allowed": False, "reason": "本地没有与 Var 残仓对应的恢复记录", "facts": facts}
            if not command_connected:
                return {"allowed": False, "reason": "Var 命令通道未连接", "facts": facts}
            return {
                "allowed": True,
                "guard": guard,
                "message": "仅平权威快照中的 Var 残仓，不会创建新的 Lighter 对冲。",
                "facts": facts,
            }
        if action == "close_lighter_residual":
            assert account is not None
            if abs(account.lighter_position) <= VAR_POSITION_TOLERANCE:
                return {"allowed": False, "reason": "Lighter 当前没有残仓", "facts": facts}
            if abs(account.var_position) > VAR_POSITION_TOLERANCE:
                return {"allowed": False, "reason": "Var 并非空仓，请使用强制整轮平仓", "facts": facts}
            return {
                "allowed": True,
                "guard": guard,
                "message": "提交精确数量的 Lighter reduce-only 市价 IOC，不会操作 Var。",
                "facts": facts,
            }
        if action == "refresh_var":
            if self.pending_var_intent is not None or self._auto_var_order_inflight:
                return {"allowed": False, "reason": "Var 订单仍处于提交或确认阶段", "facts": facts}
            return {
                "allowed": True,
                "guard": guard,
                "message": "刷新前会持久化暂停新开仓，随后等待 Var 行情重新变为新鲜。",
                "facts": facts,
            }
        return {
            "allowed": True,
            "guard": guard,
            "message": "重新读取双方权威仓位；只有精确一致时才恢复对账类安全暂停。",
            "facts": facts,
        }

    @staticmethod
    def _write_dotenv_updates(path: Path, updates: dict[str, str]) -> None:
        unknown = set(updates) - RUNTIME_DOTENV_ALLOWED_KEYS
        if unknown:
            raise RuntimeError(f"Refusing unknown runtime settings: {sorted(unknown)}")
        lines = path.read_text(encoding="utf-8").splitlines()
        pending = dict(updates)
        output: list[str] = []
        for line in lines:
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                output.append(line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in pending:
                output.append(f"{key}={pending.pop(key)}")
            else:
                output.append(line)
        if pending:
            output.append("")
            output.extend(f"{key}={value}" for key, value in pending.items())
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text("\n".join(output) + "\n", encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()

    async def _refresh_variational_page_via_cdp(self) -> None:
        debug_origin = "http://127.0.0.1:9222"

        def load_targets() -> list[dict[str, Any]]:
            response = requests.get(f"{debug_origin}/json", timeout=3)
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, list):
                raise RuntimeError("Chrome target list is malformed")
            return [item for item in value if isinstance(item, dict)]

        targets = await asyncio.to_thread(load_targets)
        target = next(
            (
                item
                for item in targets
                if str(item.get("type") or "") == "page"
                and "omni.variational.io" in str(item.get("url") or "")
                and str(item.get("webSocketDebuggerUrl") or "").startswith("ws")
            ),
            None,
        )
        if target is None:
            raise RuntimeError("未找到正在运行的 Variational 页面")
        websocket_url = str(target["webSocketDebuggerUrl"])
        async with websockets.connect(
            websocket_url,
            origin=debug_origin,
            open_timeout=3,
            close_timeout=1,
            max_size=1024 * 1024,
        ) as socket:
            await socket.send(
                json.dumps({"id": 1, "method": "Page.reload", "params": {}})
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                raw = await asyncio.wait_for(socket.recv(), timeout=5)
                response = json.loads(raw)
                if isinstance(response, dict) and response.get("id") == 1:
                    if response.get("error"):
                        raise RuntimeError(str(response["error"]))
                    break
            else:
                raise RuntimeError("Chrome 没有确认页面刷新")
        await asyncio.sleep(0.5)
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            age = await self.get_variational_quote_age_ms(self.variational_ticker)
            if age is not None and age <= self.strategy_config.max_quote_age_ms:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Var 页面刷新后，实时行情没有在 60 秒内恢复")

    async def execute_operations_action(
        self,
        action: str,
        payload: dict[str, Any],
        preview: dict[str, Any],
    ) -> dict[str, Any]:
        expected_guard = str(preview.get("guard") or "")
        async with self._operator_action_lock:
            if expected_guard != await self._operator_state_guard():
                return {"ok": False, "error": "账户或执行状态已变化，请重新确认"}
            self._operator_action_inflight = True
            try:
                if action == "pause_open":
                    self.operator_open_paused = not self.operator_open_paused
                    await self.persist_runtime_state()
                    return {
                        "ok": True,
                        "message": (
                            "已暂停新的策略开仓，平仓和对账继续运行。"
                            if self.operator_open_paused
                            else "已恢复新开仓许可，所有策略与风控门槛继续生效。"
                        ),
                    }
                if action == "stage_config":
                    updates = preview.get("updates")
                    if not isinstance(updates, dict):
                        return {"ok": False, "error": "参数确认数据已失效"}
                    await asyncio.to_thread(
                        self._write_dotenv_updates,
                        DOTENV_FILE,
                        {str(key): str(value) for key, value in updates.items()},
                    )
                    return {"ok": True, "message": "参数已安全写入 .env，重启 Runtime 后生效。"}
                if action == "reconcile":
                    matched = await self.reconcile_accounts(allow_resume=True)
                    return {
                        "ok": matched,
                        "message": (
                            "双方账户已精确对账。"
                            if matched
                            else f"对账未通过：{self.last_reconcile_status}"
                        ),
                        "error": None if matched else self.last_reconcile_status,
                    }
                self.operator_open_paused = True
                await self.persist_runtime_state()
                current_open = await self._current_open_record()
                if action == "force_round_close":
                    if current_open is None:
                        return {"ok": False, "error": "当前持仓已变化"}
                    submitted = await self.emergency_flatten_var(current_open)
                    if not submitted:
                        detail = str(self.automation_pause_reason or "").strip()
                        return {
                            "ok": False,
                            "error": (
                                detail
                                if detail and detail != "-"
                                else "Var 强制平仓未被交易通道受理"
                            ),
                        }
                    return {"ok": True, "message": "强制整轮平仓已提交，等待双方成交和权威对账。"}
                if action == "close_var_residual":
                    if current_open is None:
                        return {"ok": False, "error": "Var 残仓恢复记录已变化"}
                    submitted = await self.emergency_flatten_var(
                        current_open,
                        intent_phase="operator_var_only_close",
                    )
                    if not submitted:
                        detail = str(self.automation_pause_reason or "").strip()
                        return {
                            "ok": False,
                            "error": (
                                detail
                                if detail and detail != "-"
                                else "Var 单边残仓平仓未被交易通道受理"
                            ),
                        }
                    return {"ok": True, "message": "Var 单边残仓平仓已提交，未创建 Lighter 对冲。"}
                if action == "close_lighter_residual":
                    account = self.last_account_snapshot
                    if account is None:
                        return {"ok": False, "error": "权威账户快照已丢失"}
                    qty = abs(account.lighter_position)
                    side = "buy" if account.lighter_position > 0 else "sell"
                    key = f"operator-lighter-close:{uuid.uuid4().hex}"
                    record = OrderLifecycle(
                        trade_key=key,
                        trade_id=key,
                        side=side,
                        qty=qty,
                        asset=(self.variational_ticker or "BTC").upper(),
                        auto_hedge_enabled=True,
                        last_variational_status="operator_lighter_only_close",
                        var_fill_source="operator_lighter_only_close",
                        var_event_origin=VarEventOrigin.MANUAL_LIVE.value,
                        strategy_phase="operator_lighter_only_close",
                        strategy_tag=MANUAL_STRATEGY_TAG,
                        lighter_target_qty_override=qty,
                        lighter_reduce_only=True,
                        trace_id=new_trace_id(),
                        execution_state=EXECUTION_STATE_RECOVERY_REQUIRED,
                    )
                    async with self._record_lock:
                        self.records[key] = record
                        self.record_order.append(key)
                    await self.persist_runtime_state()
                    if not self.schedule_lighter_order(record):
                        return {"ok": False, "error": "Lighter 残仓平仓任务未能调度"}
                    return {"ok": True, "message": "Lighter reduce-only 残仓平仓已提交，等待成交确认。"}
                if action == "refresh_var":
                    await self._refresh_variational_page_via_cdp()
                    return {"ok": True, "message": "Var 页面已刷新，实时行情已恢复；新开仓仍保持暂停。"}
                return {"ok": False, "error": "未知操作"}
            finally:
                self._operator_action_inflight = False

    async def render_dashboard(self) -> Group:
        is_zh = self.args.lang == "zh"
        dashboard_frame: MarketFrame | None = None
        dashboard_observation: dict[str, Any] = {"valid": False}
        try:
            dashboard_frame, dashboard_observation = (
                await self.current_adaptive_market_frame(
                    allow_stale_for_display=True,
                )
            )
        except Exception:
            # Display construction must never disturb trading or stop the
            # dashboard.  The last complete frame remains visible below.
            dashboard_frame = None
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(
            self.variational_ticker
        )
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        var_age = await self.get_variational_quote_age_ms(self.variational_ticker)
        lighter_age = await self.get_lighter_quote_age_ms()
        command_connected = await self.runtime.command_broker.extension_connected()
        current_open = await self._current_open_record()
        async with self._record_lock:
            ordered_records = [
                self.records[key] for key in self.record_order if key in self.records
            ]
        _open, rounds = build_trade_rounds(ordered_records)
        status = self.adaptive_strategy_status_snapshot()
        frame = self.last_market_frame
        newest_display_frame = dashboard_frame or frame
        display_frame_is_live = (
            bool(dashboard_observation.get("valid"))
            if dashboard_frame is not None
            else frame is not None
        )
        if newest_display_frame is not None:
            self._dashboard_last_market_frame = newest_display_frame
        display_frame = newest_display_frame or self._dashboard_last_market_frame
        if dashboard_frame is not None:
            # Keep the BBO and derived spreads on the same 200ms display pass.
            var_bid = dashboard_frame.var_bid
            var_ask = dashboard_frame.var_ask
        epoch = self.active_parameter_epoch
        decision = self.last_strategy_decision
        close_candidate = decision.close_candidate if decision else None
        submission_block_reason = (
            self.live_open_block_reason()
            if current_open is None and self.strategy_config.execution_mode == "live"
            else None
        )
        frame_rejection_reason = str(
            dashboard_observation.get("rejection_reason") or ""
        )
        previews = {
            side: self._dashboard_direction_preview(
                side=side,
                frame=display_frame,
                epoch=epoch,
                current_open=current_open,
                live_frame=display_frame_is_live,
                frame_rejection_reason=frame_rejection_reason,
                submission_block_reason=submission_block_reason,
                command_connected=command_connected,
                is_zh=is_zh,
            )
            for side in StrategySide
        }

        mode_text = self._dashboard_mode_text(status["execution_mode"], is_zh)
        hedge_on = bool(self.args.auto_hedge)
        hedge_color = "green" if hedge_on else "red"
        hedge_text = "ON" if hedge_on else "OFF"
        title = (
            f"Var-Lit V1 | {quote_asset or self.variational_ticker or 'BTC'}"
            f" | {ADAPTIVE_MODEL_VERSION} | {mode_text}"
            f" | [{hedge_color}]{'自动对冲' if is_zh else 'Auto Hedge'}={hedge_text}[/{hedge_color}]"
        )
        header = Panel(
            f"{title} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            border_style="cyan",
        )

        market = Table(title="同步行情" if is_zh else "Synchronized Market", expand=True)
        market.add_column(
            "平台" if is_zh else "Source",
            width=28,
            min_width=28,
            max_width=28,
            no_wrap=True,
        )
        market.add_column(
            "买价" if is_zh else "Bid",
            ratio=1,
            min_width=24,
            justify="right",
            no_wrap=True,
        )
        market.add_column(
            "卖价" if is_zh else "Ask",
            ratio=1,
            min_width=24,
            justify="right",
            no_wrap=True,
        )
        market.add_column(
            "延迟" if is_zh else "Age",
            width=10,
            min_width=10,
            max_width=10,
            justify="right",
            no_wrap=True,
        )
        market.add_row(
            "Variational",
            self._fmt_price(var_bid),
            self._fmt_price(var_ask),
            self._fmt_dashboard_age(var_age, is_zh),
        )
        market.add_row(
            "Lighter",
            self._fmt_price(lighter_bid),
            self._fmt_price(lighter_ask),
            self._fmt_dashboard_age(lighter_age, is_zh),
        )

        spreads = Table(
            title=(
                "价差、预估盈亏与开仓信号"
                if is_zh
                else "Spreads, Estimated PnL & Signals"
            ),
            expand=True,
        )
        spread_columns = (
            ("方向", "Direction", 27),
            ("500U当前", "500U Current", 12),
            (
                f"{self._fmt_notional(self.strategy_config.order_notional_usd)}当前",
                f"{self._fmt_notional(self.strategy_config.order_notional_usd)} Current",
                12,
            ),
            ("预估开仓PnL", "Est. Open PnL", 14),
            ("5m中位数", "5m Median", 12),
            ("30m中位数", "30m Median", 12),
            ("1h中位数", "1h Median", 12),
            ("基准偏离", "Baseline Deviation", 13),
            ("开仓信号", "Open Signal", None),
        )
        for zh_label, en_label, width in spread_columns:
            label = zh_label if is_zh else en_label
            column_options: dict[str, Any] = {
                "justify": (
                    "left" if zh_label in {"方向", "开仓信号"} else "right"
                ),
                "no_wrap": True,
                "overflow": "ellipsis",
            }
            if width is None:
                column_options.update({"ratio": 1, "min_width": 22})
            else:
                column_options.update(
                    {"width": width, "min_width": width, "max_width": width}
                )
            spreads.add_column(label, **column_options)
        for side in StrategySide:
            preview = previews[side]
            direction = (
                "做多 Var / 做空 Lighter"
                if side is StrategySide.BUY
                else "做空 Var / 做多 Lighter"
            ) if is_zh else (
                "Long Var / Short Lighter"
                if side is StrategySide.BUY
                else "Short Var / Long Lighter"
            )
            signal_text = (
                f"{self._fmt_signal_dot(preview['allowed'])} {preview['reason']}"
            )
            spreads.add_row(
                direction,
                self._fmt_colored_rate(preview["reference_rate"]),
                self._fmt_colored_rate(preview["actual_rate"]),
                self._fmt_colored_money(preview["open_pnl"]),
                self._fmt_rate_percent(self._dashboard_window_median(side, 5)),
                self._fmt_rate_percent(self._dashboard_window_median(side, 30)),
                self._fmt_rate_percent(self._dashboard_window_median(side, 60)),
                self._fmt_colored_rate(preview["deviation"]),
                signal_text,
            )

        adaptive = Table(title="动态方向门槛" if is_zh else "Dynamic Direction Thresholds", expand=True)
        entry_threshold_label = (
            "三窗门槛"
            if is_zh
            and ADAPTIVE_MODEL_VERSION
            in {"adaptive-median-v3", "adaptive-median-v4", "adaptive-median-v5"}
            else (
                "3-Window Gate"
                if ADAPTIVE_MODEL_VERSION
                in {"adaptive-median-v3", "adaptive-median-v4", "adaptive-median-v5"}
                else "Q80"
            )
        )
        adaptive_columns = (
            ("方向", "Direction"),
            ("基准", "Baseline"),
            (entry_threshold_label, entry_threshold_label),
            ("成本线", "Economic"),
            ("均衡线", "Balance"),
            ("开仓门槛", "Live Entry Gate"),
        )
        for zh_label, en_label in adaptive_columns:
            label = zh_label if is_zh else en_label
            if zh_label == "方向":
                adaptive.add_column(
                    label,
                    width=42,
                    min_width=42,
                    max_width=42,
                    justify="left",
                    no_wrap=True,
                )
            else:
                adaptive.add_column(
                    label,
                    ratio=1,
                    min_width=14,
                    justify="right",
                    no_wrap=True,
                )
        for side in StrategySide:
            component = epoch.component(side) if epoch else None
            adaptive.add_row(
                (
                    "做多 Var / 做空 Lighter"
                    if side is StrategySide.BUY
                    else "做空 Var / 做多 Lighter"
                ) if is_zh else side.value.title(),
                self._fmt_rate_percent(component.baseline if component else None),
                self._fmt_rate_percent(
                    component.entry_opportunity if component else None
                ),
                self._fmt_rate_percent(component.economic if component else None),
                self._fmt_rate_percent(
                    (
                        component.balance
                        if component is not None
                        and component.balance > Decimal("-1000000")
                        else None
                    )
                ),
                self._fmt_rate_percent(
                    (
                        component.final
                        + self.effective_open_execution_headroom_bps(
                            side.value,
                            self.strategy_config.order_notional_usd,
                        )
                        / Decimal("10000")
                        if component is not None
                        else None
                    )
                ),
            )

        readiness = Table(
            title="数据与参数" if is_zh else "Data & Parameters",
            show_header=False,
            expand=True,
        )
        readiness.add_column(
            "field",
            style="bold",
            width=16,
            min_width=16,
            max_width=16,
            no_wrap=True,
        )
        readiness.add_column(
            "value",
            ratio=1,
            no_wrap=True,
            overflow="ellipsis",
        )
        window_parts = []
        for minutes, zh_label, en_label in (
            (5, "5m", "5m"),
            (30, "30m", "30m"),
            (60, "1h", "1h"),
        ):
            windows = [self._dashboard_window(side, minutes) for side in StrategySide]
            ready = bool(windows) and all(window is not None and window.ready for window in windows)
            reasons = sorted({window.reason for window in windows if window is not None and not window.ready})
            source = windows[0].source if windows and windows[0] is not None else "not-started"
            label = zh_label if is_zh else en_label
            if is_zh:
                source_text = {"sealed-prior": "历史基准", "live": "实时数据"}.get(source, "未开始")
                if ready:
                    window_parts.append(f"{label}：已就绪（{source_text}）")
                else:
                    reason_text = "、".join(self._dashboard_window_reason(item, True) for item in reasons)
                    window_parts.append(f"{label}：等待（{reason_text or '尚未开始'}）")
            else:
                if ready:
                    window_parts.append(f"{label}: ready ({source.replace('-', ' ')})")
                else:
                    reason_text = ", ".join(self._dashboard_window_reason(item, False) for item in reasons)
                    window_parts.append(f"{label}: waiting ({reason_text or 'not started'})")
        readiness.add_row("采样窗口" if is_zh else "Windows", " | ".join(window_parts))
        weight_text = " | ".join(
            f"{label} {format(weight * Decimal('100'), '.0f')}%"
            for label, weight in (
                ("5m", self.strategy_model.weight_5m),
                ("30m", self.strategy_model.weight_30m),
                ("1h", self.strategy_model.weight_1h),
            )
        )
        readiness.add_row("周期权重" if is_zh else "Window Weights", weight_text)
        dashboard_refresh_ms = int(
            round(self.strategy_config.dashboard_refresh_seconds * 1_000)
        )
        readiness.add_row(
            "刷新节奏" if is_zh else "Refresh Cadence",
            (
                f"行情/价差 {dashboard_refresh_ms}ms"
                " | 中位数采样 Var事件（1s兜底）"
                f" | 门槛更新 {self._fmt_policy_duration(self.strategy_config.parameter_refresh_seconds)}"
                if is_zh
                else
                f"Market/spreads {dashboard_refresh_ms}ms"
                " | median samples on Var events (1s fallback)"
                f" | threshold updates {self._fmt_policy_duration(self.strategy_config.parameter_refresh_seconds)}"
            ),
        )
        market_state = (
            ("正常" if is_zh else "Live")
            if display_frame_is_live
            else (
                (
                    {
                        "market_data_stale": "行情已过期（显示最新已知值）",
                        "source_skew_exceeded": "两端行情不同步（显示最新已知值）",
                    }.get(frame_rejection_reason, "等待新报价（显示最新已知值）")
                    if is_zh
                    else {
                        "market_data_stale": "Market stale (latest known values shown)",
                        "source_skew_exceeded": "Sources out of sync (latest known values shown)",
                    }.get(frame_rejection_reason, "Waiting for quote (latest known values shown)")
                )
                if display_frame is not None
                else ("等待有效行情" if is_zh else "Waiting for valid market data")
            )
        )
        readiness.add_row("行情状态" if is_zh else "Market", market_state)
        if status["epoch_id"]:
            epoch_age = self._fmt_duration(
                int(status["epoch_age_ms"] // 1_000)
                if status["epoch_age_ms"] is not None
                else None
            )
            epoch_text = (
                f"已生效 | {status['epoch_id'][:12]} | {epoch_age}前更新"
                f" | {self._fmt_policy_duration(self.strategy_config.parameter_refresh_seconds)}滚动激活"
                if is_zh
                else f"Active | {status['epoch_id'][:12]} | updated {epoch_age} ago"
                f" | {self._fmt_policy_duration(self.strategy_config.parameter_refresh_seconds)} rolling activation"
            )
        else:
            epoch_text = "等待生成" if is_zh else "Waiting"
        readiness.add_row("策略参数" if is_zh else "Parameters", epoch_text)
        effective_close_reserve_bps = (
            self.strategy_config.provisional_reserve_bps_per_leg
            * CLOSE_RESERVE_MULTIPLIER
        )
        readiness.add_row(
            "金额与风控" if is_zh else "Amounts & Risk",
            (
                f"参考 {self._fmt_notional(self.strategy_config.reference_notional_usd)}"
                f" | 实盘 {self._fmt_notional(self.strategy_config.order_notional_usd)}"
                f" | 平仓预留 {effective_close_reserve_bps}bps/leg"
                f" | 磨损上限 {self.strategy_config.max_normal_round_wear_bps}bps/round"
                if is_zh
                else
                f"Reference {self._fmt_notional(self.strategy_config.reference_notional_usd)}"
                f" | Live {self._fmt_notional(self.strategy_config.order_notional_usd)}"
                f" | Close reserve {effective_close_reserve_bps}bps/leg"
                f" | Wear limit {self.strategy_config.max_normal_round_wear_bps}bps/round"
            ),
        )
        sample_count, sample_coverage_ms = self.strategy_window_store.coverage()
        one_hour_sample_ready = all(
            (window := self._dashboard_window(side, 60)) is not None
            and window.ready
            and window.source == "live"
            for side in StrategySide
        )
        resume_state = self._strategy_history_resume_state
        if is_zh:
            resume_text = {
                "resumed": "历史续接已生效",
                "pending_first_live_frame": "历史已载入，等待首帧",
                "pending_partial_history": "本轮缓存已续接，继续采样",
                "not_loaded": "未尝试续接",
                "sample_file_missing": "无历史样本",
                "no_matching_valid_samples": "无匹配历史",
                "history_stale_over_5m": "历史超过5m，重新采样",
                "rejected_gap_over_5m": "停机超过5m，重新采样",
                "history_coverage_under_1h": "历史不足1h",
                "history_density_too_low": "历史密度不足",
                "sampling_disabled": "续接关闭",
            }.get(resume_state, "历史续接未采用")
        else:
            resume_text = {
                "resumed": "history resumed",
                "pending_first_live_frame": "history loaded; awaiting first frame",
                "pending_partial_history": "current session resumed; collecting",
                "not_loaded": "resume not attempted",
                "sample_file_missing": "no history",
                "no_matching_valid_samples": "no matching history",
                "history_stale_over_5m": "history stale; fresh sampling",
                "rejected_gap_over_5m": "restart gap over 5m; fresh sampling",
                "history_coverage_under_1h": "history under 1h",
                "history_density_too_low": "history density too low",
                "sampling_disabled": "resume disabled",
            }.get(resume_state, "history not used")
        sample_text = (
            f"{'ON' if self.strategy_config.sampling_enabled else 'OFF'}"
            f" | {sample_count} samples"
            f" | span {self._fmt_duration(sample_coverage_ms // 1_000)}"
            f" | 1h {'可用' if one_hour_sample_ready else '积累中'}"
            f" | JSONL {'ON' if self.strategy_market_sample_writer is not None else 'OFF'}"
            f" | {resume_text}"
            if is_zh
            else
            f"{'ON' if self.strategy_config.sampling_enabled else 'OFF'}"
            f" | {sample_count} samples"
            f" | span {self._fmt_duration(sample_coverage_ms // 1_000)}"
            f" | 1h {'ready' if one_hour_sample_ready else 'warming'}"
            f" | JSONL {'ON' if self.strategy_market_sample_writer is not None else 'OFF'}"
            f" | {resume_text}"
        )
        readiness.add_row("影子样本" if is_zh else "Shadow Samples", sample_text)
        normal_wear_usd = (
            self.strategy_config.order_notional_usd
            * self.strategy_config.max_normal_round_wear_bps
            / Decimal("10000")
        )
        readiness.add_row(
            "开平规则" if is_zh else "Open/Close Rule",
            (
                f"0-{self._fmt_policy_duration(self.strategy_config.early_exit_seconds)}：整轮下界 ≥ 0U"
                f" | 之后 ≥ {self._fmt_money(-normal_wear_usd)}"
                f" | 零磨损线上近10s累计2s可平"
                f" | {self._fmt_policy_duration(self.strategy_config.max_hold_seconds)}提醒"
                if is_zh
                else
                f"0-{self._fmt_policy_duration(self.strategy_config.early_exit_seconds)}: round floor ≥ 0U"
                f" | then ≥ {self._fmt_money(-normal_wear_usd)}"
                f" | 2s positive gross within 10s may close"
                f" | alert at {self._fmt_policy_duration(self.strategy_config.max_hold_seconds)}"
            ),
        )
        cooldown_remaining = self.round_cooldown_remaining_seconds()
        if self._last_round_closed_at <= 0:
            cooldown_text = (
                f"{self.strategy_config.round_cooldown_seconds}s（未开始）"
                if is_zh
                else f"{self.strategy_config.round_cooldown_seconds}s (not started)"
            )
        elif cooldown_remaining > 0:
            cooldown_text = (
                f"剩余 {cooldown_remaining}s"
                if is_zh
                else f"{cooldown_remaining}s remaining"
            )
        else:
            cooldown_text = "已结束" if is_zh else "finished"
        readiness.add_row(
            "轮次结束后冷却" if is_zh else "Post-Round Cooldown",
            cooldown_text,
        )
        readiness.add_row(
            "当前判断" if is_zh else "Decision",
            f"{self._dashboard_action_text(status['decision_action'], is_zh)}"
            f"：{self._dashboard_reason_text(status['decision_reason'], is_zh)}",
        )
        readiness.add_row(
            "运行模式" if is_zh else "Mode",
            f"{mode_text} | {self._dashboard_session_text(self._canary_session_state, is_zh)}"
            f" | {'轮次' if is_zh else 'Rounds'} {self._canary_round_count}",
        )

        economics = Table(
            title="持仓与盈亏评估" if is_zh else "Position & PnL Assessment",
            show_header=False,
            expand=True,
        )
        economics.add_column(
            "field",
            style="bold",
            width=20,
            min_width=20,
            max_width=20,
            no_wrap=True,
        )
        economics.add_column(
            "value",
            ratio=1,
            no_wrap=True,
            overflow="ellipsis",
        )
        position_values = {
            "status": "无持仓" if is_zh else "No position",
            "direction": "-",
            "held": "-",
            "countdown": "-",
            "notional": "-",
            "var_open": "-",
            "lighter_open": "-",
            "open_pnl": "-",
            "close_wear": "-",
            "close_reserve": "-",
            "round_now": "-",
        }
        if current_open is not None:
            held_seconds = record_hold_seconds(current_open)
            close_countdown = (
                max(0, self.strategy_config.early_exit_seconds - held_seconds)
                if held_seconds is not None
                else None
            )
            direction_zh, direction_en = self._direction_labels(current_open.side)
            actual_open_result = leg_result_by_direction(current_open)
            current_round_pnl = (
                close_candidate.round_lower_bound_usd
                if close_candidate is not None
                else None
            )
            position_values.update(
                {
                    "status": self._current_round_status_text(current_open, is_zh),
                    "direction": direction_zh if is_zh else direction_en,
                    "held": self._fmt_dashboard_duration(held_seconds, is_zh),
                    "countdown": self._fmt_dashboard_duration(close_countdown, is_zh),
                    "notional": self._fmt_notional(var_open_notional_usd(current_open)),
                    "var_open": self._fmt_fill_price(current_open.var_fill_price),
                    "lighter_open": self._fmt_fill_price(current_open.lighter_fill_price),
                    "open_pnl": self._fmt_colored_leg_result(actual_open_result),
                    "close_wear": self._fmt_colored_pnl_rate(
                        (
                            close_candidate.expected_close_pnl_usd
                            if close_candidate is not None
                            else None
                        ),
                        (
                            close_candidate.actual_close_rate
                            if close_candidate is not None
                            else None
                        ),
                    ),
                    "close_reserve": self._fmt_colored_money(
                        -close_candidate.close_reserve_usd
                        if close_candidate is not None
                        else None
                    ),
                    "round_now": self._fmt_colored_money(current_round_pnl),
                }
            )
        for zh_label, en_label, key in (
            ("状态", "Status", "status"),
            ("方向", "Direction", "direction"),
            ("持仓时间", "Held", "held"),
            ("平仓倒计时", "Close Countdown", "countdown"),
            ("开仓金额", "Open Notional", "notional"),
            ("Var 开仓价", "Var Open Price", "var_open"),
            ("Lighter 开仓价", "Lighter Open Price", "lighter_open"),
            ("开仓收益", "Open PnL", "open_pnl"),
            ("此时平仓磨损", "Close Wear Now", "close_wear"),
            ("平仓预留", "Close Reserve", "close_reserve"),
            ("当前整轮净估", "Round Lower Bound", "round_now"),
        ):
            economics.add_row(
                zh_label if is_zh else en_label,
                position_values[key],
            )

        execution = Table(
            title="运行状态" if is_zh else "Runtime Status",
            show_header=False,
            expand=True,
        )
        execution.add_column(
            "field",
            style="bold",
            width=16,
            min_width=16,
            max_width=16,
            no_wrap=True,
        )
        execution.add_column(
            "value",
            ratio=1,
            no_wrap=True,
            overflow="ellipsis",
        )
        reconcile_text = self._dashboard_reconcile_text(self.last_reconcile_outcome, is_zh)
        if self.last_account_snapshot is not None:
            reconcile_text += (
                f" | Variational {self.last_account_snapshot.var_position} BTC"
                f" | Lighter {self.last_account_snapshot.lighter_position} BTC"
                f" | {'活动委托' if is_zh else 'Active orders'} {self.last_account_snapshot.lighter_active_orders}"
            )
        execution.add_row(
            "账户对账" if is_zh else "Reconciliation",
            reconcile_text,
        )
        ready_text = "ON" if self.automation_ready else "OFF"
        ready_color = "green" if self.automation_ready else "red"
        execution.add_row(
            "自动化就绪" if is_zh else "Automation Ready",
            f"[{ready_color}]{ready_text}[/{ready_color}]",
        )
        hedge_mode_text = "ON" if self.args.auto_hedge else "OFF"
        hedge_mode_color = "green" if self.args.auto_hedge else "red"
        execution.add_row(
            "自动对冲" if is_zh else "Auto Hedge",
            (
                f"[{hedge_mode_color}]{hedge_mode_text}[/{hedge_mode_color}]"
                f" | {'手动/策略成交' if is_zh else 'manual/strategy fills'}"
            ),
        )
        order_entry_ready = self.lighter_order_entry_is_ready()
        if order_entry_ready:
            order_entry_text = (
                "[green]WS READY[/green] | 低延迟新开仓"
                if is_zh
                else "[green]WS READY[/green] | low-latency opens"
            )
        elif self.lighter_order_entry_enabled and self.lighter_order_entry_rest_fallback:
            order_entry_text = (
                "[yellow]REST FALLBACK[/yellow] | 新开仓暂停，平仓/恢复可用"
                if is_zh
                else "[yellow]REST FALLBACK[/yellow] | opens blocked; close/recovery available"
            )
        elif self.lighter_order_entry_enabled:
            order_entry_text = (
                "[red]UNAVAILABLE[/red] | 新开仓暂停"
                if is_zh
                else "[red]UNAVAILABLE[/red] | opens blocked"
            )
        else:
            order_entry_text = (
                "[red]WS OFF[/red] | 新开仓暂停"
                if is_zh
                else "[red]WS OFF[/red] | opens blocked"
            )
        execution.add_row(
            "Lighter 下单通道" if is_zh else "Lighter Order Link",
            order_entry_text,
        )
        auto_open_on = self.strategy_config.execution_mode == "live"
        auto_open_color = "green" if auto_open_on else "red"
        auto_open_text = "ON" if auto_open_on else "OFF"
        execution.add_row(
            "策略自动开仓" if is_zh else "Strategy Auto Open",
            (
                f"[{auto_open_color}]{auto_open_text}[/{auto_open_color}]"
                f" | {mode_text}"
            ),
        )
        execution.add_row(
            "策略自动平仓" if is_zh else "Strategy Auto Close",
            (
                "[green]ON[/green] | 自适应持仓"
                if is_zh
                else "[green]ON[/green] | adaptive positions"
            ),
        )
        safety_text = "正常" if is_zh else "Normal"
        if self.automation_paused:
            safety_text = (
                f"已暂停：{self._dashboard_pause_text(self.automation_pause_reason, is_zh)}"
                if is_zh
                else f"Paused: {self._dashboard_pause_text(self.automation_pause_reason, is_zh)}"
            )
        command_text = (
            ("命令通道已连接" if command_connected else "命令通道未连接")
            if is_zh
            else ("Command connected" if command_connected else "Command disconnected")
        )
        execution.add_row(
            "自动化保护" if is_zh else "Automation Guard",
            f"{safety_text} | {command_text}",
        )
        if current_open is None:
            execution.add_row("当前持仓" if is_zh else "Position", "无" if is_zh else "None")
        else:
            strategy_tag = (
                "手动"
                if is_zh and current_open.strategy_tag == MANUAL_STRATEGY_TAG
                else current_open.strategy_tag
            )
            direction_zh, direction_en = self._direction_labels(current_open.side)
            position_side = direction_zh if is_zh else direction_en
            execution.add_row(
                "当前持仓" if is_zh else "Position",
                f"{strategy_tag} | {position_side}"
                f" | {self._current_round_status_text(current_open, is_zh)}"
                f" | {'已持有' if is_zh else 'Held'} "
                f"{self._fmt_dashboard_duration(record_hold_seconds(current_open), is_zh)}",
            )

        history = Table(title="最近完成轮次" if is_zh else "Recent Completed Rounds", expand=True)
        history_columns = (
            ("序号", "#"),
            ("标签", "Tag"),
            ("方向", "Direction"),
            ("金额", "Notional"),
            ("轮次盈亏", "Round PnL"),
        )
        for zh_label, en_label in history_columns:
            label = zh_label if is_zh else en_label
            history.add_column(
                label,
                justify=(
                    "right"
                    if zh_label in {"序号", "金额", "轮次盈亏"}
                    else "left"
                ),
                no_wrap=True,
            )
        recent_rounds = list(reversed(rounds[-DASHBOARD_ORDERS:]))
        if not recent_rounds:
            history.add_row("-", "-", "-", "-", "-")
        else:
            total = len(rounds)
            for offset, trade_round in enumerate(recent_rounds):
                direction_zh, direction_en = self._direction_labels(trade_round.open_record.side)
                strategy_tag = (
                    "手动"
                    if is_zh and trade_round.open_record.strategy_tag == MANUAL_STRATEGY_TAG
                    else trade_round.open_record.strategy_tag
                )
                history.add_row(
                    str(total - offset),
                    strategy_tag,
                    direction_zh if is_zh else direction_en,
                    self._fmt_notional(var_open_notional_usd(trade_round.open_record)),
                    self._fmt_colored_money(trade_round.round_pnl),
                )

        return Group(
            header,
            market,
            readiness,
            spreads,
            adaptive,
            economics,
            execution,
            history,
        )

    async def dashboard_loop(self) -> None:
        initial_render = await self.render_dashboard()
        with Live(
            initial_render,
            console=self.dashboard_console,
            auto_refresh=False,
            screen=True,
        ) as live:
            while not self.stop_flag:
                refresh_interval = self.strategy_config.dashboard_refresh_seconds
                await asyncio.sleep(refresh_interval)
                live.update(await self.render_dashboard(), refresh=True)

    async def research_database_sync_loop(self) -> None:
        synchronizer = self.research_database_synchronizer
        if synchronizer is None:
            return
        while not self.stop_flag:
            try:
                await asyncio.to_thread(synchronizer.sync_once)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Research capture is observational. A database or disk problem
                # must never pause, delay, or terminate live order execution.
                synchronizer.sync_failures += 1
                self.logger.warning("Research database sync failed: %s", exc)
            await asyncio.sleep(self.research_database_sync_seconds)

    def _raise_for_exited_critical_tasks(self) -> None:
        """Fail fast for crashed runtime tasks, except during requested shutdown."""

        if self.stop_flag:
            return
        supervised = {
            "trade": self.trade_task,
            "execution-event-writer": self.execution_event_task,
            "adaptive-strategy-sampler": self.strategy_sample_task,
            "strategy-signal": self.strategy_signal_task,
            "reconcile": self.reconcile_task,
            "Lighter-order-watchdog": self.lighter_order_watchdog_task,
            "Var-intent-watchdog": self.var_intent_watchdog_task,
            "dashboard": self.dashboard_task,
        }
        for name, task in supervised.items():
            # A signal can arrive while this health check is walking the task
            # list.  Once shutdown is requested, tasks that observe stop_flag
            # and return are expected to finish normally.
            if self.stop_flag:
                return
            if task is None or not task.done():
                continue
            if task.cancelled():
                raise RuntimeError(f"Critical task stopped unexpectedly: {name}")
            exc = task.exception()
            if exc is not None:
                raise RuntimeError(f"Critical task failed: {name}: {exc}") from exc
            raise RuntimeError(f"Critical task exited unexpectedly: {name}")

    async def run(self) -> None:
        if self.order_log_writer is not None:
            self.order_log_writer.start()
        if self.trace_writer is not None:
            self.trace_writer.start()
        if self.strategy_market_sample_writer is not None:
            # The legacy sample file was append-only.  Trim it before the
            # writer starts so no append/replace race is possible.
            await asyncio.to_thread(
                AsyncJsonlWriter.compact_jsonl_window,
                self.strategy_market_sample_writer.path,
                timestamp_field="sample_timestamp_ms",
                cutoff_ms=(
                    time.time_ns() // 1_000_000
                    - STRATEGY_CACHE_STARTUP_MAX_MS
                ),
            )
            self.strategy_market_sample_writer.start()
        if self.research_database_synchronizer is not None:
            self.research_database_task = asyncio.create_task(
                self.research_database_sync_loop(),
                name="research-database-sync",
            )
        self.setup_signal_handlers()
        self.var_event_accept_after = datetime.now(timezone.utc)
        await self.runtime.start()
        if self.operations_dashboard_enabled:
            self.operations_dashboard_server = OperationsDashboardServer(
                snapshot_factory=self.operations_dashboard_snapshot,
                action_preparer=self.prepare_operations_action,
                action_executor=self.execute_operations_action,
                asset_dir=OPERATIONS_DASHBOARD_ASSET_DIR,
                host=OPERATIONS_DASHBOARD_HOST,
                port=self.operations_dashboard_port,
                refresh_seconds=self.strategy_config.dashboard_refresh_seconds,
            )
            await self.operations_dashboard_server.start()
            self.logger.info(
                "Operations dashboard listening on loopback only: http://%s:%s",
                OPERATIONS_DASHBOARD_HOST,
                self.operations_dashboard_port,
            )
        self.print_startup_next_steps()
        self.logger.info(
            "Listening for Variational forwarder events on ws://%s:%s and ws://%s:%s; command broker ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
            FORWARDER_HOST,
            FORWARDER_COMMAND_PORT,
        )

        await self.wait_for_variational_ready()
        self.logger.info("Variational heartbeat is live")
        await self.wait_for_variational_portfolio_ready()
        self.logger.info("Variational portfolio snapshot is live")
        # Everything received before the initial portfolio is a startup
        # baseline (often including the server's latest historical trade).
        # Recovery uses the portfolio and deterministic Lighter ids instead of
        # replaying those events as new executions.
        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("Captured startup Variational trade cursor seq=%s", self.trade_event_cursor)
        self.initialize_lighter_client()
        await self.prewarm_lighter_order_entry()
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")
        pending_recovery_outcome = VarPortfolioRecoveryOutcome.UNKNOWN
        if self.pending_var_intent is not None:
            await asyncio.sleep(VAR_PORTFOLIO_RECOVERY_DELAY_SECONDS)
            pending_recovery_outcome = (
                await self.inspect_pending_var_intent_from_portfolio()
            )
        restored_hedges_without_probe = (
            await self.prepare_restored_lighter_recovery()
        )
        await self.refresh_pending_lighter_orders()
        for record in restored_hedges_without_probe:
            if record.hedge_status in {"not_started", "queued", "submitting"}:
                self.schedule_lighter_order(record)
        if (
            pending_recovery_outcome
            is VarPortfolioRecoveryOutcome.CONFIRMED_NOT_FILLED
            and self.pending_var_intent is not None
            and self.pending_var_intent.provisional_trade_key
        ):
            await self.rollback_unconfirmed_var_commit()
        await self.reconcile_accounts(allow_resume=True)
        startup_event_count = await self.drain_pending_trade_events()
        if startup_event_count:
            self.logger.info(
                "Processed %s Variational trade events received during startup",
                startup_event_count,
            )
        startup_hedges = [task for task in self.hedge_tasks if not task.done()]
        if startup_hedges:
            await asyncio.wait(startup_hedges, timeout=LIGHTER_FILL_TIMEOUT_SECONDS)
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.execution_event_task = asyncio.create_task(
            self.execution_event_loop(), name="execution-event-writer"
        )
        self.trade_task = asyncio.create_task(self.trade_loop())
        self.strategy_sample_task = asyncio.create_task(
            self.strategy_sample_loop(), name="adaptive-strategy-sampler"
        )
        self.strategy_signal_task = asyncio.create_task(
            self.strategy_signal_loop(), name="strategy-signal-loop"
        )
        self.reconcile_task = asyncio.create_task(self.reconcile_loop())
        self.lighter_order_watchdog_task = asyncio.create_task(self.lighter_order_watchdog_loop())
        self.var_intent_watchdog_task = asyncio.create_task(self.var_intent_watchdog_loop())
        if getattr(self.args, "dashboard", True):
            self.dashboard_task = asyncio.create_task(
                self.dashboard_loop(), name="terminal-dashboard"
            )
        else:
            self.logger.info(
                "Terminal dashboard rendering disabled; trading, risk, telemetry, and logs remain active"
            )

        while not self.stop_flag:
            await asyncio.sleep(0.25)
            if self.stop_flag:
                break
            self._raise_for_exited_critical_tasks()

    async def close(self) -> None:
        self.stop_flag = True

        if self.operations_dashboard_server is not None:
            await self.operations_dashboard_server.stop()
            self.operations_dashboard_server = None

        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self.strategy_signal_task and not self.strategy_signal_task.done():
            self.strategy_signal_task.cancel()
            await asyncio.gather(self.strategy_signal_task, return_exceptions=True)

        if self.strategy_sample_task and not self.strategy_sample_task.done():
            self.strategy_sample_task.cancel()
            await asyncio.gather(self.strategy_sample_task, return_exceptions=True)

        if self.reconcile_task and not self.reconcile_task.done():
            self.reconcile_task.cancel()
            await asyncio.gather(self.reconcile_task, return_exceptions=True)

        if self.lighter_order_watchdog_task and not self.lighter_order_watchdog_task.done():
            self.lighter_order_watchdog_task.cancel()
            await asyncio.gather(self.lighter_order_watchdog_task, return_exceptions=True)

        if self.var_intent_watchdog_task and not self.var_intent_watchdog_task.done():
            self.var_intent_watchdog_task.cancel()
            await asyncio.gather(self.var_intent_watchdog_task, return_exceptions=True)

        # Do not tear down order-entry, private fills, or the single execution
        # writer while an IOC is still signing/sending/reconciling.  This grace
        # period is shutdown-only and never touches the live order hot path.
        active_hedges = [task for task in self.hedge_tasks if not task.done()]
        if active_hedges:
            _done, pending_hedges = await asyncio.wait(
                active_hedges,
                timeout=(
                    LIGHTER_ORDER_ENTRY_RESPONSE_TIMEOUT_SECONDS
                    + LIGHTER_ERROR_CONFIRM_SECONDS
                    + 5.0
                ),
            )
            if pending_hedges:
                self.logger.warning(
                    "Shutdown grace expired with %s Lighter hedge task(s) still active; "
                    "persisting them for deterministic restart reconciliation",
                    len(pending_hedges),
                )
                async with self._record_lock:
                    for trade_key, task in self.lighter_order_tasks_by_trade_key.items():
                        if task not in pending_hedges:
                            continue
                        record = self.records.get(trade_key)
                        if record is None or record.hedge_status in {
                            "filled",
                            "overfilled",
                        }:
                            continue
                        record.execution_state = EXECUTION_STATE_RECOVERY_REQUIRED
                        if not record.hedge_error:
                            record.hedge_error = (
                                "Runtime stopped while Lighter hedge outcome was unresolved; "
                                "reconcile deterministic client order id on restart"
                            )
                with contextlib.suppress(Exception):
                    await self.persist_runtime_state()
                for task in pending_hedges:
                    task.cancel()
                await asyncio.gather(*pending_hedges, return_exceptions=True)

        await self.stop_lighter_streams()

        if self.lighter_order_entry is not None:
            await self.lighter_order_entry.close()

        remaining_hedges = [task for task in self.hedge_tasks if not task.done()]
        if remaining_hedges:
            for task in remaining_hedges:
                task.cancel()
            await asyncio.gather(*remaining_hedges, return_exceptions=True)
        self.hedge_tasks.clear()

        if self.execution_event_task and not self.execution_event_task.done():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.execution_event_queue.join(),
                    timeout=1.0,
                )
            self.execution_event_task.cancel()
            await asyncio.gather(self.execution_event_task, return_exceptions=True)

        with contextlib.suppress(Exception):
            await self.persist_runtime_state()

        if self.lighter_client is not None:
            close_method = getattr(self.lighter_client, "close", None)
            if callable(close_method):
                with contextlib.suppress(Exception):
                    close_result = close_method()
                    if asyncio.iscoroutine(close_result):
                        await close_result

        await self.runtime.stop()
        if self.order_log_writer is not None:
            await self.order_log_writer.close()
        if self.trace_writer is not None:
            await self.trace_writer.close()
        if self.strategy_market_sample_writer is not None:
            await self.strategy_market_sample_writer.close()
        if self.research_database_synchronizer is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    self.research_database_synchronizer.sync_once
                )
        if self.research_database_task and not self.research_database_task.done():
            self.research_database_task.cancel()
            await asyncio.gather(
                self.research_database_task,
                return_exceptions=True,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Var-Lit V1 BTC execution and hedge runtime."
    )
    parser.add_argument(
        "--lang",
        choices=["zh", "en"],
        default="zh",
        help="Dashboard language: zh (Chinese) or en (English). Default: zh",
    )
    parser.add_argument(
        "--no-hedge",
        action="store_false",
        dest="auto_hedge",
        help="Disable automatic Lighter hedge placement (default: enabled)",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_false",
        dest="dashboard",
        help=(
            "Disable terminal dashboard rendering while keeping all market, "
            "strategy, risk, telemetry, and trading tasks active"
        ),
    )
    parser.set_defaults(auto_hedge=True, dashboard=True)
    return parser.parse_args()


async def _amain() -> None:
    load_runtime_env()
    configure_runtime_paths()
    args = parse_args()
    runtime = VariationalToLighterRuntime(args)
    try:
        await runtime.run()
    finally:
        await runtime.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
