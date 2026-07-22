"""Adaptive median strategy domain package.

The package owns strategy mathematics and decisions only.  Runtime I/O,
exchange clients and order execution remain in :mod:`main`.
"""

from .engine import (
    close_floor_usd,
    CLOSE_RESERVE_MULTIPLIER,
    LONG_HOLD_FLOOR_START_SECONDS,
    LONG_HOLD_FLOOR_STEP_SECONDS,
    LONG_HOLD_FLOOR_STEP_BPS,
    StrategyEngine,
    long_hold_floor_adjustment_usd,
)
from .execution_survival import (
    ExecutionSurvivalModel,
    SurvivalCalibration,
    load_execution_survival_model,
)
from .model_config import ModelConfig, load_model_config
from .models import (
    Action,
    CloseCandidate,
    Decision,
    DirectionalRates,
    DirectionalThresholds,
    MarketFrame,
    OpenCandidate,
    ParameterEpoch,
    PositionContext,
    Side,
    SourceClock,
    ThresholdComponents,
    WindowStats,
)
from .parameters import (
    EpochActivator,
    OpportunitySample,
    build_parameter_candidate,
    compile_baseline,
    compile_entry_opportunity,
    compile_exit_opportunity,
    compile_q80,
    opportunity_balance_threshold,
)
from .statistics import RollingWindowStore
from .serialization import (
    epoch_from_payload,
    epoch_to_payload,
    open_candidate_from_payload,
    open_candidate_to_payload,
)

__all__ = [
    "Action",
    "CloseCandidate",
    "CLOSE_RESERVE_MULTIPLIER",
    "Decision",
    "DirectionalRates",
    "DirectionalThresholds",
    "EpochActivator",
    "ExecutionSurvivalModel",
    "MarketFrame",
    "ModelConfig",
    "OpenCandidate",
    "OpportunitySample",
    "ParameterEpoch",
    "PositionContext",
    "RollingWindowStore",
    "Side",
    "SourceClock",
    "StrategyEngine",
    "LONG_HOLD_FLOOR_START_SECONDS",
    "LONG_HOLD_FLOOR_STEP_SECONDS",
    "LONG_HOLD_FLOOR_STEP_BPS",
    "SurvivalCalibration",
    "ThresholdComponents",
    "WindowStats",
    "build_parameter_candidate",
    "close_floor_usd",
    "compile_baseline",
    "compile_entry_opportunity",
    "compile_exit_opportunity",
    "compile_q80",
    "load_model_config",
    "load_execution_survival_model",
    "long_hold_floor_adjustment_usd",
    "opportunity_balance_threshold",
    "epoch_from_payload",
    "epoch_to_payload",
    "open_candidate_from_payload",
    "open_candidate_to_payload",
]
