"""Adaptive median strategy domain package.

The package owns strategy mathematics and decisions only.  Runtime I/O,
exchange clients and order execution remain in :mod:`main`.
"""

from .engine import CLOSE_RESERVE_MULTIPLIER, StrategyEngine
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
    "ThresholdComponents",
    "WindowStats",
    "build_parameter_candidate",
    "compile_baseline",
    "compile_entry_opportunity",
    "compile_exit_opportunity",
    "compile_q80",
    "load_model_config",
    "opportunity_balance_threshold",
    "epoch_from_payload",
    "epoch_to_payload",
    "open_candidate_from_payload",
    "open_candidate_to_payload",
]
