# adaptive-median-v1 Architecture

`adaptive-median-v1` fully replaces the fixed-threshold decision layer. Git history is the only archive of the prior strategy; the runtime has no fallback or comparison path. The proven Variational Firm Quote/Commit, Lighter Market IOC hedge, account reconciliation, recovery, and manual-trade hedge infrastructure remains intact.

## Runtime modes

- `observe` compiles parameters, records samples, and displays candidates, but never opens a position.
- `canary` requires a new one-use token, a fresh flat-account reconciliation, no active Lighter orders or pending intent, at least one hour of observation, and two confirmed parameter proposals.
- One completed adaptive round enters `REVIEW_REQUIRED` and consumes the token. A new token is an explicit review acknowledgement for another one-round canary; there is no unlimited production mode.
- Hedge failures, account inconsistency, a new-open Firm amount above 200U, or a risk-limit breach enter `HALTED`. Existing positions close by their frozen BTC quantity, whose notional may naturally exceed 200U after a price move.
- Execution mode controls only new opens. Managed closes, reconciliation, and recovery for an existing adaptive position remain active.

Manual Variational trades are tagged `manual` and still receive an automatic Lighter hedge, but the adaptive strategy never closes them.

## Pure strategy boundary

`adaptive_strategy/` performs no network, disk, or execution-client I/O. Immutable `MarketFrame`, `WindowStats`, `ParameterEpoch`, `OpenCandidate`, and `CloseCandidate` values enter `StrategyEngine`; it returns a `Decision` with an explicit reason code. `main.py` owns market adaptation, authorization, and execution.

The hot path reads one current `MarketFrame` and one frozen `ParameterEpoch` and performs O(1) comparisons. Window scans, statistics, JSON writes, and network requests are outside it. An in-flight candidate always keeps its original epoch.

## Parameter formula

For direction `d`:

```text
B_d = 0.15×M5m_d + 0.55×M30m_d + 0.30×M1h_d
Q_d = 0.15×Q80_5m_d + 0.55×Q80_30m_d + 0.30×Q80_1h_d

T_econ_d = -wear - B_opposite + reserve_open_d + reserve_close_opposite
T_open_d = max(Q_d, T_econ_d, T_balance_d)
```

The 30-minute window has the largest weight because it matches the intended holding horizon. The 5-minute window is a smaller short-term correction used with the live 500U screen, actual 200U depth, and Firm Guard. The one-hour window limits short-horizon noise.

v1 fixes normal round wear at 1.0bps and provisional reserve at 0.50bps per leg for each open and close phase. Candidate parameters are compiled every five minutes. The first fully qualified 5m/30m/1h proposal activates at about 60 minutes. Later changes require two consecutive proposals across a `0.25×MAD1h` deadband; each activation step is capped at `0.50×MAD1h`, and an epoch remains frozen for at least ten minutes.

Samples store both the 500U reference depth and 200U execution depth. All live 5m, 30m, and 1h windows must be complete and healthy before parameters can activate; no four-hour prior substitutes for a live window. Raw JSONL remains append-only, so longer windows can still be researched offline without entering the production formula. A data gap of 60 seconds, stale statistics, or an expired epoch blocks new opens while sampling, managed close, recovery, and reconciliation continue.

Every 200ms dashboard pass rebuilds BBO, 500U/200U spreads, and estimated open PnL from the same latest display frame. Window medians publish after every valid one-second sample. The first complete formal threshold activates at 60 minutes; later changes compile every five minutes, require two confirmations, and remain epoch-frozen. Faster display updates do not bypass parameter stability.

Opportunity samples no more than 15 seconds apart form one event. If one direction has more than twice the other direction's one-hour event count, only the overactive direction is tightened; the weak direction is never loosened.

## Firm Guard and exits

The 500U reference rate performs the initial screen. If both directions pass, the larger standardized excess wins; an equal score is resolved by the larger actual-round lower bound. The existing single Firm Quote is then rechecked with the frozen threshold and current Lighter depth for the exact Firm quantity. A new-open Firm notional above 200U or above the frozen order amount is rejected before Commit and halts canary. Closing an existing position uses its frozen BTC quantity and is not blocked when market movement makes the close notional exceed 200U.

Closing has two independent requirements. The current close-side rate must first regress to at least the opposite weighted baseline frozen at entry. The round lower bound after close reserve must also be non-negative before 30 minutes; from 30 minutes onward, it must be at least `-1.0bps` of actual open notional. At 120 minutes the runtime raises an alert but keeps both controlled conditions; it never forces an unconditional market exit.

Execution-wear samples are report-only. They cannot change reserve or thresholds online; any change requires an audited new model version.

## Deployment

The state schema is v2. An empty pre-v2 state is reinitialized; a pre-v2 state containing a position or pending intent is refused. Adaptive opens must carry a model-matching frozen epoch. Start in `observe`, verify one hour of clean data and the immediately activated first epoch, then explicitly configure `canary` with a new token for one 200U round. Later epoch changes still require two confirmations.

The command channel uses a separate `VARIATIONAL_COMMAND_AUTH_TOKEN` of at least 32 characters, and the exact same value must be entered in the Chrome extension popup. It is not the one-use canary token. After upgrading the extension, clear the old template cache and verify that all four long/short open/close order and quote templates are ready. This release phase validates macOS only; the Windows bundle remains frozen until the macOS live path has completed end-to-end testing.
