"""Periodic 5m/30m/1h robust statistics outside the strategy hot path."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .models import DirectionalRates, Side, WindowStats


ZERO = Decimal("0")


def _median(ordered: list[Decimal]) -> Decimal:
    count = len(ordered)
    midpoint = count // 2
    if count % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal("2")


def _nearest_rank(ordered: list[Decimal], percentile: int) -> Decimal:
    if not 1 <= percentile <= 100:
        raise ValueError("percentile must be between 1 and 100")
    rank = (percentile * len(ordered) + 99) // 100
    return ordered[rank - 1]


@dataclass(frozen=True, slots=True)
class _Sample:
    timestamp_ms: int
    rates: DirectionalRates
    bridges_previous: bool = False


class RollingWindowStore:
    """A bounded sample store scanned only by the cold-path compiler."""

    # All three windows are formal strategy inputs.  Disk retention is managed
    # independently; only this bounded rolling hour reaches the compiler.
    WINDOWS = (5, 30, 60)
    MAX_WINDOW_MS = 60 * 60 * 1_000

    def __init__(
        self,
        *,
        minimum_density_per_second: Decimal = Decimal("0.10"),
        maximum_gap_ms: int = 60_000,
        maximum_latest_age_ms: int = 2_000,
    ) -> None:
        if minimum_density_per_second <= ZERO:
            raise ValueError("minimum density must be positive")
        if maximum_gap_ms <= 0 or maximum_latest_age_ms <= 0:
            raise ValueError("gap and latest-age limits must be positive")
        self.minimum_density_per_second = minimum_density_per_second
        self.maximum_gap_ms = maximum_gap_ms
        self.maximum_latest_age_ms = maximum_latest_age_ms
        self._samples: deque[_Sample] = deque()

    def add(
        self,
        *,
        timestamp_ms: int,
        rates: DirectionalRates,
        bridges_previous: bool = False,
    ) -> None:
        if self._samples and timestamp_ms < self._samples[-1].timestamp_ms:
            raise ValueError("samples must be chronological")
        if self._samples and timestamp_ms == self._samples[-1].timestamp_ms:
            self._samples[-1] = _Sample(
                timestamp_ms,
                rates,
                self._samples[-1].bridges_previous,
            )
        else:
            self._samples.append(_Sample(timestamp_ms, rates, bridges_previous))
        # Keep one maximum-gap interval beyond 1h.  The compiler uses the last
        # sample at or before the window boundary as a coverage anchor, so
        # millisecond scheduler drift cannot make a continuously sampled 1h
        # window remain permanently one tick short.
        cutoff = timestamp_ms - self.MAX_WINDOW_MS - self.maximum_gap_ms
        while self._samples and self._samples[0].timestamp_ms < cutoff:
            self._samples.popleft()

    def frozen_copy(self) -> "FrozenRollingWindowStore":
        """Return an immutable sample copy suitable for a worker thread.

        The live store remains owned by the event-loop thread.  Copying its
        already-immutable samples into a tuple creates a detached input that a
        parameter compiler can scan without racing later ``add`` calls.
        """

        return FrozenRollingWindowStore(
            minimum_density_per_second=self.minimum_density_per_second,
            maximum_gap_ms=self.maximum_gap_ms,
            maximum_latest_age_ms=self.maximum_latest_age_ms,
            _samples=tuple(self._samples),
        )

    def coverage(self) -> tuple[int, int]:
        """Return valid in-memory sample count and elapsed coverage in ms."""

        if not self._samples:
            return 0, 0
        return (
            len(self._samples),
            max(0, self._samples[-1].timestamp_ms - self._samples[0].timestamp_ms),
        )

    def snapshot(
        self,
        *,
        now_ms: int,
    ) -> Mapping[Side, Mapping[int, WindowStats]]:
        result: dict[Side, dict[int, WindowStats]] = {Side.BUY: {}, Side.SELL: {}}
        for minutes in self.WINDOWS:
            cutoff = now_ms - minutes * 60 * 1_000
            samples = [sample for sample in self._samples if sample.timestamp_ms >= cutoff]
            coverage_anchor_ms = next(
                (
                    sample.timestamp_ms
                    for sample in reversed(self._samples)
                    if sample.timestamp_ms <= cutoff
                ),
                None,
            )
            for side in Side:
                live = self._window_stats(
                    side=side,
                    minutes=minutes,
                    samples=samples,
                    now_ms=now_ms,
                    coverage_anchor_ms=coverage_anchor_ms,
                )
                result[side][minutes] = live
        return result

    def _window_stats(
        self,
        *,
        side: Side,
        minutes: int,
        samples: list[_Sample],
        now_ms: int,
        coverage_anchor_ms: int | None,
    ) -> WindowStats:
        if not samples:
            return WindowStats(
                side=side,
                window_minutes=minutes,
                median=ZERO,
                q80=ZERO,
                mad=ZERO,
                sample_count=0,
                span_ms=0,
                density_per_second=ZERO,
                max_gap_ms=0,
                latest_age_ms=0,
                ready=False,
                reason="empty_window",
                q95=None,
            )
        ordered = sorted(sample.rates.for_side(side) for sample in samples)
        median = _median(ordered)
        deviations = sorted(abs(value - median) for value in ordered)
        span_ms = samples[-1].timestamp_ms - samples[0].timestamp_ms
        latest_age_ms = max(0, now_ms - samples[-1].timestamp_ms)
        max_gap_ms = max(
            (
                right.timestamp_ms - left.timestamp_ms
                for left, right in zip(samples, samples[1:])
                if not right.bridges_previous
            ),
            default=0,
        )
        if (
            coverage_anchor_ms is not None
            and coverage_anchor_ms < samples[0].timestamp_ms
            and not samples[0].bridges_previous
        ):
            max_gap_ms = max(max_gap_ms, samples[0].timestamp_ms - coverage_anchor_ms)
        # Measure against the requested wall-clock window, not merely the span
        # between the first and last surviving samples.  Otherwise one fresh
        # tick after a restart could appear artificially dense.
        density = Decimal(len(samples)) / Decimal(minutes * 60)
        cutoff = now_ms - minutes * 60 * 1_000
        reason = "ready"
        ready = True
        if coverage_anchor_ms is None or coverage_anchor_ms > cutoff:
            ready, reason = False, "insufficient_span"
        elif density < self.minimum_density_per_second:
            ready, reason = False, "insufficient_density"
        elif max_gap_ms >= self.maximum_gap_ms:
            ready, reason = False, "data_gap"
        elif latest_age_ms > self.maximum_latest_age_ms:
            ready, reason = False, "latest_sample_stale"
        return WindowStats(
            side=side,
            window_minutes=minutes,
            median=median,
            q80=_nearest_rank(ordered, 80),
            mad=_median(deviations),
            sample_count=len(samples),
            span_ms=span_ms,
            density_per_second=density,
            max_gap_ms=max_gap_ms,
            latest_age_ms=latest_age_ms,
            ready=ready,
            reason=reason,
            source="live",
            q95=_nearest_rank(ordered, 95),
        )


@dataclass(frozen=True, slots=True)
class FrozenRollingWindowStore:
    """Detached, read-only input for cold-path statistics compilation."""

    minimum_density_per_second: Decimal
    maximum_gap_ms: int
    maximum_latest_age_ms: int
    _samples: tuple[_Sample, ...]

    def snapshot(
        self,
        *,
        now_ms: int,
    ) -> Mapping[Side, Mapping[int, WindowStats]]:
        # Reuse the single validated statistics implementation.  This working
        # deque exists only inside the caller (normally a background thread),
        # while the frozen tuple and the live event-loop store stay untouched.
        working = RollingWindowStore(
            minimum_density_per_second=self.minimum_density_per_second,
            maximum_gap_ms=self.maximum_gap_ms,
            maximum_latest_age_ms=self.maximum_latest_age_ms,
        )
        working._samples = deque(self._samples)
        return working.snapshot(now_ms=now_ms)
