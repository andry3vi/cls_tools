"""Scan exposure: how often the scan stood on each set voltage.

The DAQ steps the set voltage through the ScanningRanges in their listed order, each from its
first to its second value in StepSize steps, and then

- runs the same sequence backwards ('updown'): the last and the first set voltage are dwelt on
  twice in a row, at the turn. Most runs since November 2024;
- or starts again from the first set voltage ('sawtooth'): most runs before November 2024, and
  some later ones.

Each dwell lasts BunchesPerChannel triggers plus a settling time, so a cycle gives every set
voltage the same exposure in both patterns (a range listed twice gets twice the exposure).

Counting visits from the events, as changes of DV, gets the exposure wrong in two ways:

- the two dwells at each turn of an up-down scan merge into one visit, so the end points look
  half visited, although they count as much as their neighbours (Bi 8244: 428 and 480 events at
  -130 and 530 V, 426-516 at the next set voltages);
- a dwell without events is not seen. Below about 2 events per dwell (online runs: 1.1-1.3)
  quiet set voltages look under-visited, and more so the ones that happened to count less.

`scan_dwells` reconstructs the scan instead: it places each observed visit in the cycle, at the
position closest to what the triggers elapsed since the previous visit predict, and counts the
dwells of every set voltage between the first and the last event. The trigger counter is not an
exact clock of the steps (settling triggers, 1.0-1.2 triggers per step in runs with one trigger
per step), so the position is predicted from the median triggers per step.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise

import numpy as np

PATTERNS = ("updown", "sawtooth")
SELECT_VISITS = 2000  # visits used to choose the pattern and the starting position
_DECIMALS = 6  # set voltages are compared after rounding to 1 uV


@dataclass(frozen=True)
class ScanDwells:
    """Dwells of the scan on each set voltage, between a run's first and last event."""

    dwells: dict[float, int]  # set voltage [V] -> dwells
    per_cycle: dict[float, int]  # set voltage [V] -> dwells per scan cycle
    pattern: str  # 'updown' or 'sawtooth'
    sequence_from: str  # 'ranges' (ScanningRanges and StepSize) or 'events' (observed DVs)
    triggers_per_dwell: float  # median triggers from one dwell to the next
    cycles: float  # dwells in the run / dwells per cycle
    residual: float  # mean |position - predicted position| [dwells]; 0 for a perfect clock

    def sweeps(self) -> dict[float, float]:
        """Dwells per set voltage divided by its dwells per cycle: equal for all set voltages
        of a run, up to the partial cycles at its start and end."""
        return {v: n / self.per_cycle[v] for v, n in self.dwells.items()}


def scan_sequence(ranges: Sequence[Sequence[float]], step: float) -> np.ndarray:
    """Set voltages of one sweep: the ranges in their listed order, each from its first to its
    second value in steps of `step` (rounded to 1 uV)."""
    parts = []
    step = abs(float(step))
    for r in ranges:
        a, b = float(r[0]), float(r[-1])
        if a == b or step == 0:
            parts.append(np.array([a]))
            continue
        s = step if b > a else -step
        n = round((b - a) / s)
        parts.append(a + s * np.arange(n + 1))
    return np.round(np.concatenate(parts), _DECIMALS) if parts else np.zeros(0)


def _cycle(sweep: np.ndarray, pattern: str) -> np.ndarray:
    return np.r_[sweep, sweep[::-1]] if pattern == "updown" else sweep


def _visits(dv: np.ndarray, bunch: np.ndarray) -> tuple[list[float], list[int]]:
    """Set voltage and first trigger of every visit (a run of events with one DV)."""
    first = np.r_[0, np.flatnonzero(np.diff(dv) != 0) + 1]
    return dv[first].tolist(), bunch[first].tolist()


def _triggers_per_dwell(values: list[float], starts: list[int], sweep: np.ndarray) -> float:
    """Median triggers between consecutive visits of neighbouring set voltages of the sweep."""
    nb = {(float(a), float(b)) for a, b in pairwise(sweep.tolist()) if a != b}
    nb |= {(b, a) for a, b in nb}
    gaps = [
        s1 - s0
        for (v0, v1), (s0, s1) in zip(pairwise(values), pairwise(starts), strict=True)
        if (v0, v1) in nb
    ]
    return float(np.median(gaps)) if len(gaps) >= 5 else math.nan


def _track(
    values: list[float],
    starts: list[int],
    occ: dict[float, list[int]],
    period: int,
    tau: float,
    first: int,
) -> tuple[list[int], float]:
    """Position in the unrolled cycle of every visit, and the summed |position - prediction|.
    Consecutive visits have different set voltages, so each one moves at least one dwell on."""
    k = first
    pos = [k]
    cost = 0.0
    for i in range(1, len(values)):
        pred = k + (starts[i] - starts[i - 1]) / tau
        best, best_d = k + 1, math.inf
        for ph in occ[values[i]]:
            m = math.floor((pred - ph) / period)
            for p in (ph + m * period, ph + (m + 1) * period):
                if p <= k:
                    p += period * ((k - p) // period + 1)
                d = abs(p - pred)
                if d < best_d:
                    best, best_d = p, d
        cost += best_d
        k = best
        pos.append(k)
    return pos, cost


def _fit(
    values: list[float], starts: list[int], cycle: np.ndarray, tau: float
) -> tuple[list[int], float]:
    """Track with the starting position (among the first set voltage's places in the cycle)
    that fits the first SELECT_VISITS visits best."""
    occ: dict[float, list[int]] = {}
    for i, v in enumerate(cycle.tolist()):
        occ.setdefault(v, []).append(i)
    n = min(len(values), SELECT_VISITS)
    first = min(
        occ[values[0]],
        key=lambda ph: _track(values[:n], starts[:n], occ, len(cycle), tau, ph)[1],
    )
    return _track(values, starts, occ, len(cycle), tau, first)


def scan_dwells(
    dv: np.ndarray,
    bunch: np.ndarray,
    ranges: Sequence[Sequence[float]] | None = None,
    step: float | None = None,
    pattern: str = "auto",
) -> ScanDwells:
    """Dwells of the scan on each set voltage (see the module docstring).

    dv, bunch: set voltage and trigger number of every event, in time order (Bunch, then TOF).
    ranges, step: ScanningRanges and StepSize of the run. Without them, or when the events have
    set voltages outside them, the sweep is the observed set voltages in ascending order.
    pattern: 'updown', 'sawtooth', or 'auto' (the one whose positions follow the triggers best).
    """
    if pattern not in ("auto", *PATTERNS):
        raise ValueError(f"pattern must be 'auto', 'updown' or 'sawtooth', not {pattern!r}")
    dv = np.round(np.asarray(dv, dtype=float), _DECIMALS)
    bunch = np.asarray(bunch, dtype=np.int64)
    observed = np.unique(dv)
    sweep, source = np.zeros(0), "events"
    if ranges is not None and step is not None and len(ranges):
        sweep = scan_sequence(ranges, step)
        source = "ranges"
    if not len(sweep) or not np.isin(observed, sweep).all():
        sweep, source = observed, "events"
    if len(dv) == 0:
        return ScanDwells({}, {}, "updown", source, math.nan, 0.0, 0.0)

    values, starts = _visits(dv, bunch)
    tau = _triggers_per_dwell(values, starts, sweep)
    if not np.isfinite(tau) or tau <= 0:
        span = starts[-1] - starts[0]
        tau = span / max(len(values) - 1, 1) if span > 0 else 1.0

    candidates = PATTERNS if pattern == "auto" else (pattern,)
    if source == "events" and pattern in ("auto", "sawtooth"):
        # without the ranges the direction of a sawtooth is not known
        fits = [
            (p, s)
            for p in candidates
            for s in ((sweep, sweep[::-1]) if p == "sawtooth" else (sweep,))
        ]
    else:
        fits = [(p, sweep) for p in candidates]
    if len(fits) > 1:
        n = min(len(values), SELECT_VISITS)
        fits = [min(fits, key=lambda f: _fit(values[:n], starts[:n], _cycle(f[1], f[0]), tau)[1])]
    chosen, sw = fits[0]
    cycle = _cycle(sw, chosen)
    pos, cost = _fit(values, starts, cycle, tau)

    period = len(cycle)
    n_dwells = pos[-1] - pos[0] + 1
    per_phase = np.full(period, n_dwells // period)
    rest = n_dwells % period
    per_phase[(pos[0] + np.arange(rest)) % period] += 1
    dwells: dict[float, int] = {}
    per_cycle: dict[float, int] = {}
    for v, n in zip(cycle.tolist(), per_phase.tolist(), strict=True):
        dwells[v] = dwells.get(v, 0) + int(n)
        per_cycle[v] = per_cycle.get(v, 0) + 1
    keep = set(observed.tolist())
    return ScanDwells(
        dwells={v: n for v, n in dwells.items() if v in keep},
        per_cycle={v: n for v, n in per_cycle.items() if v in keep},
        pattern=chosen,
        sequence_from=source,
        triggers_per_dwell=float(tau),
        cycles=n_dwells / period,
        residual=cost / max(len(values) - 1, 1),
    )
