"""PMT counting statistics: a Poisson check of the counts per trigger, afterpulse bursts, and an
artificial per-PMT dead time.

Each raw event carries its trigger number (Bunch) and its time inside the acquisition window
(TOF, us); triggers come at a fixed period, so the event time is

    t = Bunch * period + TOF   [us]

Counts per trigger are compared with Poisson statistics by their Fano factor (variance / mean).
The rate changes along the scan, so the counts are taken per scan step and pooled over steps.
Only the triggers strictly between a step visit's first and last event are used: those two are
known to hold an event, and empty triggers before or after them cannot be told from the DAQ's
settling gaps, while the triggers in between are an unbiased sample (zeros included). Runs with
one or two triggers per step (BunchesPerChannel 1) have no such triggers and get no Fano factor.
A healthy PMT counting uncorrelated photons gives 1.

In the Bi offline runs (October 2026) the Fano factor was 1.2-2.0 in every run: the PMTs produce
bursts of afterpulses within a few us (tail to ~50 us), seen as same-PMT pairs within 2 us at
10-300x the uncorrelated rate while pairs across PMTs were not in excess, with one PMT several
times worse than the other. A 5 us extending dead time per PMT removed 2-5 % of the events and
brought the Fano factor to 0.96-0.98; the fits went from chi2_red ~2 to 1.09.

Indicators, per PMT:
    fano            Fano factor of the counts per trigger, with its error under Poisson counting
    burst_fraction  fraction of events less than BURST_WINDOW_US after the previous event of the
                    same PMT; burst_expected is the same for uncorrelated counting with the run's
                    own ToF profile
    fano_dead       Fano factor after a BURST_WINDOW_US dead time, i.e. what apply_dead_time
                    would give
and for the summed counts, the excess of their Fano factor over the PMTs' own (correlated
fluctuations: beam or laser intensity, noise bursts on all channels).

Only the Fano factors raise a CountingWarning (|F - 1| > FANO_TOL and > FANO_SIGMA sigma, PMTs
with at least MIN_EVENTS events); the burst indicators say whether a dead time would cure it.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np

BURST_WINDOW_US = 5.0  # afterpulse window; also the default dead time
FANO_TOL = 0.1  # |Fano - 1| allowed, if also more than FANO_SIGMA sigma
FANO_SIGMA = 5.0
# burst_fraction - burst_expected above which the warning names afterpulses as the cause. Not a
# warning on its own: with bunched beams the light comes in bursts too, more than the run-averaged
# ToF profile predicts (7-28 % in online runs whose Fano factor is 1).
BURST_WARN = 0.01
MIN_EVENTS = 1000  # fewer events on a PMT: indicators reported, no warning
MIN_TRIGGERS_PER_STEP = 20  # scan steps with fewer triggers are left out of the Fano factor
DEFAULT_PERIOD_US = 1000.0


class CountingWarning(UserWarning):
    """The PMT counts do not look like Poisson counting."""


def warn(message: str) -> None:
    warnings.warn(message, CountingWarning, stacklevel=3)


@dataclass(frozen=True)
class CountingIssue:
    kind: str  # "super_poisson", "sub_poisson", "correlated"
    channel: int | None  # None for the summed counts
    message: str


@dataclass(frozen=True)
class PMTCounting:
    channel: int
    events: int
    rate_cps: float  # counts per second of trigger time inside the scan steps
    fano: float
    fano_err: float
    burst_fraction: float
    burst_expected: float
    fano_dead: float

    @property
    def burst_excess(self) -> float:
        return self.burst_fraction - self.burst_expected


@dataclass(frozen=True)
class CountingCheck:
    period_us: float
    n_triggers: int  # first to last trigger of the run, for the rates
    n_fano_triggers: int  # triggers inside the scan steps the Fano factors are taken on
    pmts: dict[int, PMTCounting]
    fano_all: float  # summed counts of all PMTs
    fano_all_err: float
    correlated_excess: float  # fano_all minus the rate-weighted Fano of the single PMTs
    issues: tuple[CountingIssue, ...]

    @property
    def kinds(self) -> set[str]:
        return {issue.kind for issue in self.issues}


def trigger_period_us(ts: np.ndarray, bunch: np.ndarray, tof: np.ndarray) -> float:
    """Trigger period [us] from the run's own timestamps: TS span over trigger span, to 3
    significant digits (TS has a 10 ms resolution, so the run must last >= 10 s). Falls back to
    max(DEFAULT_PERIOD_US, max TOF) for runs too short to tell."""
    fallback = DEFAULT_PERIOD_US
    if len(tof):
        fallback = max(fallback, float(np.ceil(np.max(tof))))
    if len(ts) < 2:
        return fallback
    duration = float(np.max(ts) - np.min(ts))
    span = float(np.max(bunch) - np.min(bunch))
    if duration < 10.0 or span < 1:
        return fallback
    return float(f"{1e6 * duration / span:.3g}")


def event_time_us(bunch: np.ndarray, tof: np.ndarray, period_us: float) -> np.ndarray:
    return np.asarray(bunch, dtype=np.float64) * period_us + np.asarray(tof, dtype=np.float64)


def dead_time_keep(
    bunch: np.ndarray,
    tdc: np.ndarray,
    tof: np.ndarray,
    period_us: float,
    dead_time_us: float | Mapping[int, float],
) -> np.ndarray:
    """Events kept by an extending (paralysable) dead time per PMT: an event is dropped when the
    previous event of the same PMT, kept or not, is closer than the dead time. dead_time_us is
    one value for all PMTs or {channel: us}; channels not in the mapping, or with <= 0, keep all
    their events."""
    t = event_time_us(bunch, tof, period_us)
    tdc = np.asarray(tdc)
    keep = np.ones(len(t), dtype=bool)
    for channel in np.unique(tdc):
        if isinstance(dead_time_us, Mapping):
            dead = float(dead_time_us.get(int(channel), 0.0))
        else:
            dead = float(dead_time_us)
        if dead <= 0:
            continue
        idx = np.flatnonzero(tdc == channel)
        order = idx[np.argsort(t[idx], kind="stable")]
        keep[order[1:]] = np.diff(t[order]) >= dead
    return keep


class _Triggers:
    """Trigger structure of a run (events in time order): the triggers strictly between the
    first and last event of each visit of a scan step, with the step's set voltage."""

    def __init__(self, dv: np.ndarray, bunch: np.ndarray) -> None:
        if len(dv) == 0:
            dv, bunch = np.zeros(1), np.zeros(1, np.int64)
        visit = np.r_[0, np.cumsum(np.diff(dv) != 0)]
        first = np.r_[0, np.flatnonzero(np.diff(visit)) + 1]
        last = np.r_[first[1:] - 1, len(dv) - 1]
        start, stop = bunch[first] + 1, bunch[last] - 1
        span = np.maximum(stop - start + 1, 0)
        offset = np.r_[0, np.cumsum(span)[:-1]]
        self.n = int(span.sum())
        inside = (bunch >= start[visit]) & (bunch <= stop[visit])
        self.event_index = np.where(inside, offset[visit] + bunch - start[visit], -1)
        _, step = np.unique(np.repeat(dv[first], span), return_inverse=True)
        self.step = step
        self.n_per_step = np.bincount(step) if self.n else np.zeros(0, np.int64)

    def counts(self, mask: np.ndarray) -> np.ndarray:
        index = self.event_index[mask]
        return np.bincount(index[index >= 0], minlength=self.n)

    def pooled(self, counts: np.ndarray) -> tuple[float, float, float]:
        """Pooled variance and mean sums over steps and the Poisson variance of the first."""
        n = self.n_per_step
        s1 = np.bincount(self.step, counts, minlength=len(n))
        s2 = np.bincount(self.step, counts.astype(np.float64) ** 2, minlength=len(n))
        mean = s1 / n
        var = s2 / n - mean**2
        use = (n > MIN_TRIGGERS_PER_STEP) & (mean > 0)
        num = float(np.sum(n[use] * var[use]))
        den = float(np.sum(n[use] * mean[use]))
        null_var = float(np.sum(n[use] * (mean[use] + 2 * mean[use] ** 2)))
        return num, den, null_var


def _fano(pooled: tuple[float, float, float]) -> tuple[float, float]:
    num, den, null_var = pooled
    if den <= 0:
        return np.nan, np.nan
    return num / den, float(np.sqrt(null_var)) / den


def _burst(
    t: np.ndarray, tof: np.ndarray, period_us: float, n_triggers: int
) -> tuple[float, float]:
    """Observed and uncorrelated fractions of events within BURST_WINDOW_US after the previous
    event of the same PMT; t, tof of one PMT, any order."""
    if len(t) < 2 or n_triggers == 0:
        return np.nan, np.nan
    w = BURST_WINDOW_US
    observed = float(np.sum(np.diff(np.sort(t)) < w)) / len(t)
    # Expected number of other events in [tau - w, tau) per trigger, from the ToF profile; for
    # tau < w the window reaches into the end of the previous trigger. Only the mean is needed,
    # so the events are taken in ToF order (sorted queries keep searchsorted fast).
    tof = np.sort(tof)

    def cumulative(x: np.ndarray) -> np.ndarray:
        return np.searchsorted(tof, x, side="left").astype(np.float64)

    expected = cumulative(tof) - cumulative(tof - w)
    early = tof < w
    expected[early] += len(tof) - cumulative(period_us - w + tof[early])
    return observed, float(np.mean(1 - np.exp(-expected / n_triggers)))


def check_counting(
    dv: np.ndarray, bunch: np.ndarray, tdc: np.ndarray, tof: np.ndarray, period_us: float
) -> CountingCheck:
    """Counting statistics of a run's events (any order) per PMT channel and summed."""
    dv, tdc, tof = np.asarray(dv, float), np.asarray(tdc), np.asarray(tof, float)
    bunch = np.asarray(bunch).astype(np.int64)
    if np.any(np.diff(bunch) < 0):
        order = np.argsort(bunch, kind="stable")
        dv, bunch, tdc, tof = dv[order], bunch[order], tdc[order], tof[order]
    t = event_time_us(bunch, tof, period_us)
    triggers = _Triggers(dv, bunch)
    n_triggers = int(bunch[-1] - bunch[0] + 1) if len(bunch) else 0
    live_s = n_triggers * period_us * 1e-6
    keep_dead = dead_time_keep(bunch, tdc, tof, period_us, BURST_WINDOW_US)

    pmts: dict[int, PMTCounting] = {}
    sum_num = 0.0
    for channel in np.unique(tdc):
        on = tdc == channel
        pooled = triggers.pooled(triggers.counts(on))
        sum_num += pooled[0]
        fano, fano_err = _fano(pooled)
        fano_dead, _ = _fano(triggers.pooled(triggers.counts(on & keep_dead)))
        burst, burst_exp = _burst(t[on], tof[on], period_us, n_triggers)
        pmts[int(channel)] = PMTCounting(
            channel=int(channel),
            events=int(on.sum()),
            rate_cps=float(on.sum()) / live_s if live_s else np.nan,
            fano=fano,
            fano_err=fano_err,
            burst_fraction=burst,
            burst_expected=burst_exp,
            fano_dead=fano_dead,
        )
    pooled_all = triggers.pooled(triggers.counts(np.ones(len(t), dtype=bool)))
    fano_all, fano_all_err = _fano(pooled_all)
    correlated = (pooled_all[0] - sum_num) / pooled_all[1] if pooled_all[1] > 0 else np.nan
    check = CountingCheck(
        period_us=period_us,
        n_triggers=n_triggers,
        n_fano_triggers=triggers.n,
        pmts=pmts,
        fano_all=fano_all,
        fano_all_err=fano_all_err,
        correlated_excess=correlated,
        issues=(),
    )
    return CountingCheck(**{**check.__dict__, "issues": _issues(check)})


def _off(fano: float, err: float) -> bool:
    return bool(abs(fano - 1) > FANO_TOL and abs(fano - 1) > FANO_SIGMA * err)


def _issues(check: CountingCheck) -> tuple[CountingIssue, ...]:
    issues = []
    w = f"{BURST_WINDOW_US:g} us"
    for p in check.pmts.values():
        if p.events < MIN_EVENTS or not _off(p.fano, p.fano_err):
            continue
        text = (
            f"PMT {p.channel} counts are not Poisson: Fano factor {p.fano:.2f} +- {p.fano_err:.2f}"
        )
        if p.fano < 1:
            issues.append(
                CountingIssue(
                    "sub_poisson",
                    p.channel,
                    text + " (below 1: counting losses, e.g. dead time or saturation)",
                )
            )
            continue
        if p.burst_excess > BURST_WARN:
            text += (
                f"; {100 * p.burst_fraction:.1f} % of its events come less than {w} after the "
                f"previous one ({100 * p.burst_expected:.1f} % expected): afterpulse bursts"
            )
        if abs(p.fano_dead - 1) < abs(p.fano - 1) / 2:
            text += f"; a {w} dead time gives {p.fano_dead:.2f} (apply_dead_time)"
        else:
            text += f"; a {w} dead time does not fix it ({p.fano_dead:.2f})"
        issues.append(CountingIssue("super_poisson", p.channel, text))
    if check.correlated_excess > FANO_TOL and _off(check.fano_all, check.fano_all_err):
        issues.append(
            CountingIssue(
                "correlated",
                None,
                f"the summed PMT counts fluctuate together: Fano factor {check.fano_all:.2f}, "
                f"{check.correlated_excess:.2f} above the single PMTs' (beam or laser intensity, "
                "or noise bursts on all channels: apply_filter)",
            )
        )
    return tuple(issues)
