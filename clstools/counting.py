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
times worse than the other. The CLS systematics study (2026-11-02) took a 10 us extending dead
time per PMT: Bi 8244/8245 Fano 1.9-2.0 -> 1.03, chi2_red 2.0-2.5 -> 1.12-1.14, 5.0-5.2 % of the
events removed (5 and 20 us give the same hyperfine constants within 0.12 / 0.2 MHz).

With a bunched beam (Yb+ I340) the counts in the ToF gate were Poisson to 1-2 % and a dead time
removed real photons of the same bunch: 7-17 % at the line peaks, distorting widths and
amplitudes. The pile-up indicator below measures that loss from the data, so a dead time is only
applied when asked for (CLSDataFrame.apply_dead_time) and warned about when it would cost real
photons.

Indicators, per PMT:
    fano            Fano factor of the counts per trigger, with its error under Poisson counting
    burst_fraction  fraction of events less than BURST_WINDOW_US after the previous event of the
                    same PMT; burst_expected is the same for uncorrelated counting with the run's
                    own ToF profile
    fano_dead       Fano factor after a DEAD_TIME_US dead time, i.e. what apply_dead_time would
                    give
    pileup_dead     fraction of uncorrelated (real) events a DEAD_TIME_US dead time would remove:
                    events with an event of the same PMT within the dead time before them in the
                    previous trigger of the same scan-step visit (same rate and ToF profile, no
                    afterpulse correlation). ~0.1 % for a DC beam, several % for a bunched one.
and for the summed counts, the excess of their Fano factor over the PMTs' own (correlated
fluctuations: beam or laser intensity, noise bursts on all channels).

Split tests (a Fano test of the spectrum, CLS systematics scripts/yb_afterpulse.py): the counts
per set voltage of two halves of the data are compared, chi2 = sum_s (A_s - r_s B_s)^2 /
(A_s + r_s^2 B_s) with r_s the exposure ratio, and chi2 / dof is 1 for Poisson counting:
    odd/even trigger      per-trigger statistics only (afterpulses, noise bursts)
    alternating blocks    adds every fluctuation that decorrelates between sweeps (beam or laser
                          intensity on the BLOCK_S scale): what adds variance to the spectrum
    first/second half     adds slow drifts of the line position or shape during the run
The exposure of the time splits is the number of visits of each set voltage in each half, seen
from the events, so set voltages with fewer than SPLIT_MIN_EVENTS_PER_VISIT events per visit
(where empty visits go unseen) are left out of them.

Only the Fano factors raise a CountingWarning (|F - 1| > FANO_TOL and > FANO_SIGMA sigma, PMTs
with at least MIN_EVENTS events); the burst indicators say whether a dead time would cure it.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np

BURST_WINDOW_US = 5.0  # afterpulse window of the burst indicator
DEAD_TIME_US = 10.0  # default of apply_dead_time and of the fano_dead / pileup_dead indicators
PILEUP_WARN = 0.01  # real-photon loss of a dead time above which apply_dead_time warns
FANO_TOL = 0.1  # |Fano - 1| allowed, if also more than FANO_SIGMA sigma
FANO_SIGMA = 5.0
# burst_fraction - burst_expected above which the warning names afterpulses as the cause. Not a
# warning on its own: with bunched beams the light comes in bursts too, more than the run-averaged
# ToF profile predicts (7-28 % in online runs whose Fano factor is 1).
BURST_WARN = 0.01
MIN_EVENTS = 1000  # fewer events on a PMT: indicators reported, no warning
MIN_TRIGGERS_PER_STEP = 20  # scan steps with fewer triggers are left out of the Fano factor
DEFAULT_PERIOD_US = 1000.0
BLOCK_S = 10.0  # block length of the alternating-blocks split test
SPLIT_MIN_EVENTS_PER_VISIT = 3.0  # set voltages below this are left out of the time splits
SPLIT_MIN_SETPOINTS = 10  # fewer usable set voltages: no split test result


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
    pileup_dead: float = np.nan

    @property
    def burst_excess(self) -> float:
        return self.burst_fraction - self.burst_expected


@dataclass(frozen=True)
class SplitTest:
    """Dispersion of the counts per set voltage between two halves of the data."""

    name: str
    chi2_red: float  # 1 for Poisson counting
    chi2_red_err: float  # sqrt(2 / n)
    n_setpoints: int


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
    splits: tuple[SplitTest, ...] = ()  # summed counts of all PMTs; empty without timestamps
    dead_time_us: float = DEAD_TIME_US  # of the fano_dead / pileup_dead indicators

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


def _time_order(*arrays: np.ndarray) -> list[np.ndarray]:
    """The arrays in trigger order (the first one is Bunch), stable."""
    bunch = np.asarray(arrays[0]).astype(np.int64)
    rest = [np.asarray(a) for a in arrays[1:]]
    if np.any(np.diff(bunch) < 0):
        order = np.argsort(bunch, kind="stable")
        return [bunch[order], *(a[order] for a in rest)]
    return [bunch, *rest]


def _visit_starts(dv: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Visit number of every event and the index of each visit's first event (time order)."""
    visit = np.r_[0, np.cumsum(np.diff(dv) != 0)] if len(dv) else np.zeros(0, np.int64)
    first = np.r_[0, np.flatnonzero(np.diff(visit)) + 1] if len(dv) else np.zeros(0, np.int64)
    return visit, first


def pileup_fraction(
    dv: np.ndarray,
    bunch: np.ndarray,
    tdc: np.ndarray,
    tof: np.ndarray,
    dead_time_us: float | Mapping[int, float] = DEAD_TIME_US,
) -> dict[int, float]:
    """Fraction of uncorrelated (real) events of each PMT that a dead time would remove.

    Surrogate (CLS systematics scripts/yb_afterpulse.py): the previous trigger of the same
    scan-step visit has the same rate and ToF profile but no afterpulse correlation with the
    current one, so the fraction of events with an event of the same PMT in the previous trigger
    less than the dead time earlier in ToF is the loss a dead time causes on real photons
    (pile-up). Events in the first trigger of a visit are not counted; the wrap-around into the
    end of the trigger before for ToF < dead time is left out (dead time / period, small).
    """
    bunch, dv, tdc, tof = _time_order(bunch, dv, tdc, tof)
    tof = tof.astype(np.float64)
    out: dict[int, float] = {}
    if len(bunch) < 2:
        return {int(c): np.nan for c in np.unique(tdc)}
    visit, first = _visit_starts(dv)
    eligible = bunch - 1 >= bunch[first][visit]
    channels, ch = np.unique(tdc, return_inverse=True)
    mult = len(channels)
    key = bunch * mult + ch
    for i, channel in enumerate(channels):
        dead = (
            float(dead_time_us.get(int(channel), 0.0))
            if isinstance(dead_time_us, Mapping)
            else float(dead_time_us)
        )
        on = ch == i
        use = on & eligible
        if dead <= 0 or not use.any():
            out[int(channel)] = 0.0 if dead <= 0 else np.nan
            continue
        stride = float(np.ceil(tof[on].max())) + dead + 1.0
        comp = np.sort(key[on] * stride + tof[on])
        base = (key[use] - mult) * stride
        hi = base + tof[use]
        lo = base + np.maximum(tof[use] - dead, 0.0)
        n = np.searchsorted(comp, hi, side="right") - np.searchsorted(comp, lo, side="right")
        # an event exactly at lo (ToF difference = dead time) is kept by the dead time
        out[int(channel)] = float(np.mean(n > 0))
    return out


def split_tests(
    dv: np.ndarray, bunch: np.ndarray, ts: np.ndarray, block_s: float = BLOCK_S
) -> tuple[SplitTest, ...]:
    """Split tests of the counts per set voltage (all events given), see the module docstring."""
    bunch, dv, ts = _time_order(bunch, dv, ts)
    ts = ts.astype(np.float64)
    names = ("odd/even trigger", f"alternating {block_s:g} s blocks", "first/second half")
    if len(bunch) < 2:
        return tuple(SplitTest(n, np.nan, np.nan, 0) for n in names)
    _, first = _visit_starts(dv)
    setpoints, inv = np.unique(dv, return_inverse=True)
    k = len(setpoints)
    v_inv = inv[first]
    vt = ts[first]
    visits = np.bincount(v_inv, minlength=k)
    counts = np.bincount(inv, minlength=k)
    dense = counts >= SPLIT_MIN_EVENTS_PER_VISIT * np.maximum(visits, 1)
    t0, tmed = float(ts.min()), float(np.median(vt))

    out = []
    for name, ev_a, visit_a in (
        (names[0], bunch % 2 == 0, None),
        (names[1], ((ts - t0) // block_s) % 2 == 0, ((vt - t0) // block_s) % 2 == 0),
        (names[2], ts < tmed, vt < tmed),
    ):
        a = np.bincount(inv[ev_a], minlength=k).astype(float)
        b = np.bincount(inv[~ev_a], minlength=k).astype(float)
        if visit_a is None:
            r = np.full(k, a.sum() / b.sum() if b.sum() else np.nan)
            ok, nfix = a + b > 0, 1
        else:
            va = np.bincount(v_inv[visit_a], minlength=k).astype(float)
            vb = np.bincount(v_inv[~visit_a], minlength=k).astype(float)
            with np.errstate(divide="ignore", invalid="ignore"):
                r = va / vb
            ok, nfix = (a + b > 0) & (va > 0) & (vb > 0) & dense, 0
        ok &= np.isfinite(r)
        n = int(ok.sum())
        if n - nfix < SPLIT_MIN_SETPOINTS:
            out.append(SplitTest(name, np.nan, np.nan, n))
            continue
        chi = (a[ok] - r[ok] * b[ok]) ** 2 / (a[ok] + r[ok] ** 2 * b[ok])
        out.append(SplitTest(name, float(chi.sum() / (n - nfix)), float(np.sqrt(2.0 / n)), n))
    return tuple(out)


def check_counting(
    dv: np.ndarray,
    bunch: np.ndarray,
    tdc: np.ndarray,
    tof: np.ndarray,
    period_us: float,
    ts: np.ndarray | None = None,
    dead_time_us: float = DEAD_TIME_US,
) -> CountingCheck:
    """Counting statistics of a run's events (any order) per PMT channel and summed.

    With the timestamps ts the split tests of the summed counts are made too. dead_time_us is
    the dead time of the fano_dead and pileup_dead indicators.
    """
    dv, tdc, tof = np.asarray(dv, float), np.asarray(tdc), np.asarray(tof, float)
    bunch = np.asarray(bunch).astype(np.int64)
    ts_arr = np.asarray(ts, float) if ts is not None else np.zeros(len(bunch))
    bunch, dv, tdc, tof, ts_arr = _time_order(bunch, dv, tdc, tof, ts_arr)
    t = event_time_us(bunch, tof, period_us)
    triggers = _Triggers(dv, bunch)
    n_triggers = int(bunch[-1] - bunch[0] + 1) if len(bunch) else 0
    live_s = n_triggers * period_us * 1e-6
    keep_dead = dead_time_keep(bunch, tdc, tof, period_us, dead_time_us)
    pileup = pileup_fraction(dv, bunch, tdc, tof, dead_time_us)

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
            pileup_dead=pileup.get(int(channel), np.nan),
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
        splits=split_tests(dv, bunch, ts_arr) if ts is not None else (),
        dead_time_us=float(dead_time_us),
    )
    return CountingCheck(**{**check.__dict__, "issues": _issues(check)})


def _off(fano: float, err: float) -> bool:
    return bool(abs(fano - 1) > FANO_TOL and abs(fano - 1) > FANO_SIGMA * err)


def _issues(check: CountingCheck) -> tuple[CountingIssue, ...]:
    issues = []
    w = f"{BURST_WINDOW_US:g} us"
    d = f"{check.dead_time_us:g} us"
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
            text += f"; a {d} dead time gives {p.fano_dead:.2f} (apply_dead_time)"
        else:
            text += f"; a {d} dead time does not fix it ({p.fano_dead:.2f})"
        if p.pileup_dead > PILEUP_WARN:
            text += (
                f", but it would also remove {100 * p.pileup_dead:.1f} % of uncorrelated events "
                "(bunched beam or high rate)"
            )
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
