"""Scan-voltage calibration tables, checks of the in-run calibration readback, and the cooler
readback smoothing.

Scan calibration table
----------------------
The scan HV (USB-231 DAC in split mode -> Trek 609E-6, x1000) has three separable parts: a gain
stable to a few ppm, an offset of about -0.9 V that drifts by ~0.1 V over hours, and a
reproducible DAC nonlinearity of ~0.25 V rms. A per-run straight line through CalSet/CalReadback
cannot follow the nonlinearity, so the bench-measured shape is applied instead and only the
offset is fitted to each run's own readback:

    applied(DV) = divider_scale * (shape(DV) + offset_run)

Tables ship as JSON in ``clstools/data/``. Format ``clstools-scan-calibration``, version 1:

    format, format_version   "clstools-scan-calibration", 1
    id                       unique name, e.g. "scan_hv_2026-10-24"
    measured                 ISO date of the bench measurement; runs taken before it get a warning
    electrode                scanned electrode the table was measured on ("CEC", "LCR")
    description              free text
    set_v                    set voltages [V], strictly increasing (the DAQ's integer-volt grid)
    shape_v                  applied HV minus its offset at each set voltage [V]
    cubic                    optional, cubic fit of the shape, highest power first; used off the
                             grid. Fitted to the whole table at load time when absent.
    bench_offset_v           offset of the bench measurement [V]; fallback when a run has no
                             usable readback, and the reference for the offset sanity check
    divider_scale(_err)      true divider ratio / nominal (1.0 until a certificate is known)
    repeat_rms_v             shape reproducibility between bench runs [V]
    gain_spread_ppm          gain spread between bench runs [ppm]
    provenance               free-form: producer, source runs, ...

All voltages are at the HV side, read through the nominal 1000:1 divider.
"""

from __future__ import annotations

import dataclasses
import datetime
import functools
import json
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

TABLE_FORMAT = "clstools-scan-calibration"
TABLE_FORMAT_VERSION = 1
SCAN_MODELS = ("lut", "cubic")

# Readback checks. Starting values from the Bi offline runs (October 2026): in-run readback noise
# 63-93 mV rms, readback gain within -8...-99 ppm of the bench table, offsets within ~0.1 V of
# the bench, lags of 1 and 2 steps, unsettled readings 260 and 400 V off after range jumps.
# Older runs differ without being faulty: before mid-2025 the readback is noisy (0.3-1.3 V rms,
# point to point), and in January 2025 the offset sat near +1.5 V.
REJECT_MIN_V = 0.5  # a reading is unsettled if further than max(this, REJECT_SIGMA robust sigma)
REJECT_SIGMA = 5.0
MAX_LAG_STEPS = 3
LAG_RMS_RATIO = 0.7  # a lag is taken only if it lowers the residual rms below this fraction
RMS_WARN_TABLE_V = 0.3  # residual rms against the table
RMS_WARN_POLY_V = 0.75  # against a free polynomial, which leaves the ~0.25 V rms nonlinearity
GAIN_WARN_TABLE_PPM = 300.0  # readback gain against the table, if also > GAIN_WARN_SIGMA sigma
GAIN_WARN_SIGMA = 5.0
SCALE_WARN = 0.01  # |readback gain - 1| (vs nominal or table): wrong VAccDiv / disconnected
OFFSET_WARN_V = 5.0  # |run offset - bench offset|

# Issues that are faults of the run file itself: Load_Run warns about these. The others (noisy,
# gain, offset) describe how well a table matches the run and are warned about by
# Use_Scan_Calibration.
DATAFILE_ISSUES = frozenset({"non_finite", "too_few_points", "lag", "unsettled", "readback_scale"})

# Cooler readback. The DAQ reads the cooler every ~0.1 s with a short aperture, aliasing the
# 50 Hz mains ripple (0.31 V rms) into a ~2.4 s pseudo-oscillation that the event times cannot
# follow; a 5 s centred moving average removes it and keeps the real slow drift.
COOLER_WINDOW_S = 5.0
COOLER_MAX_DEV_V = 5.0  # readings further than this from the run median are readback faults


class CalibrationWarning(UserWarning):
    """A problem found in a run's calibration data or in how a calibration applies to it."""


def warn(message: str) -> None:
    # stacklevel 3: the user's call into CLSDataFrame, not this helper or the method
    warnings.warn(message, CalibrationWarning, stacklevel=3)


# ---------------------------------------------------------------------- readback analysis


@dataclass(frozen=True)
class ReadbackIssue:
    kind: str  # non_finite, too_few_points, lag, unsettled, noisy, gain, offset, ...
    message: str


@dataclass(frozen=True)
class ReadbackFit:
    """Fit of a run's CalSet/CalReadback against a model, with what looked wrong.

    For a table model, offset_v is the run offset and gain_ppm the slope of (readback - table):
    it should be ~0. For a polynomial model, offset_v is the intercept and gain_ppm the linear
    coefficient's deviation from the nominal gain of 1.
    """

    model: str
    offset_v: float
    offset_err_v: float
    gain_ppm: float
    gain_err_ppm: float
    residual_rms_v: float
    n_points: int
    lag_steps: int
    set_v: np.ndarray  # set voltages after the lag shift [V]
    readback_v: np.ndarray  # readback paired with set_v [V at the HV side]
    keep: np.ndarray  # bool, points used by the fit (finite and settled)
    issues: tuple[ReadbackIssue, ...] = ()

    @property
    def rejected_set_v(self) -> list[float]:
        finite = np.isfinite(self.readback_v)
        return [float(v) for v in self.set_v[finite & ~self.keep]]

    @property
    def kinds(self) -> set[str]:
        return {i.kind for i in self.issues}


def _robust_fit(
    s: np.ndarray, rb: np.ndarray, reference: Callable[[np.ndarray], np.ndarray] | None, order: int
) -> tuple[np.ndarray, np.ndarray]:
    """Fit on the settled points; returns (residuals of all points, keep mask).

    With a reference, the model is reference(s) + constant; otherwise a polynomial of `order`.
    A point is unsettled when its residual is further than max(REJECT_MIN_V, REJECT_SIGMA robust
    sigma) from the median residual of all points. The polynomial is refitted on the kept
    points until they no longer change; with a reference the constant does not move the
    residuals relative to their median, so one pass is final.
    """
    finite = np.isfinite(rb)
    keep = finite
    base = reference(s) if reference is not None else None
    for _ in range(10):
        if base is not None:
            d = rb - base
            d = d - d[keep].mean()
        elif keep.sum() > order:
            d = rb - np.polyval(np.polyfit(s[keep], rb[keep], order), s)
        else:
            d = rb - rb[keep].mean()
        med = np.median(d[finite])
        mad = np.median(np.abs(d[finite] - med))
        new = finite & (np.abs(d - med) <= max(REJECT_MIN_V, REJECT_SIGMA * 1.4826 * mad))
        if np.array_equal(new, keep):
            break
        keep = new
    return d, keep


def analyse_readback(
    cal_set: np.ndarray,
    readback_v: np.ndarray,
    reference: Callable[[np.ndarray], np.ndarray] | None = None,
    order: int = 1,
    model: str | None = None,
) -> ReadbackFit:
    """Check a run's calibration readback and fit it, correcting a readback lag and dropping
    unsettled readings.

    Parameters
    ----------
    cal_set : array
        CalSet in acquisition order [V].
    readback_v : array
        CalReadback at the HV side, i.e. x VAccDiv [V], same order.
    reference : callable, optional
        Offset-free expected readback for a set voltage (a scan table's shape). Without it a
        polynomial of `order` is fitted.
    model : str, optional
        Label stored in the result.

    The readback can lag the set value by whole steps (each reading belongs to the step
    `lag` earlier). Lags of 1..MAX_LAG_STEPS are tried and one is taken only when it lowers
    the residual rms below LAG_RMS_RATIO of the best so far.
    """
    s0 = np.asarray(cal_set, dtype=float)
    rb0 = np.asarray(readback_v, dtype=float)
    label = model or ("reference" if reference is not None else f"poly{order}")
    issues: list[ReadbackIssue] = []

    n_bad = int((~np.isfinite(rb0)).sum() + (~np.isfinite(s0)).sum())
    if n_bad:
        issues.append(ReadbackIssue("non_finite", f"{n_bad} non-finite calibration values ignored"))
    ok0 = np.isfinite(s0)
    s0, rb0 = s0[ok0], rb0[ok0]

    min_points = 2 if reference is not None else order + 3
    if np.isfinite(rb0).sum() < min_points:
        n = int(np.isfinite(rb0).sum())
        issues.append(
            ReadbackIssue(
                "too_few_points",
                f"{n} usable calibration point{'s' if n != 1 else ''}, "
                f"at least {min_points} needed to check the readback",
            )
        )
        keep = np.isfinite(rb0)
        offset = (
            float(np.mean(rb0[keep] - (reference(s0[keep]) if reference else 0.0))) if n else np.nan
        )
        return ReadbackFit(
            label, offset, np.nan, np.nan, np.nan, np.nan, n, 0, s0, rb0, keep, tuple(issues)
        )

    def fit(lag: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
        s, rb = (s0[:-lag], rb0[lag:]) if lag else (s0, rb0)
        d, keep = _robust_fit(s, rb, reference, order)
        return s, rb, d, keep, float(d[keep].std())

    best_lag, best = 0, fit(0)
    for lag in range(1, MAX_LAG_STEPS + 1):
        if len(s0) - lag < min_points:
            break
        alt = fit(lag)
        if alt[4] < LAG_RMS_RATIO * best[4]:
            best_lag, best = lag, alt
    s, rb, d, keep, rms = best
    n = int(keep.sum())

    if reference is not None:
        r = rb[keep] - reference(s[keep])
        offset = float(r.mean())
        offset_err = float(r.std(ddof=1) / np.sqrt(n))
        slope_target = 0.0
        x, y = s[keep], r
    else:
        p = np.polyfit(s[keep], rb[keep], order)
        offset = float(p[-1])
        offset_err = float(d[keep].std(ddof=1) / np.sqrt(n))
        slope_target = 1.0
        x, y = s[keep], rb[keep]
    if n > 3 and np.ptp(x) > 0:
        coef, cov = np.polyfit(x, y, 1 if reference is not None else order, cov=True)
        gain_ppm = float(1e6 * (coef[-2] - slope_target))
        gain_err_ppm = float(1e6 * np.sqrt(cov[-2, -2]))
    else:
        gain_ppm = gain_err_ppm = np.nan

    if best_lag:
        issues.append(
            ReadbackIssue(
                "lag",
                f"readback lags the set value by {best_lag} step{'s' if best_lag > 1 else ''} "
                f"(residual rms {fit(0)[4]:.3f} V without the shift, {rms:.3f} V with it)",
            )
        )
    rejected = np.isfinite(rb) & ~keep
    if rejected.any():
        jump = np.r_[True, np.abs(np.diff(s)) > 1.5 * np.median(np.abs(np.diff(s)))]
        n_rej = int(rejected.sum())
        listed = ", ".join(
            f"{v:g} V ({dv:+.2f})" for v, dv in zip(s[rejected][:10], d[rejected][:10], strict=True)
        )
        issues.append(
            ReadbackIssue(
                "unsettled",
                f"{n_rej} calibration reading{'s' if n_rej > 1 else ''} rejected as unsettled, "
                f"{int((rejected & jump).sum())} of them at the first step after a jump: "
                f"{listed}{', ...' if n_rej > 10 else ''}",
            )
        )
    rms_warn = RMS_WARN_TABLE_V if reference is not None else RMS_WARN_POLY_V
    if rms > rms_warn:
        issues.append(ReadbackIssue("noisy", f"readback residual rms {rms:.3f} V (> {rms_warn} V)"))
    if np.isfinite(gain_ppm):
        against = "the table" if reference is not None else "the set value"
        significant = abs(gain_ppm) > GAIN_WARN_SIGMA * gain_err_ppm
        if abs(gain_ppm) > 1e6 * SCALE_WARN and significant:
            issues.append(
                ReadbackIssue(
                    "readback_scale",
                    f"readback scale {1 + gain_ppm / 1e6:.5f} against {against} (nominal 1): "
                    "wrong VAccDiv, or the readback is not connected?",
                )
            )
        elif reference is not None and abs(gain_ppm) > GAIN_WARN_TABLE_PPM and significant:
            issues.append(
                ReadbackIssue(
                    "gain",
                    f"readback gain differs from the table by {gain_ppm:+.0f} "
                    f"+- {gain_err_ppm:.0f} ppm: wrong table, divider ratio or scanned electrode?",
                )
            )
    return ReadbackFit(
        label,
        offset,
        offset_err,
        gain_ppm,
        gain_err_ppm,
        rms,
        n,
        best_lag,
        s,
        rb,
        keep,
        tuple(issues),
    )


# ---------------------------------------------------------------------- scan calibration table


def _date(value: Any) -> datetime.date:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value)[:10])


@dataclass(frozen=True)
class ScanCalibration:
    """A bench-measured scan-voltage table (see the module docstring for the file format)."""

    id: str
    measured: datetime.date
    electrode: str
    set_v: np.ndarray
    shape_v: np.ndarray
    cubic: np.ndarray
    bench_offset_v: float
    divider_scale: float = 1.0
    divider_scale_err: float = 0.0
    repeat_rms_v: float = np.nan
    gain_spread_ppm: float = np.nan
    description: str = ""
    provenance: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ScanCalibration:
        if d.get("format") != TABLE_FORMAT:
            raise ValueError(f"not a scan calibration table (format {d.get('format')!r})")
        if d.get("format_version") != TABLE_FORMAT_VERSION:
            raise ValueError(
                f"scan calibration format version {d.get('format_version')!r} is not supported "
                f"(this clstools reads version {TABLE_FORMAT_VERSION})"
            )
        set_v = np.asarray(d["set_v"], dtype=float)
        shape_v = np.asarray(d["shape_v"], dtype=float)
        if set_v.shape != shape_v.shape or set_v.ndim != 1 or len(set_v) < 4:
            raise ValueError("set_v and shape_v must be 1-d arrays of the same length (>= 4)")
        if not np.all(np.diff(set_v) > 0):
            raise ValueError("set_v must be strictly increasing")
        cubic = (
            np.asarray(d["cubic"], dtype=float)
            if d.get("cubic") is not None
            else np.polyfit(set_v, shape_v, 3)
        )
        return cls(
            id=str(d["id"]),
            measured=_date(d["measured"]),
            electrode=str(d.get("electrode", "")),
            set_v=set_v,
            shape_v=shape_v,
            cubic=cubic,
            bench_offset_v=float(d["bench_offset_v"]),
            divider_scale=float(d.get("divider_scale", 1.0)),
            divider_scale_err=float(d.get("divider_scale_err", 0.0)),
            repeat_rms_v=float(d.get("repeat_rms_v", np.nan)),
            gain_spread_ppm=float(d.get("gain_spread_ppm", np.nan)),
            description=str(d.get("description", "")),
            provenance=dict(d.get("provenance", {})),
        )

    @classmethod
    def load(cls, path: str | Path) -> ScanCalibration:
        return cls.from_dict(json.loads(Path(path).read_text()))

    # ------------------------------------------------------------------ evaluation

    def covers(self, dv: np.ndarray) -> np.ndarray:
        dv = np.asarray(dv, dtype=float)
        return np.asarray((dv >= self.set_v[0]) & (dv <= self.set_v[-1]))

    def on_grid(self, dv: np.ndarray) -> np.ndarray:
        dv = np.asarray(dv, dtype=float)
        idx = np.clip(np.searchsorted(self.set_v, dv), 0, len(self.set_v) - 1)
        return np.asarray(self.set_v[idx] == dv)

    def shape(self, dv: np.ndarray, model: str = "lut") -> np.ndarray:
        """Offset-free applied voltage [V] for set voltages dv [V], before the divider scale.

        'lut' takes the table on its grid and the cubic off it; 'cubic' uses the cubic
        everywhere (no DAC nonlinearity, for comparison and systematics). Neither warns about
        set values outside the table: CLSDataFrame.Use_Scan_Calibration checks that once.
        """
        dv = np.asarray(dv, dtype=float)
        out = np.asarray(np.polyval(self.cubic, dv), dtype=float)
        if model == "cubic":
            return out
        if model != "lut":
            raise ValueError(f"unknown scan model {model!r}, choose from {SCAN_MODELS}")
        idx = np.clip(np.searchsorted(self.set_v, dv), 0, len(self.set_v) - 1)
        hit = self.set_v[idx] == dv
        out[hit] = self.shape_v[idx[hit]]
        return out

    def applied(self, dv: np.ndarray, offset_v: float, model: str = "lut") -> np.ndarray:
        """Applied scan voltage [V] for set voltages dv and a run offset."""
        return self.divider_scale * (self.shape(dv, model) + offset_v)

    def scan_map(self, offset_v: float, model: str = "lut") -> Callable[[np.ndarray], np.ndarray]:
        """Callable for CLSDataFrame.Set_Scan_Voltage_Map."""
        return lambda dv: self.applied(dv, offset_v, model)

    def fit_run_offset(
        self, cal_set: np.ndarray, readback_v: np.ndarray, model: str = "lut"
    ) -> ReadbackFit:
        """Run offset from the in-run readback (set [V], readback at the HV side [V]), with the
        lag / unsettled / noise / gain checks of analyse_readback, plus an offset sanity check
        against the bench."""
        fit = analyse_readback(
            cal_set,
            readback_v,
            reference=lambda s: self.shape(s, model),
            model=f"{self.id}:{model}",
        )
        if np.isfinite(fit.offset_v) and abs(fit.offset_v - self.bench_offset_v) > OFFSET_WARN_V:
            issue = ReadbackIssue(
                "offset",
                f"run offset {fit.offset_v:+.3f} V is {fit.offset_v - self.bench_offset_v:+.3f} V "
                f"from the bench offset of table {self.id} ({self.bench_offset_v:+.3f} V)",
            )
            fit = dataclasses.replace(fit, issues=(*fit.issues, issue))
        return fit


@functools.cache
def _packaged_tables() -> tuple[ScanCalibration, ...]:
    root = resources.files("clstools") / "data"
    tables = []
    for entry in root.iterdir():
        if entry.name.endswith(".json"):
            d = json.loads(entry.read_text())
            if d.get("format") == TABLE_FORMAT:
                tables.append(ScanCalibration.from_dict(d))
    return tuple(sorted(tables, key=lambda t: t.measured))


def available_tables() -> list[str]:
    """Ids of the scan calibration tables shipped with clstools, oldest first."""
    return [t.id for t in _packaged_tables()]


def load_table(name: str | Path) -> ScanCalibration:
    """A packaged table by id, or a table file by path."""
    for t in _packaged_tables():
        if t.id == str(name):
            return t
    path = Path(name)
    if path.suffix == ".json" and path.exists():
        return ScanCalibration.load(path)
    raise ValueError(
        f"no scan calibration table {str(name)!r}; packaged tables: {available_tables()}"
    )


def select_table(run_date: datetime.date) -> ScanCalibration:
    """The packaged table measured most recently on or before run_date, else the oldest one."""
    tables = _packaged_tables()
    if not tables:
        raise RuntimeError("no scan calibration tables are packaged with clstools")
    earlier = [t for t in tables if t.measured <= run_date]
    return earlier[-1] if earlier else tables[0]


# ---------------------------------------------------------------------- cooler readback


def cooler_readings(ts: np.ndarray, vrfq: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Distinct cooler readings and the time each first appears; ts, vrfq in time order.

    The DAQ latches the latest cooler reading onto every event, so a new reading shows up as a
    change of the value in the event stream.
    """
    first = np.r_[0, np.flatnonzero(np.diff(vrfq) != 0) + 1]
    return ts[first], vrfq[first]


def faulty_readings(v: np.ndarray, max_dev: float | None) -> np.ndarray:
    """Readings further than max_dev from their median (same units as v)."""
    if max_dev is None or len(v) == 0:
        return np.zeros(np.shape(v), dtype=bool)
    return np.asarray(np.abs(v - np.median(v)) > max_dev)


def smooth_readings(t: np.ndarray, v: np.ndarray, window_s: float) -> np.ndarray:
    """Centred moving average of the readings over window_s seconds (t in seconds, increasing)."""
    s = pd.Series(v, index=pd.to_datetime(t, unit="s"))
    return np.asarray(
        s.rolling(pd.Timedelta(seconds=window_s), center=True, min_periods=1).mean().to_numpy(),
        dtype=float,
    )
