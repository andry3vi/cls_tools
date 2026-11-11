# Changelog

All notable changes to `clstools` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

One default changes results: `Compute_Voltages` now averages the cooler readings (`'smooth'`)
instead of using each event's own reading (`'pbp'`), see Changed. The rest is opt-in apart from
the warnings, and the single-point calibration fallback for runs that used to crash.

The other numbers below come from the Bi offline runs and the CLS systematics study
(2026-11-02). With `Use_Scan_Calibration`, the Bi offline Cl(²D₅/₂) result moves by +131.5 kHz
from the per-run line (the nonlinearity it misses; offset-only fit, `gain="bench"`). On six Bi
runs the table reproduces the Bi analysis: offsets to 10 µV, the same lags and rejections, and
per-event V within 5 µV from the table's rounding. On the seven systematics runs (Bi
8244/8245, Yb+ 7504–7548), the run offset and scale agree with the systematics rematch:
- Yb+: within 0.6 ppm and 1 mV;
- Bi: within their 19–41 ppm errors.

### Added

- `clstools.calibration`: scan-voltage tables (`ScanCalibration`, format documented in the
  module) with the bench table of 24 Oct 2026 packaged in `clstools/data/`, `available_tables`,
  `load_table`; `analyse_readback` (readback lag, unsettled readings, noise, gain and offset
  checks); `CalibrationWarning`.
- `CLSDataFrame.Use_Scan_Calibration`: table shape + per-run offset and scale from the readback,
  with a warning when the run is before the table's validity, and for set voltages off the
  table's grid or range.
- `CLSDataFrame.Set_Scan_Voltage_Map`: hook for any set → applied scan-voltage map (the one the Bi
  analysis carried in a local copy of clstools); used by `Compute_Voltages` and
  `Frequency_ranges`.
- `Compute_Voltages(cooler_correction="smooth")`: faulty cooler readings dropped, centred
  moving average, interpolated to each event (`Vrfq_smooth` column); `cooler_window_s`,
  `cooler_max_dev_v`. Now the default, see Changed.
- `Load_Run` always checks the calibration readback (`Readback_check`) and warns about faults in
  the file; `filter_calibration="robust"` corrects them before the polynomial fit. `Cal_raw`
  keeps the calibration points as recorded.
- `Info`/`Info_to_csv` report the scan table, run offset and scale, readback diagnostics, and
  the cooler window, faults and ripple.
- `clstools.counting`: PMT counting check (`check_counting`, `CountingCheck`, `CountingWarning`).
  Per PMT it gives:
  - the Fano factor of the counts per trigger;
  - the afterpulse-burst fraction;
  - the Fano factor a 10 µs dead time would give, and the fraction of uncorrelated (real)
    events that dead time would remove (`pileup_fraction`; Bi 0.15–0.27 %, Yb+ 1.0–4.5 %).

  `Load_Run` runs the check unless `counting_check=False`, and warns when a PMT is not Poisson.
  `Check_Counting` reruns it, and `Info`/`Info_to_csv` report it. On the Bi runs it flags PMT 4:
  Fano 2.2–3.1, against 1.1–1.2 for PMT 3.
- `CLSDataFrame.apply_dead_time`: artificial extending dead time per PMT, the Bi analysis's
  afterpulse cut.
  - It applies only when called. The default is 10 µs (CLS systematics: Bi 8244/8245 Fano
    1.9–2.0 → 1.03, 5.0–5.2 % of the events removed), or `{channel: us}`.
  - It warns when the pile-up loss of real photons is above 1 % (bunched beams: use none or
    1 µs).
  - `apply_filter` now also reruns the counting check.
- `CLSDataFrame.Compute_Setpoint_Bins`: one point per scan set voltage, counts at the mean
  frequency of the gated events.
  - Under-visited set voltages (sweep end points) are dropped and listed in `Setpoint_dropped`.
  - It is identical to the systematics' binning on 8244 and 7509.
  - On Bi 8244/8245 it lowers χ²_red from 3.4/4.2 to 2.5/2.0 and moves Bl by 2.6 MHz.
- Run scale in `Use_Scan_Calibration(gain=...)` and `ScanCalibration.fit_run`. The default
  `'auto'` follows the CLS systematics rule:
  - the scale is fitted with the offset when the calibration spans ≥ 400 V;
  - otherwise it is the campaign gain packaged with the table (12 campaigns, per `Experiment`);
  - otherwise it is 1.

  `'free'`, `'campaign'`, `'bench'` (scale 1, the previous behaviour) or a number select the
  scale explicitly. It is stored in `Scan_scale`, `Scan_scale_err` and `Scan_scale_mode`.
- Table format v1, optional fields `valid_from`, `valid_from_run` and `campaign_gains`. The
  packaged table is valid from run 5487 (4 Nov 2024), when the DAC chain changed. Runs before
  that get a warning that the table does not describe them; later runs taken before the
  measurement date no longer warn.
- A dead or corrupt calibration readback no longer gives a run offset: `Use_Scan_Calibration`
  uses the table's bench offset, with a warning.
- Cooler ripple diagnostics (`cooler_ripple`, `CoolerRipple`, `Cooler_ripple`): alias period,
  ripple rms (a line broadening, not corrected) and reading interval, for every
  `cooler_correction`, shown by `Info`.
- `docs/MANUAL.md`: reference for every public method and function.

### Changed

- `Compute_Voltages(cooler_correction=...)` defaults to `'smooth'` (was `'pbp'`), and
  `cooler_window_s` to `'auto'`: 4 alias periods of the run's readings, at least 15 s. `'pbp'`
  added the aliased mains ripple to every event: 0.31–0.37 V rms on the systematics runs
  (2.5–3.3 MHz), 1–2 V rms before May 2026. The averaging leaves 11–55 mV.
  - On Bi 8244/8245 the hyperfine constants move by ≤ 0.07 MHz (Al) and ≤ 0.36 MHz (Bl), and χ²
    is unchanged (CLS systematics, step S4 with 15 s). Faulty readings (> 5 V from the median,
    5 % in Yb+ 7504) are dropped instead of entering V.
  - `cooler_correction='pbp'` restores the old behaviour.
- The readback gain against a table (`ReadbackFit.gain_ppm`) is now the slope against the table
  value instead of the set voltage, i.e. exactly scale − 1. The difference is 0.1 % of the gain.

### Fixed

- A calibration with too few points no longer raises (`LinAlgError: Singular matrix`, 64 runs of
  the August 2026 archive sweep): `Cal_err` is NaN, or with fewer points than coefficients the
  nominal calibration (applied = set voltage) is used, with a warning.
- A `CalSet`/`CalReadback` length mismatch no longer raises; the common pairs are used, with a
  warning. Non-finite calibration values are ignored instead of producing NaN coefficients.
- `Dropped_calibration_points` is initialised, so `Info()` no longer fails after
  `filter_calibration=False`.
- `Load_Run` clears a scan map installed for a previously loaded run.

## [0.5.0] - 2026-08-27

### Added

- Test suite (`tests/`) covering the Doppler physics, the calibration fit and outlier filtering,
  and the gating/binning pipeline, using synthetic ASDF fixtures and directly-constructed Dask
  frames (no real run data lives in this repo).
- GitHub Actions CI (`.github/workflows/ci.yml`): ruff lint/format, mypy, and pytest across
  Python 3.11–3.13 on every push and pull request. Branch protection on `main` requires all of
  them before merge.
- `CONTRIBUTING.md` with the branching/commit/PR conventions.
- `uv.lock`, now tracked, so CI and local installs are reproducible (`uv sync --frozen`).
- mypy (non-strict, `clstools/` only; see `[tool.mypy]` in `pyproject.toml`), catching two real
  latent bugs (below) along the way.
- `LICENSE` — GPL-3.0, verbatim from gnu.org. `pyproject.toml` had declared this license since
  before this release, but no `LICENSE` file existed to back it up.

### Changed

- `requires-python` narrowed from `>=3.8` to `>=3.11` to match what is actually tested; classifiers
  updated to match.
- `apply_filter` now copies the computed frame (`tmp.compute().copy()`) before mutating it,
  avoiding a pandas `SettingWithCopyWarning` surfaced by the new tests. No behavior change.
- `Frequency_ranges`'s declared return type was `tuple`; it always returned a `list`. Fixed the
  annotation to match the actual (unchanged) behavior.
- `Compute_WL`'s `Frequency_stepsize` and `Frequency_ranges`'s returned bounds are now coerced to
  `float` explicitly (`dopplershift` is typed to admit an `ndarray`, though these call sites only
  ever pass scalars). No behavior change for scalar inputs, which is the only way these are
  currently called.
- `pyproject.toml`'s `Homepage`/`Repository` URLs fixed (were pointing at a defunct `gitlab.jyu.fi`
  mirror); added an `Issues` link.

### Known issues

- `dopplerfactor(voltage=0.0, mass=...)` can return `nan` for some masses due to a floating-point
  cancellation (`m**2*c**4` vs `(e*voltage+m*c**2)**2` are not always bit-identical at exactly
  V=0). Tracked as an `xfail` in `tests/test_physics.py`; not yet fixed.

## [0.4.2] and earlier

Predates this changelog. See `git log` for history.
