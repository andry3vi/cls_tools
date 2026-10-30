# Changelog

All notable changes to `clstools` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

Default results are unchanged: everything below is opt-in except the warnings, and the
single-point calibration fallback for runs that used to crash. With `Use_Scan_Calibration`, the Bi
offline Cl(²D₅/₂) result moves by +131.5 kHz from the per-run line (the nonlinearity it misses);
on six Bi runs the new path reproduces the Bi analysis (offsets to 10 µV, same lags and rejections,
per-event V within 5 µV from the table's rounding).

### Added

- `clstools.calibration`: scan-voltage tables (`ScanCalibration`, format documented in the
  module) with the bench table of 24 Oct 2026 packaged in `clstools/data/`, `available_tables`,
  `load_table`; `analyse_readback` (readback lag, unsettled readings, noise, gain and offset
  checks); `CalibrationWarning`.
- `CLSDataFrame.Use_Scan_Calibration`: table shape + per-run offset from the readback, with a
  warning when the run predates the table, and for set voltages off the table's grid or range.
- `CLSDataFrame.Set_Scan_Voltage_Map`: hook for any set → applied scan-voltage map (the one the Bi
  analysis carried in a local copy of clstools); used by `Compute_Voltages` and
  `Frequency_ranges`.
- `Compute_Voltages(cooler_correction="smooth")`: faulty cooler readings dropped, 5 s centred
  moving average, interpolated to each event (`Vrfq_smooth` column); `cooler_window_s`,
  `cooler_max_dev_v`.
- `Load_Run` always checks the calibration readback (`Readback_check`) and warns about faults in
  the file; `filter_calibration="robust"` corrects them before the polynomial fit. `Cal_raw`
  keeps the calibration points as recorded.
- `Info`/`Info_to_csv` report the scan table, run offset, readback diagnostics and cooler faults.
- `clstools.counting`: PMT counting check (`check_counting`, `CountingCheck`, `CountingWarning`).
  Per PMT it gives the Fano factor of the counts per trigger, the afterpulse-burst fraction and
  the Fano factor a dead time would give. `Load_Run` runs it unless `counting_check=False`, and
  warns when a PMT is not Poisson. `Check_Counting` reruns it, and `Info`/`Info_to_csv` report it.
  On the Bi runs it flags PMT 4: Fano 2.2–3.1, against 1.1–1.2 for PMT 3.
- `CLSDataFrame.apply_dead_time`: artificial extending dead time per PMT (default 5 µs, or
  `{channel: us}`), the Bi analysis's afterpulse cut. `apply_filter` now also reruns the
  counting check.

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
