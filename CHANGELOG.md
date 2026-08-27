# Changelog

All notable changes to `clstools` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
