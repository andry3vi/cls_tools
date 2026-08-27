# Contributing

`clstools` is a small library with one public class, `CLSDataFrame` (`clstools/DataFrame.py`).
This document covers the mechanics of contributing; see `README.md` for what the library does.

## Local setup

```powershell
uv sync --group dev
```

`uv sync --frozen` uninstalls anything in your environment that isn't in `uv.lock` — if you have
extra packages installed for local scratch work (fitting libraries used by `debug_tmp/` scripts,
say), a bare sync will remove them. They're easy to reinstall with `uv pip install <package>`;
they just don't belong in the committed lockfile.

## Branching

- `main` is the current release baseline and is always green. Don't commit to it directly.
- New work goes on a short-lived topic branch off `main`, named for what it touches, e.g.
  `calib/outlier-threshold`, `physics/doppler-nan-fix`.
- `dev` is a dormant leftover branch, kept in case something on it is still needed. It is not an
  active integration branch — target `main` with new work.

## Commits

Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`, with an optional
scope, e.g. `fix(calibration): guard against zero-std outlier threshold`.

If a commit changes a physical result — a Doppler formula, a calibration fit, a gate boundary —
say so in the commit body, with the old and new number and the reason. That's the single most
useful thing in the history of a repo whose job is producing trustworthy numbers.

Keep commits small and individually green; don't mix a refactor with a behavior change.

## Before opening a PR

```powershell
uvx ruff@0.16.3 check .
uvx ruff@0.16.3 format --check .
uv run --group dev pytest -q -ra
```

All three run in CI (`.github/workflows/ci.yml`) on every push and PR, and must pass before merge.

Describe in the PR: what changed, whether any physical result moved, and what you ran to check
it. A change to the Doppler math, voltage calibration, or gating logic should come with a new or
updated test — see `tests/` for the existing style (synthetic ASDF fixtures / directly-constructed
Dask frames, since no real run data lives in this repo; see `CLAUDE.md`, "Tracked vs. scratch").

## Releasing

Bump `version` in `pyproject.toml`, move the relevant `CHANGELOG.md` entries from `[Unreleased]`
into a new dated section, and update the wheel-name example in `README.md` to match.
