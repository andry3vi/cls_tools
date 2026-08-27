"""Shared fixtures for the clstools test suite.

No real ASDF run data is available in this repo (see CLAUDE.md, "Tracked vs.
scratch") -- run data lives on network shares outside version control. Tests
that need an ASDF file build a minimal synthetic tree matching the schema
`CLSDataFrame.Load_Run` reads; tests that only need the Dask/pandas pipeline
construct `self.Run` directly and skip ASDF entirely.
"""

from __future__ import annotations

import asdf
import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from clstools import CLSDataFrame

RAW_COLUMNS = ["TS", "DV", "Bunch", "TDC", "TOF", "Vrfq"]


def build_asdf_tree(
    *,
    run: int = 1,
    cooler_voltage: float = 0.5,
    laser_setpoint: float = 15000.0,
    dwell_time: float = 0.1,
    experiment: str = "TEST",
    date: str = "2026-01-01",
    step_size: float = 1.0,
    scanning_ranges: list[list[float]] | None = None,
    cal_set: np.ndarray | None = None,
    cal_readback: np.ndarray | None = None,
    raw: np.ndarray | None = None,
) -> dict:
    """Build a tree with the exact keys `Load_Run` reads from an ASDF file."""
    if scanning_ranges is None:
        scanning_ranges = [[-5.0, 5.0]]
    if cal_set is None:
        cal_set = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    if cal_readback is None:
        # Exact linear relationship: slope=2.0, intercept=0.5, no noise.
        cal_readback = 2.0 * cal_set + 0.5
    if raw is None:
        n = 20
        rng = np.random.default_rng(0)
        raw = np.column_stack(
            [
                1_700_000_000.0 + np.arange(n),  # TS
                rng.uniform(-5.0, 5.0, n),  # DV
                np.arange(n) % 4,  # Bunch
                1 + (np.arange(n) % 4),  # TDC, cycles 1..4
                rng.uniform(50.0, 70.0, n),  # TOF
                np.full(n, cooler_voltage),  # Vrfq
            ]
        )
    return {
        "Run": run,
        "CoolerVoltage": cooler_voltage,
        "LaserSetpoint": laser_setpoint,
        "DwellTime": dwell_time,
        "Experiment": experiment,
        "Date": date,
        "StepSize": step_size,
        "ScanningRanges": scanning_ranges,
        "CalSet": cal_set,
        "CalReadback": cal_readback,
        "raw": raw,
    }


def write_asdf(tmp_path, tree: dict, name: str = "run_1.asdf"):
    path = tmp_path / name
    af = asdf.AsdfFile(tree)
    af.write_to(str(path))
    return path


@pytest.fixture
def synthetic_run_path(tmp_path):
    """Path to a synthetic ASDF run file with a clean linear calibration."""
    return write_asdf(tmp_path, build_asdf_tree())


@pytest.fixture
def make_run_path(tmp_path):
    """Factory fixture: build+write a synthetic ASDF file with overrides."""

    def _make(**overrides):
        return write_asdf(
            tmp_path, build_asdf_tree(**overrides), name=f"run_{overrides.get('run', 1)}.asdf"
        )

    return _make


@pytest.fixture
def empty_frame() -> CLSDataFrame:
    """A CLSDataFrame with default voltage-division settings and no run loaded."""
    return CLSDataFrame()


def dask_run(df: pd.DataFrame, npartitions: int = 1) -> dd.DataFrame:
    return dd.from_pandas(df, npartitions=npartitions)
