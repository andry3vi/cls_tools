"""Tests for CLSDataFrame.Compute_Setpoint_Bins: one point per scan set voltage, counts of the
gated events at their mean frequency, under-visited set voltages (sweep end points) dropped.

The runs are built event by event: up-down sweeps over the set voltages, so the two end points
are visited once per up-down cycle and the others twice.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from clstools import CLSDataFrame

from .conftest import dask_run

SETPOINTS = np.arange(10.0)


def up_down_run(cycles=20, per_visit=6, seed=0):
    """Events of up-down sweeps 0..9..0: consecutive visits of the turning points merge, so 0
    and 9 get half the visits. Every visit has `per_visit` events alternating between PMT 3 and
    4; F = 1e6 * DV + noise; ToF 0-100 us."""
    rng = np.random.default_rng(seed)
    one = np.r_[SETPOINTS, SETPOINTS[::-1]]
    sweep = np.tile(one, cycles)
    keep = np.r_[True, np.diff(sweep) != 0]  # merge 9,9 and 0,0
    visits = sweep[keep]
    dv = np.repeat(visits, per_visit)
    n = len(dv)
    bunch = np.repeat(np.arange(len(visits)) * 30, per_visit) + np.tile(
        np.arange(per_visit), len(visits)
    )
    return pd.DataFrame(
        {
            "TS": 1_793_000_000.0 + bunch * 1e-3,
            "DV": dv,
            "Bunch": bunch.astype(float),
            "TDC": np.where(np.arange(n) % 2, 4.0, 3.0),
            "TOF": rng.uniform(0.0, 100.0, n),
            "Vrfq": np.full(n, 3.0),
            "F": 1e6 * dv + rng.normal(0.0, 1e3, n),
            "V": 30000.0 - dv,
        }
    )


def frame_for(events):
    frame = CLSDataFrame()
    frame.Run = dask_run(events, npartitions=3)
    return frame


def test_one_point_per_set_voltage_with_its_counts_and_mean_frequency():
    events = up_down_run()
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins()

    inner = events[events["DV"].between(1, 8)]
    want = inner.groupby("DV").agg(n=("F", "size"), f=("F", "mean"), v=("V", "mean"))
    assert got["DV"].tolist() == want.index.tolist()  # sorted by frequency = by DV here
    np.testing.assert_array_equal(got["Fcount"], want["n"])
    np.testing.assert_allclose(got["Fmean"], want["f"])
    np.testing.assert_allclose(got["Vmean"], want["v"])
    np.testing.assert_allclose(got["Fcount_err"], np.sqrt(want["n"]))
    assert (got["visits"] == 40).all()
    assert frame.Setpoint_binned is got


def test_sweep_end_points_are_dropped_and_listed():
    frame = frame_for(up_down_run())
    frame.Compute_Setpoint_Bins()
    assert frame.Setpoint_dropped == [0.0, 9.0]
    frame.Compute_Setpoint_Bins(min_visits=0.4)
    assert frame.Setpoint_dropped == [] and len(frame.Setpoint_binned) == 10


def test_gates_select_the_counted_events():
    events = up_down_run()
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins(TOF_gate=[20.0, 60.0], PMT_gate=[3])
    sel = events[
        (events["TOF"] > 20)
        & (events["TOF"] < 60)
        & (events["TDC"] == 3)
        & events["DV"].between(1, 8)
    ]
    want = sel.groupby("DV").agg(n=("F", "size"), f=("F", "mean"))
    np.testing.assert_array_equal(got["Fcount"], want["n"])
    np.testing.assert_allclose(got["Fmean"], want["f"])


def test_set_voltage_without_gated_events_keeps_its_frequency():
    events = up_down_run()
    events.loc[events["DV"] == 4, "TDC"] = 1.0  # nothing on PMT 3/4 at 4 V
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins(PMT_gate=[3, 4]).set_index("DV")
    assert got.loc[4.0, "Fcount"] == 0 and got.loc[4.0, "Fcount_err"] == 1.0
    assert got.loc[4.0, "Fmean"] == pytest.approx(events.loc[events["DV"] == 4, "F"].mean())


def test_voltage_and_frequency_gates_select_set_voltages():
    frame = frame_for(up_down_run())
    got = frame.Compute_Setpoint_Bins(V_gate=[2.5, 7.5])
    assert got["DV"].tolist() == [3.0, 4.0, 5.0, 6.0, 7.0]
    got = frame.Compute_Setpoint_Bins(F_gate=[1.5e6, 4.5e6])
    assert got["DV"].tolist() == [2.0, 3.0, 4.0]


def test_points_follow_unevenly_spaced_frequencies():
    # with a scan table the frequency spacing of the set voltages is uneven: the points sit at
    # the events' mean frequency, not on a regular grid
    events = up_down_run()
    events["F"] = 1e6 * events["DV"] + 0.3e6 * np.sin(events["DV"])
    got = frame_for(events).Compute_Setpoint_Bins()
    np.testing.assert_allclose(got["Fmean"], 1e6 * got["DV"] + 0.3e6 * np.sin(got["DV"]))
