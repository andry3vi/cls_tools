"""Tests for CLSDataFrame.Compute_Setpoint_Bins: one point per scan set voltage, counts of the
gated events at their mean frequency, set voltages scanned fewer times than the others (partial
cycles) dropped.

The runs are simulated as the DAQ takes them (conftest.simulate_scan): up-down sweeps over the
ranges with both turning points dwelt on twice, so every set voltage gets the same exposure per
cycle, including the end points.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from clstools import CLSDataFrame

from .conftest import dask_run, simulate_scan

RANGES = [[0.0, 9.0]]


def frame_for(events, ranges=RANGES, step=1.0):
    frame = CLSDataFrame()
    frame.Run = dask_run(events, npartitions=3)
    frame.ScanningRanges = ranges
    frame.Step_Size = step
    return frame


def dense_run(**kw):
    kw = {"cycles": 20, "bunches_per_channel": 10, "settle": (1, 4), "events_per_dwell": 6} | kw
    return simulate_scan(RANGES, **kw)


def test_one_point_per_set_voltage_with_its_counts_and_mean_frequency():
    events, truth = dense_run()
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins()

    want = events.groupby("DV").agg(n=("F", "size"), f=("F", "mean"), v=("V", "mean"))
    assert got["DV"].tolist() == want.index.tolist()  # sorted by frequency = by DV here
    np.testing.assert_array_equal(got["Fcount"], want["n"])
    np.testing.assert_allclose(got["Fmean"], want["f"])
    np.testing.assert_allclose(got["Vmean"], want["v"])
    np.testing.assert_allclose(got["Fcount_err"], np.sqrt(want["n"]))
    assert got["visits"].tolist() == [truth[v] for v in got["DV"]]
    assert frame.Setpoint_binned is got
    assert frame.Scan_dwells.pattern == "updown"


def test_sweep_end_points_are_kept():
    # the two dwells at each turn merge into one visit in the event stream; they are still two
    # dwells, so the end points are not under-exposed
    frame = frame_for(dense_run()[0])
    got = frame.Compute_Setpoint_Bins()
    assert frame.Setpoint_dropped == [] and len(got) == 10
    assert (got["visits"] == 40).all()


def test_sparse_run_keeps_every_set_voltage():
    # one trigger per step, half an event per dwell: most dwells have no event
    ranges = [[0.0, 49.0]]
    events, truth = simulate_scan(ranges, cycles=12, events_per_dwell=0.5, seed=7)
    frame = frame_for(events, ranges)
    got = frame.Compute_Setpoint_Bins()
    assert frame.Setpoint_dropped == []
    assert got["visits"].tolist() == [truth[v] for v in got["DV"]]


def test_partial_cycle_is_dropped_and_listed():
    # 1.4 cycles of 0..19..0: up, down, and up again to 15 V, so 16-19 V were scanned once and
    # the rest 1.5 times
    ranges = [[0.0, 19.0]]
    events, _ = simulate_scan(ranges, cycles=1.4, events_per_dwell=8, seed=8)
    frame = frame_for(events, ranges)
    frame.Compute_Setpoint_Bins()
    assert frame.Setpoint_dropped == [16.0, 17.0, 18.0, 19.0]
    got = frame.Compute_Setpoint_Bins(min_visits=0)
    assert frame.Setpoint_dropped == [] and len(got) == 20


def test_without_scan_metadata_the_events_give_the_sequence():
    events, truth = dense_run()
    frame = frame_for(events, ranges=None, step=None)
    got = frame.Compute_Setpoint_Bins()
    assert frame.Scan_dwells.sequence_from == "events"
    assert frame.Setpoint_dropped == []
    assert got["visits"].tolist() == [truth[v] for v in got["DV"]]


def test_gates_select_the_counted_events():
    events, _ = dense_run()
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins(TOF_gate=[20.0, 60.0], PMT_gate=[3])
    sel = events[(events["TOF"] > 20) & (events["TOF"] < 60) & (events["TDC"] == 3)]
    want = sel.groupby("DV").agg(n=("F", "size"), f=("F", "mean"))
    np.testing.assert_array_equal(got["Fcount"], want["n"])
    np.testing.assert_allclose(got["Fmean"], want["f"])


def test_set_voltage_without_gated_events_keeps_its_frequency():
    events, _ = dense_run()
    events.loc[events["DV"] == 4, "TDC"] = 1.0  # nothing on PMT 3/4 at 4 V
    frame = frame_for(events)
    got = frame.Compute_Setpoint_Bins(PMT_gate=[3, 4]).set_index("DV")
    assert got.loc[4.0, "Fcount"] == 0 and got.loc[4.0, "Fcount_err"] == 1.0
    assert got.loc[4.0, "Fmean"] == pytest.approx(events.loc[events["DV"] == 4, "F"].mean())


def test_voltage_and_frequency_gates_select_set_voltages():
    frame = frame_for(dense_run()[0])
    got = frame.Compute_Setpoint_Bins(V_gate=[2.5, 7.5])
    assert got["DV"].tolist() == [3.0, 4.0, 5.0, 6.0, 7.0]
    got = frame.Compute_Setpoint_Bins(F_gate=[1.5e6, 4.5e6])
    assert got["DV"].tolist() == [2.0, 3.0, 4.0]


def test_points_follow_unevenly_spaced_frequencies():
    # with a scan table the frequency spacing of the set voltages is uneven: the points sit at
    # the events' mean frequency, not on a regular grid
    events, _ = dense_run()
    events["F"] = 1e6 * events["DV"] + 0.3e6 * np.sin(events["DV"])
    got = frame_for(events).Compute_Setpoint_Bins()
    np.testing.assert_allclose(got["Fmean"], 1e6 * got["DV"] + 0.3e6 * np.sin(got["DV"]))


def test_events_in_any_order():
    events, truth = dense_run()
    shuffled = events.sample(frac=1.0, random_state=1).reset_index(drop=True)
    got = frame_for(pd.DataFrame(shuffled)).Compute_Setpoint_Bins()
    assert got["visits"].tolist() == [truth[v] for v in got["DV"]]
