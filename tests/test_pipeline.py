"""Tests for the Dask/pandas pipeline stages downstream of Load_Run:
Compute_Voltages, Compute_WL, Shift_Ref, Frequency_ranges, the gating/binning
methods, and apply_filter.

These construct `self.Run` and the metadata attributes directly rather than
going through Load_Run/ASDF, so each stage is tested in isolation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from clstools import CLSDataFrame

from .conftest import dask_run


@pytest.fixture
def frame() -> CLSDataFrame:
    return CLSDataFrame(VAccDiv=1000, VCoolDiv=10000, VCoolOffset=0.0)


def make_run(**columns) -> pd.DataFrame:
    return pd.DataFrame(columns)


# --------------------------------------------------------------------- Compute_Voltages


def test_compute_voltages_pbp_matches_manual_calculation(frame):
    dv = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    vrfq = np.array([0.5, 0.5, 0.5, 0.5, 0.5])
    frame.Run = dask_run(make_run(DV=dv, Vrfq=vrfq))
    frame.Cal_order = 1
    frame.Cal = [0.1, 2.0]  # intercept, slope

    frame.Compute_Voltages(cooler_correction="pbp")

    dv_cal = (dv * frame.Cal[1] + frame.Cal[0]) * frame.VAccDiv
    expected_v = vrfq * frame.VCoolDiv + frame.VCoolOffset - dv_cal
    np.testing.assert_allclose(frame.Sorted["V"].to_numpy(), expected_v)


def test_compute_voltages_mean_uses_average_vrfq(frame):
    dv = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    vrfq = np.array([0.4, 0.5, 0.5, 0.5, 0.6])  # mean = 0.5
    frame.Run = dask_run(make_run(DV=dv, Vrfq=vrfq))
    frame.Cal_order = 1
    frame.Cal = [0.1, 2.0]

    frame.Compute_Voltages(cooler_correction="mean")

    dv_cal = (dv * frame.Cal[1] + frame.Cal[0]) * frame.VAccDiv
    expected_v = vrfq.mean() * frame.VCoolDiv + frame.VCoolOffset - dv_cal
    np.testing.assert_allclose(frame.Sorted["V"].to_numpy(), expected_v)


def test_compute_voltages_quadratic_calibration(frame):
    dv = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    vrfq = np.zeros_like(dv)
    frame.Run = dask_run(make_run(DV=dv, Vrfq=vrfq))
    frame.Cal_order = 2
    frame.Cal = [0.1, 2.0, 0.05]  # intercept, linear, quadratic

    frame.Compute_Voltages(cooler_correction="pbp")

    dv_cal = (frame.Cal[2] * dv**2 + dv * frame.Cal[1] + frame.Cal[0]) * frame.VAccDiv
    expected_v = vrfq * frame.VCoolDiv + frame.VCoolOffset - dv_cal
    np.testing.assert_allclose(frame.Sorted["V"].to_numpy(), expected_v)


def test_compute_voltages_invalid_correction_raises(frame):
    frame.Run = dask_run(make_run(DV=np.array([0.0]), Vrfq=np.array([0.0])))
    frame.Cal_order = 1
    frame.Cal = [0.0, 1.0]

    with pytest.raises(AttributeError):
        frame.Compute_Voltages(cooler_correction="bogus")


# --------------------------------------------------------------------- Compute_WL / Shift_Ref


def _frame_with_voltages(frame, mass=133.0, laser_set=15000.0, step_size=1.0, vcool_init=0.5):
    v = np.array([4990.0, 4995.0, 5000.0, 5005.0, 5010.0])
    frame.Run = dask_run(make_run(V=v))
    frame.Mass = mass
    frame.Laser_set = laser_set
    frame.Step_Size = step_size
    frame.Vcool_init = vcool_init
    return v


def test_compute_wl_matches_manual_dopplershift(frame):
    v = _frame_with_voltages(frame)
    ref = 1.0e9
    frame.Compute_WL(Mass=frame.Mass, ref=ref, harmonic=2)

    expected_wn = frame.dopplershift(
        2 * frame.Laser_set, v, frame.Mass, collinear=False, rest_to_lab=False
    )
    expected_f = frame.WN_to_f * expected_wn - ref
    np.testing.assert_allclose(frame.Sorted["WN"].to_numpy(), expected_wn)
    np.testing.assert_allclose(frame.Sorted["F"].to_numpy(), expected_f)


def test_compute_wl_frequency_stepsize_matches_manual_calculation(frame):
    _frame_with_voltages(frame)
    frame.Compute_WL(Mass=frame.Mass, ref=0.0, harmonic=2)

    vcool_v = frame.Vcool_init * frame.VCoolDiv
    wn_a = frame.dopplershift(
        2 * frame.Laser_set, vcool_v, frame.Mass, collinear=False, rest_to_lab=False
    )
    wn_b = frame.dopplershift(
        2 * frame.Laser_set,
        vcool_v + frame.Step_Size,
        frame.Mass,
        collinear=False,
        rest_to_lab=False,
    )
    expected_stepsize = abs(wn_a - wn_b) * frame.WN_to_f
    assert frame.Frequency_stepsize == pytest.approx(expected_stepsize)


def test_shift_ref_changes_f_but_not_wn(frame):
    _frame_with_voltages(frame)
    frame.Compute_WL(Mass=frame.Mass, ref=0.0, harmonic=2)
    wn_before = frame.Sorted["WN"].to_numpy().copy()

    frame.Shift_Ref(ref=5.0e8)

    np.testing.assert_allclose(frame.Sorted["WN"].to_numpy(), wn_before)
    expected_f = frame.WN_to_f * wn_before - 5.0e8
    np.testing.assert_allclose(frame.Sorted["F"].to_numpy(), expected_f)


# --------------------------------------------------------------------- Frequency_ranges


def test_frequency_ranges_matches_manual_dopplershift(frame):
    frame.Vcool_init = 0.5
    frame.VCoolDiv = 10000
    frame.VCoolOffset = 0.0
    frame.Laser_set = 15000.0
    frame.ScanningRanges = [[-100.0, 100.0]]

    [[f_min, f_max]] = frame.Frequency_ranges(Mass=133.0, ref=0.0, harmonic=2)

    base_v = frame.Vcool_init * frame.VCoolDiv + frame.VCoolOffset
    max_v = base_v - 100.0
    min_v = base_v - (-100.0)
    wn_min = frame.dopplershift(
        2 * frame.Laser_set, max_v, 133.0, collinear=False, rest_to_lab=False
    )
    wn_max = frame.dopplershift(
        2 * frame.Laser_set, min_v, 133.0, collinear=False, rest_to_lab=False
    )
    assert f_min == pytest.approx(frame.WN_to_f * wn_min / 1e6)
    assert f_max == pytest.approx(frame.WN_to_f * wn_max / 1e6)


# --------------------------------------------------------------------- gating/binning


def _gating_run() -> pd.DataFrame:
    return make_run(
        TS=np.arange(10),
        F=np.linspace(-100.0, 100.0, 10),
        TOF=np.linspace(50.0, 60.0, 10),
        DV=np.linspace(-5.0, 5.0, 10),
        TDC=np.array([1, 2, 3, 4, 1, 2, 3, 4, 1, 2]),
        V=np.linspace(4900.0, 5100.0, 10),
    )


def test_compute_bins_voltage_and_pmt_gate_selects_expected_rows(frame):
    df = _gating_run()
    frame.Run = dask_run(df)
    frame.Frequency_stepsize = 20.0

    frame.Compute_Bins(V_gate=[-3.0, 3.0], PMT_gate=[1, 2])

    expected = df[(df.DV > -3.0) & (df.DV < 3.0) & df.TDC.isin([1, 2])]
    assert frame.Binned["Fcount"].sum() == len(expected)


def test_compute_bins_frequency_gate_selects_expected_rows(frame):
    df = _gating_run()
    frame.Run = dask_run(df)
    frame.Frequency_stepsize = 20.0

    frame.Compute_Bins(F_gate=[-50.0, 50.0])

    expected = df[(df.F > -50.0) & (df.F < 50.0)]
    assert frame.Binned["Fcount"].sum() == len(expected)


def test_compute_tof_pmt_gate_matches_manual_groupby(frame):
    df = _gating_run()
    frame.Run = dask_run(df)

    frame.Compute_ToF(PMT_gate=[3, 4])

    expected = df[df.TDC.isin([3, 4])].groupby("TOF").size()
    assert frame.ToF_binned["counts"].sum() == len(df[df.TDC.isin([3, 4])])
    np.testing.assert_allclose(
        sorted(frame.ToF_binned["counts"].to_numpy()), sorted(expected.to_numpy())
    )


def test_compute_raw_bins_tof_gate_matches_manual_groupby(frame):
    df = _gating_run()
    frame.Run = dask_run(df)

    frame.Compute_Raw_Bins(TOF_gate=[52.0, 58.0])

    expected = df[(df.TOF > 52.0) & (df.TOF < 58.0)]
    assert frame.Raw_binned["counts"].sum() == len(expected)


# --------------------------------------------------------------------- apply_filter


def test_apply_filter_drops_high_multiplicity_timestamps(frame):
    # TS=0 appears 3 times, everything else once.
    ts = np.array([0, 0, 0, 1, 2, 3])
    df = make_run(TS=ts, DV=np.arange(6, dtype=float))
    frame.Run = dask_run(df)

    frame.apply_filter(filter_window=2)

    result = frame.Run.compute()
    assert 0 not in result["TS"].to_numpy()
    assert sorted(result["TS"].to_numpy()) == [1, 2, 3]


def test_apply_filter_noop_when_window_is_zero(frame):
    ts = np.array([0, 0, 0, 1, 2, 3])
    df = make_run(TS=ts, DV=np.arange(6, dtype=float))
    frame.Run = dask_run(df)

    frame.apply_filter(filter_window=0)

    result = frame.Run.compute()
    assert sorted(result["TS"].to_numpy()) == sorted(ts)
