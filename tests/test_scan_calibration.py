"""Tests for the packaged scan calibration table, the readback analysis against it, and
CLSDataFrame.Use_Scan_Calibration.

Synthetic readbacks are built from the table itself plus a known offset, noise, lag and
unsettled readings, so the expected fit results are known exactly. The table's own validity is
checked against an independent bench run (tests/data/), which was not used to build it.
"""

from __future__ import annotations

import datetime
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from clstools import (
    CalibrationWarning,
    CLSDataFrame,
    ScanCalibration,
    analyse_readback,
    available_tables,
    load_table,
)

from .conftest import build_asdf_tree, write_asdf

TABLE_ID = "scan_hv_2026-10-24"
DATA = Path(__file__).parent / "data"


@pytest.fixture(scope="module")
def table() -> ScanCalibration:
    return load_table(TABLE_ID)


def readback_for(table, cal_set, offset, noise=0.08, lag=0, seed=1):
    """Readback at the HV side [V] of a scan following the table: reading i belongs to step
    i - lag (the first `lag` readings repeat the first step)."""
    rng = np.random.default_rng(seed)
    applied = table.shape(cal_set) + offset
    if lag:
        applied = np.r_[np.full(lag, applied[0]), applied[:-lag]]
    return applied + rng.normal(0.0, noise, len(cal_set))


def scan(*ranges):
    """CalSet of one sweep over integer-volt ranges, in acquisition order."""
    return np.concatenate([np.arange(lo, hi + 1, 1.0) for lo, hi in ranges])


# --------------------------------------------------------------------- table


def test_packaged_table_loads(table):
    assert TABLE_ID in available_tables()
    assert table.measured == datetime.date(2026, 10, 24)
    assert table.electrode == "CEC"
    assert table.set_v[0] == -3000 and table.set_v[-1] == 3000
    assert len(table.set_v) == len(table.shape_v) == 6001
    assert table.bench_offset_v == pytest.approx(-0.937, abs=1e-3)


def test_table_matches_an_independent_bench_run(table):
    # cls-cal linearity run 20261024_184702 (not used to build the table), drift-corrected with
    # its interleaved zero checks: after offset + gain the table must hold to the bench
    # repeatability, while a straight line cannot follow the DAC nonlinearity.
    p = pd.read_csv(DATA / "bench_linearity_20261024_184702.csv")
    sweep, zero = p[p.kind == "sweep"], p[p.kind == "zero"]
    drift = np.interp(sweep.t_s, zero.t_s, zero.mean_kv)
    hv = pd.Series(
        1e3 * (sweep.mean_kv.to_numpy() - drift), index=np.round(1e3 * sweep.set_kv).astype(int)
    )
    hv = hv.groupby(level=0).mean()
    s = hv.index.to_numpy(float)

    diff = hv.to_numpy() - table.shape(s)
    gain, offset = np.polyfit(s, diff, 1)
    resid = diff - (gain * s + offset)
    assert np.std(resid) < 0.010  # 6 mV measured
    assert abs(gain) < 50e-6  # -19 ppm measured

    line = hv.to_numpy() - np.polyval(np.polyfit(s, hv.to_numpy(), 1), s)
    assert np.std(line) > 0.15  # the nonlinearity the table corrects (~0.24 V rms)


def test_shape_lut_on_grid_and_cubic_off_grid(table):
    on = np.array([-2000.0, -1.0, 0.0, 517.0, 2999.0])
    idx = np.searchsorted(table.set_v, on)
    np.testing.assert_array_equal(table.shape(on), table.shape_v[idx])

    off = np.array([-1500.5, 0.25, 517.3])
    np.testing.assert_allclose(table.shape(off), np.polyval(table.cubic, off))
    np.testing.assert_allclose(table.shape(on, "cubic"), np.polyval(table.cubic, on))
    with pytest.raises(ValueError, match="unknown scan model"):
        table.shape(on, "linear")


def test_applied_includes_offset_and_divider_scale(table):
    dv = np.array([-100.0, 0.0, 250.0])
    scaled = ScanCalibration(**{**table.__dict__, "divider_scale": 1.0001})
    np.testing.assert_allclose(scaled.applied(dv, -0.8), 1.0001 * (table.shape(dv) - 0.8))
    np.testing.assert_allclose(scaled.scan_map(-0.8)(dv), scaled.applied(dv, -0.8))


@pytest.mark.parametrize(
    "change, message",
    [
        ({"format": "something-else"}, "not a scan calibration table"),
        ({"format_version": 2}, "format version 2 is not supported"),
        ({"set_v": [0, 2, 1, 3]}, "strictly increasing"),
        ({"shape_v": [0.0, 1.0]}, "same length"),
    ],
)
def test_table_format_is_validated(change, message):
    d = {
        "format": "clstools-scan-calibration",
        "format_version": 1,
        "id": "t",
        "measured": "2026-01-01",
        "set_v": [0, 1, 2, 3],
        "shape_v": [0.0, 1.0, 2.0, 3.0],
        "bench_offset_v": 0.0,
    }
    with pytest.raises(ValueError, match=message):
        ScanCalibration.from_dict({**d, **change})


def test_table_without_cubic_gets_one_fitted():
    d = {
        "format": "clstools-scan-calibration",
        "format_version": 1,
        "id": "t",
        "measured": "2026-01-01",
        "set_v": [0, 1, 2, 3, 4],
        "shape_v": [0.0, 1.0, 8.0, 27.0, 64.0],
        "bench_offset_v": 0.0,
    }
    cal = ScanCalibration.from_dict(d)
    np.testing.assert_allclose(cal.shape(np.array([2.5]), "cubic"), [15.625], atol=1e-9)


def test_table_file_round_trip(tmp_path, table):
    d = {
        "format": "clstools-scan-calibration",
        "format_version": 1,
        "id": "custom",
        "measured": "2027-01-01",
        "set_v": table.set_v.tolist(),
        "shape_v": table.shape_v.tolist(),
        "bench_offset_v": -0.5,
    }
    path = tmp_path / "custom.json"
    path.write_text(json.dumps(d))
    cal = load_table(path)
    assert cal.id == "custom" and cal.bench_offset_v == -0.5
    with pytest.raises(ValueError, match="no scan calibration table"):
        load_table("does-not-exist")


# --------------------------------------------------------------------- readback analysis


def test_offset_recovered_from_a_clean_readback(table):
    s = scan((100, 160), (400, 460), (800, 860))
    fit = table.fit_run_offset(s, readback_for(table, s, offset=-0.82))
    assert fit.issues == ()
    assert fit.lag_steps == 0
    assert fit.offset_v == pytest.approx(-0.82, abs=4 * fit.offset_err_v)
    assert fit.offset_err_v == pytest.approx(0.08 / np.sqrt(len(s)), rel=0.2)
    assert fit.residual_rms_v == pytest.approx(0.08, rel=0.2)


@pytest.mark.parametrize("lag", [1, 2])
def test_readback_lag_is_detected_and_corrected(table, lag):
    # runs 8369 (1 step) and 8363 (2 steps) of the Bi experiment
    s = scan((280, 380), (500, 560))
    fit = table.fit_run_offset(s, readback_for(table, s, offset=-0.84, lag=lag))
    assert fit.lag_steps == lag
    assert "lag" in fit.kinds
    assert fit.offset_v == pytest.approx(-0.84, abs=4 * fit.offset_err_v)
    np.testing.assert_array_equal(fit.set_v, s[:-lag])


def test_without_lag_correction_the_offset_would_be_a_step_off(table):
    # the failure the lag check exists for: lag-0 pairing of a 1-step-lagged up-sweep puts every
    # reading one step (1 V) low
    s = scan((280, 380))
    rb = readback_for(table, s, offset=-0.84, lag=1)
    naive = np.mean(rb[1:] - table.shape(s[1:]))
    assert naive == pytest.approx(-1.84, abs=0.05)
    assert table.fit_run_offset(s, rb).offset_v == pytest.approx(-0.84, abs=0.05)


def test_unsettled_readings_after_jumps_are_rejected(table):
    # run 8363: -409 and -269 V at the first steps of two ranges
    s = scan((100, 150), (510, 560), (840, 890))
    rb = readback_for(table, s, offset=-0.8)
    first_510, first_840 = 51, 102
    rb[first_510] -= 408.8
    rb[first_840] -= 269.3
    fit = table.fit_run_offset(s, rb)
    assert fit.rejected_set_v == [510.0, 840.0]
    assert "2 of them at the first step after a jump" in next(
        i.message for i in fit.issues if i.kind == "unsettled"
    )
    assert fit.offset_v == pytest.approx(-0.8, abs=4 * fit.offset_err_v)
    assert fit.n_points == len(s) - 2


def test_noisy_readback_is_flagged(table):
    s = scan((0, 200))
    fit = table.fit_run_offset(s, readback_for(table, s, offset=-0.8, noise=0.4))
    assert "noisy" in fit.kinds


def test_gain_against_the_table_is_flagged_only_when_significant(table):
    wide = scan((-1500, 1500))
    rb = readback_for(table, wide, offset=-0.8) * (1 + 1000e-6)
    assert "gain" in table.fit_run_offset(wide, rb).kinds

    # the same gain over a 60 V scan is not measurable against 80 mV noise: no warning
    narrow = scan((300, 360))
    rb = readback_for(table, narrow, offset=-0.8) * (1 + 1000e-6)
    assert "gain" not in table.fit_run_offset(narrow, rb).kinds


def test_offset_far_from_the_bench_is_flagged(table):
    s = scan((0, 100))
    fit = table.fit_run_offset(s, readback_for(table, s, offset=6.0))
    assert "offset" in fit.kinds
    assert fit.offset_v == pytest.approx(6.0, abs=0.05)


def test_too_few_points(table):
    fit = table.fit_run_offset(
        np.array([100.0]), np.array([table.shape(np.array([100.0]))[0] - 0.9])
    )
    assert "too_few_points" in fit.kinds
    assert fit.offset_v == pytest.approx(-0.9)

    fit = analyse_readback(np.array([]), np.array([]), reference=table.shape)
    assert np.isnan(fit.offset_v) and fit.n_points == 0


def test_without_the_table_a_lag_looks_like_a_one_step_offset(table):
    # Why Load_Run checks against the table: on an up-sweep, reading i = applied(set[i] - 1 V), so
    # a straight line absorbs the lag into its offset and only the jumps between ranges stand out.
    # The table's DAC nonlinearity does not shift with the lag, which is what makes it visible.
    s = scan((0, 100), (300, 400))
    rb = np.random.default_rng(2).normal(0.0, 0.05, len(s))
    applied = 0.9989 * s - 0.9
    fit = analyse_readback(s, rb + np.r_[applied[0], applied[:-1]], order=1)
    assert fit.lag_steps == 0
    assert fit.offset_v == pytest.approx(-0.9 - 0.9989, abs=0.05)
    # the first reading (no earlier step to lag behind) and the one after the jump stand out
    assert fit.rejected_set_v == [0.0, 300.0]
    assert fit.gain_ppm == pytest.approx(-1100, abs=100)

    applied = table.shape(s) - 0.9
    fit = table.fit_run_offset(s, rb + np.r_[applied[0], applied[:-1]])
    assert fit.lag_steps == 1
    assert fit.offset_v == pytest.approx(-0.9, abs=0.05)


# --------------------------------------------------------------------- CLSDataFrame integration


def run_tree(table, lag=0, offset=-0.84, ranges=((280, 330), (500, 540)), step=1.0, **kwargs):
    s = scan(*ranges)
    rb = readback_for(table, s, offset=offset, lag=lag) / 1000.0  # file stores HV / VAccDiv
    n = 400
    rng = np.random.default_rng(3)
    dv = rng.choice(s, n)
    raw = np.column_stack(
        [
            1_793_000_000.0 + np.sort(rng.uniform(0, 600, n)),  # 2026-10-26
            dv,
            np.zeros(n),
            1 + (np.arange(n) % 4),
            rng.uniform(50.0, 70.0, n),
            np.full(n, 3.0),
        ]
    )
    return build_asdf_tree(
        cal_set=s,
        cal_readback=rb,
        raw=raw,
        cooler_voltage=3.0,
        step_size=step,
        scanning_ranges=[list(r) for r in ranges],
        **kwargs,
    )


def test_use_scan_calibration_applies_table_and_run_offset(tmp_path, table):
    path = write_asdf(tmp_path, run_tree(table, lag=1))
    frame = CLSDataFrame()
    with pytest.warns(CalibrationWarning, match="lags the set value by 1 step"):
        frame.Load_Run(str(path), filter_calibration=False)
    with warnings.catch_warnings():
        warnings.simplefilter("error", CalibrationWarning)  # nothing new to report
        fit = frame.Use_Scan_Calibration()

    assert frame.Scan_calibration is table
    assert fit.lag_steps == 1
    assert frame.Scan_offset_v == fit.offset_v == pytest.approx(-0.84, abs=4 * fit.offset_err_v)

    frame.Compute_Voltages()
    dv = frame.Sorted["DV"].to_numpy()
    np.testing.assert_allclose(frame.Sorted["DV_cal"].to_numpy(), table.shape(dv) + fit.offset_v)
    np.testing.assert_allclose(
        frame.Sorted["V"].to_numpy(), 3.0 * frame.VCoolDiv - frame.Sorted["DV_cal"].to_numpy()
    )


def test_offset_override_keeps_the_fit_as_diagnostic(tmp_path, table):
    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, run_tree(table))))
    fit = frame.Use_Scan_Calibration(table=TABLE_ID, offset_v=-0.5)
    frame.Compute_Voltages()
    dv = frame.Sorted["DV"].to_numpy()
    np.testing.assert_allclose(frame.Sorted["DV_cal"].to_numpy(), table.shape(dv) - 0.5)
    assert fit.offset_v == pytest.approx(-0.84, abs=0.05)


def test_run_before_the_table_date_warns(tmp_path, table):
    tree = run_tree(table)
    tree["raw"][:, 0] -= 30 * 86400  # September 2026
    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, tree)))
    with pytest.warns(CalibrationWarning, match="predates scan calibration table"):
        frame.Use_Scan_Calibration()


def test_off_grid_and_out_of_range_set_voltages_warn(tmp_path, table):
    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, run_tree(table, step=0.5))))
    with pytest.warns(CalibrationWarning, match="not on the grid"):
        frame.Use_Scan_Calibration()

    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, run_tree(table, ranges=((2950, 3000),)))))
    frame.ScanningRanges = [[2950.0, 3050.0]]
    with pytest.warns(CalibrationWarning, match="outside table"):
        frame.Use_Scan_Calibration()


def test_frequency_ranges_use_the_installed_map(tmp_path, table):
    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, run_tree(table))))
    frame.Use_Scan_Calibration()
    got = frame.Frequency_ranges(Mass=209.0, ref=0.0, harmonic=2)

    u0 = frame.Vcool_init * frame.VCoolDiv
    for (lo, hi), (f_lo, f_hi) in zip(frame.ScanningRanges, got, strict=True):
        a_lo, a_hi = table.applied(np.array([lo, hi], float), frame.Scan_offset_v)
        for u, f in ((u0 - a_hi, f_lo), (u0 - a_lo, f_hi)):
            wn = frame.dopplershift(
                2 * frame.Laser_set, u, 209.0, collinear=False, rest_to_lab=False
            )
            assert f == pytest.approx(frame.WN_to_f * wn / 1e6)


def test_load_run_clears_an_installed_map(tmp_path, table):
    frame = CLSDataFrame()
    path = write_asdf(tmp_path, run_tree(table))
    frame.Load_Run(str(path))
    frame.Use_Scan_Calibration()
    frame.Load_Run(str(path))
    assert frame.ScanVoltageMap is None and frame.Scan_calibration is None


def test_unknown_scan_model_is_rejected(tmp_path, table):
    frame = CLSDataFrame()
    frame.Load_Run(str(write_asdf(tmp_path, run_tree(table))))
    with pytest.raises(ValueError, match="unknown scan model"):
        frame.Use_Scan_Calibration(model="linear")
