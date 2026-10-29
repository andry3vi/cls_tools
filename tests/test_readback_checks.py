"""Tests for the calibration readback checks Load_Run always runs, and for the faults in the
file's calibration data it now survives instead of crashing (CalSet/CalReadback length
mismatch, non-finite values, single-point calibrations).
"""

from __future__ import annotations

import warnings

import numpy as np
import pytest

from clstools import CalibrationWarning, CLSDataFrame, load_table

from .conftest import build_asdf_tree, write_asdf


def realistic_cal(lag=0, noise=0.08, seed=0):
    """CalSet over two ranges and a readback (HV / 1000) following the packaged scan table with
    an offset of -0.9 V, with the in-run readback noise of the Bi runs."""
    s = np.r_[np.arange(100.0, 161.0), np.arange(400.0, 461.0)]
    applied = load_table("scan_hv_2026-10-24").shape(s) - 0.9
    if lag:
        applied = np.r_[np.full(lag, applied[0]), applied[:-lag]]
    rb = applied + np.random.default_rng(seed).normal(0.0, noise, len(s))
    return s, rb / 1000.0


def load(tmp_path, _kw=None, **tree):
    frame = CLSDataFrame()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame.Load_Run(str(write_asdf(tmp_path, build_asdf_tree(**tree))), **(_kw or {}))
    messages = [str(w.message) for w in caught if issubclass(w.category, CalibrationWarning)]
    return frame, messages


def test_clean_readback_raises_no_warning(tmp_path):
    s, rb = realistic_cal()
    frame, messages = load(tmp_path, cal_set=s, cal_readback=rb)
    assert messages == []
    assert frame.Readback_check.issues == ()
    assert frame.Readback_check.lag_steps == 0
    assert frame.Readback_check.model == "scan_hv_2026-10-24:lut"
    assert frame.Readback_check.offset_v == pytest.approx(-0.9, abs=0.05)
    assert abs(frame.Readback_check.gain_ppm) < 5 * frame.Readback_check.gain_err_ppm


def test_lagged_readback_warns_but_changes_no_numbers(tmp_path):
    s, rb = realistic_cal(lag=1)
    frame, messages = load(tmp_path, cal_set=s, cal_readback=rb)
    assert any("lags the set value by 1 step" in m for m in messages)
    assert frame.Readback_check.lag_steps == 1
    # default filter: the polynomial is still fitted to the lag-0 pairs, as before
    np.testing.assert_array_equal(frame.Cal_raw["Set"], s)
    assert frame.Cal_df["Set"].iloc[0] == s[0]


def test_robust_filter_corrects_lag_and_drops_unsettled(tmp_path):
    s, rb = realistic_cal(lag=1)
    # a -400 V reading taken right after the jump to the second range; with the 1-step lag it
    # belongs to the last step of the first range, set[60]
    rb[61] -= 0.4
    frame = CLSDataFrame()
    with pytest.warns(CalibrationWarning):
        frame.Load_Run(
            str(write_asdf(tmp_path, build_asdf_tree(cal_set=s, cal_readback=rb))),
            filter_calibration="robust",
        )
    # lag-1 pairs: set[i] with readback[i + 1]; the unsettled reading belongs to set[60]
    assert frame.Dropped_calibration_points == [s[60]]
    keep = np.ones(len(s) - 1, bool)
    keep[60] = False
    slope, intercept = np.polyfit(s[:-1][keep], rb[1:][keep], 1)
    assert frame.Cal[1] == pytest.approx(slope, rel=1e-9)
    assert frame.Cal[0] == pytest.approx(intercept, abs=1e-9)


def test_invalid_filter_calibration_value(tmp_path):
    with pytest.raises(ValueError, match="filter_calibration"):
        CLSDataFrame().Load_Run(
            str(write_asdf(tmp_path, build_asdf_tree())), filter_calibration="x"
        )


def test_length_mismatch_is_truncated_with_a_warning(tmp_path):
    s, rb = realistic_cal()
    frame, messages = load(tmp_path, cal_set=s, cal_readback=rb[:-3])
    assert any("CalSet has 122 values but CalReadback 119" in m for m in messages)
    assert len(frame.Cal_raw) == 119
    np.testing.assert_array_equal(frame.Cal_raw["Set"], s[:119])


def test_non_finite_readback_values_are_ignored(tmp_path):
    s, rb = realistic_cal()
    rb[[5, 70]] = np.nan
    frame, messages = load(tmp_path, cal_set=s, cal_readback=rb, _kw={"filter_calibration": False})
    assert any("2 non-finite calibration values ignored" in m for m in messages)
    assert np.all(np.isfinite(frame.Cal))
    finite = np.isfinite(rb)
    slope, intercept = np.polyfit(s[finite], rb[finite], 1)
    assert frame.Cal[1] == pytest.approx(slope) and frame.Cal[0] == pytest.approx(intercept)


def test_single_point_calibration_no_longer_crashes(tmp_path):
    # 64 archive runs (Aug 2026 sweep) raised LinAlgError: Singular matrix here
    frame, messages = load(tmp_path, cal_set=np.array([100.0]), cal_readback=np.array([0.0991]))
    assert any("usable calibration point" in m for m in messages)
    assert any("nominal calibration" in m for m in messages)
    assert frame.Cal == [0.0, 1.0 / frame.VAccDiv]
    assert all(np.isnan(frame.Cal_err))


def test_few_points_fit_without_uncertainties(tmp_path):
    frame, messages = load(
        tmp_path,
        cal_set=np.array([100.0, 200.0]),
        cal_readback=np.array([0.0991, 0.1990]),
        _kw={"filter_calibration": False},
    )
    assert any("Cal_err is NaN" in m for m in messages)
    assert frame.Cal[1] == pytest.approx((0.1990 - 0.0991) / 100.0)
    assert all(np.isnan(frame.Cal_err))


def test_disconnected_readback_flags_the_gain(tmp_path):
    s, _ = realistic_cal()
    rb = np.random.default_rng(0).normal(0.0, 1e-5, len(s))  # readback stuck near 0 V
    _, messages = load(tmp_path, cal_set=s, cal_readback=rb)
    assert any("readback is not connected" in m for m in messages)


def test_noisy_readback_is_reported_by_the_table_not_by_load_run(tmp_path):
    # pre-2025 runs: 0.3-1.3 V rms readback noise, a property of the data rather than a fault
    s, rb = realistic_cal(noise=0.6)
    frame, messages = load(tmp_path, cal_set=s, cal_readback=rb)
    assert messages == []
    assert "noisy" in frame.Readback_check.kinds
    with pytest.warns(CalibrationWarning, match="readback residual rms"):
        frame.Use_Scan_Calibration()
