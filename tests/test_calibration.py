"""Tests for Load_Run's polynomial calibration: fitting, ordering, outlier
filtering, and ignore_intercept. Uses synthetic ASDF files (see conftest.py)
since no real run data is available in this repo.
"""

from __future__ import annotations

import numpy as np
import pytest

from clstools import CLSDataFrame

from .conftest import build_asdf_tree, write_asdf


def test_run_metadata_is_read_from_the_tree(synthetic_run_path):
    frame = CLSDataFrame()
    frame.Load_Run(str(synthetic_run_path))

    assert frame.run_number == 1
    assert frame.Vcool_init == pytest.approx(0.5)
    assert frame.Laser_set == pytest.approx(15000.0)
    assert frame.Step_Size == pytest.approx(1.0)
    assert frame.ScanningRanges == [[-5.0, 5.0]]
    assert frame.Size == 20


def test_linear_calibration_recovers_known_coefficients(synthetic_run_path):
    # build_asdf_tree's default CalReadback = 2.0*CalSet + 0.5, exact (no noise).
    # filter_calibration=False: with a perfectly linear fit the residuals are
    # pure float roundoff (~1e-15) and the 2-sigma filter is not meaningful on
    # them -- outlier filtering itself is exercised separately below.
    frame = CLSDataFrame()
    frame.Load_Run(str(synthetic_run_path), cal_order=1, filter_calibration=False)

    # Cal is reversed to ascending order: Cal[0] is the intercept (see CLAUDE.md).
    assert frame.Cal[0] == pytest.approx(0.5, abs=1e-9)
    assert frame.Cal[1] == pytest.approx(2.0, abs=1e-9)


def test_quadratic_calibration_recovers_known_coefficients(tmp_path):
    cal_set = np.array([-2.0, -1.0, 0.0, 1.0, 2.0])
    cal_readback = 0.3 * cal_set**2 + 2.0 * cal_set + 0.5  # exact quadratic
    path = write_asdf(tmp_path, build_asdf_tree(cal_set=cal_set, cal_readback=cal_readback))

    frame = CLSDataFrame()
    frame.Load_Run(str(path), cal_order=2)

    assert frame.Cal[0] == pytest.approx(0.5, abs=1e-8)
    assert frame.Cal[1] == pytest.approx(2.0, abs=1e-8)
    assert frame.Cal[2] == pytest.approx(0.3, abs=1e-8)


def _cal_set_with_one_outlier() -> tuple[np.ndarray, np.ndarray]:
    # 21 exactly-linear inlier points dilute the outlier's leverage on the fit
    # enough that its residual clears 2 sigma of the (mostly-clean) residual
    # distribution -- see the numerical exploration in this PR's description.
    inliers = np.linspace(-10.0, 10.0, 21)
    cal_set = np.append(inliers, 25.0)
    cal_readback = 3.0 * cal_set + 1.0
    cal_readback[-1] += 200.0
    return cal_set, cal_readback


def test_outlier_filtering_drops_the_outlier_and_refits(tmp_path):
    cal_set, cal_readback = _cal_set_with_one_outlier()
    path = write_asdf(tmp_path, build_asdf_tree(cal_set=cal_set, cal_readback=cal_readback))

    frame = CLSDataFrame()
    frame.Load_Run(str(path), cal_order=1, filter_calibration=True)

    assert frame.Dropped_calibration_points == pytest.approx([25.0])

    # The refit should match an independent polyfit over the inliers only.
    inlier_mask = cal_set != 25.0
    expected_slope, expected_intercept = np.polyfit(
        cal_set[inlier_mask], cal_readback[inlier_mask], 1
    )
    assert frame.Cal[0] == pytest.approx(expected_intercept, abs=1e-6)
    assert frame.Cal[1] == pytest.approx(expected_slope, abs=1e-6)


def test_filter_calibration_false_keeps_the_outlier(tmp_path):
    cal_set, cal_readback = _cal_set_with_one_outlier()
    path = write_asdf(tmp_path, build_asdf_tree(cal_set=cal_set, cal_readback=cal_readback))

    frame = CLSDataFrame()
    frame.Load_Run(str(path), cal_order=1, filter_calibration=False)

    expected_slope, expected_intercept = np.polyfit(cal_set, cal_readback, 1)
    assert frame.Cal[0] == pytest.approx(expected_intercept, abs=1e-6)
    assert frame.Cal[1] == pytest.approx(expected_slope, abs=1e-6)


def test_ignore_intercept_zeroes_cal0(synthetic_run_path):
    frame = CLSDataFrame()
    frame.Load_Run(str(synthetic_run_path), cal_order=1, ignore_intercept=True)

    assert frame.Cal[0] == 0
    assert frame.Cal_err[0] == 0
    assert frame.Cal[1] == pytest.approx(2.0, abs=1e-9)


def test_update_cal_refits_current_cal_df(synthetic_run_path):
    frame = CLSDataFrame()
    frame.Load_Run(str(synthetic_run_path), cal_order=1)
    original_slope = frame.Cal[1]

    frame.Update_Cal(cal_order=2)

    assert frame.Cal_order == 2
    assert len(frame.Cal) == 3
    # Quadratic term should be ~0 since the underlying data is exactly linear.
    assert frame.Cal[2] == pytest.approx(0.0, abs=1e-6)
    assert frame.Cal[1] == pytest.approx(original_slope, abs=1e-6)
