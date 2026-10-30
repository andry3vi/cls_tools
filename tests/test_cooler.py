"""Tests for the cooler readback handling in Compute_Voltages: the 'smooth' correction (faulty
readings dropped, 5 s centred moving average, interpolation to each event) and the fault check
of 'pbp' / 'mean'.

The DAQ latches the latest cooler reading onto every event, so the synthetic runs do the same;
the expected smoothing is computed by brute force over the reading times.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from clstools import CalibrationWarning, CLSDataFrame

from .conftest import dask_run

VCOOLDIV = 10000


def cooler_run(fault_at=None, n_events=20000, seed=0):
    """Events over 60 s with a cooler reading every ~0.1 s: 30 kV, 0.31 V rms ripple aliased to a
    2.4 s period, 0.2 V/min drift. Returns the event frame and the readings (t, v) in Vrfq units."""
    rng = np.random.default_rng(seed)
    t_r = np.cumsum(rng.uniform(0.08, 0.12, 600))
    v_r = (30000.0 + 0.44 * np.sin(2 * np.pi * t_r / 2.4) + 0.2 * t_r / 60) / VCOOLDIV
    if fault_at is not None:
        v_r[fault_at] = 26000.0 / VCOOLDIV  # run 7504: readback drop to 26 kV
    ts = np.sort(rng.uniform(t_r[0], t_r[-1] + 0.1, n_events))
    vrfq = v_r[np.searchsorted(t_r, ts, side="right") - 1]
    events = pd.DataFrame({"TS": 1_793_000_000.0 + ts, "DV": np.zeros(n_events), "Vrfq": vrfq})
    return events, 1_793_000_000.0 + t_r, v_r


def frame_for(events):
    frame = CLSDataFrame(VAccDiv=1000, VCoolDiv=VCOOLDIV)
    frame.Run = dask_run(events, npartitions=4)
    frame.Cal_order = 1
    frame.Cal = [0.0, 1.0 / 1000]  # applied scan voltage = DV = 0
    frame.run_number = 1
    return frame


def expected_smooth(events, window_s=5.0, drop=None):
    """Brute force: each reading placed where it first appears among the events, averaged with
    every reading within +-window/2 of it, then interpolated linearly to the event times."""
    ts, vrfq = events["TS"].to_numpy(), events["Vrfq"].to_numpy()
    first = np.r_[0, np.flatnonzero(np.diff(vrfq) != 0) + 1]
    t, v = ts[first], vrfq[first]
    if drop is not None:
        keep = np.abs(v * VCOOLDIV - np.median(v * VCOOLDIV)) <= drop
        t, v = t[keep], v[keep]
    v_s = np.array([v[np.abs(t - ti) <= window_s / 2].mean() for ti in t])
    return np.interp(ts, t, v_s)


def test_smooth_matches_brute_force_average():
    events, _, _ = cooler_run()
    frame = frame_for(events)
    frame.Compute_Voltages(cooler_correction="smooth")

    got = frame.Sorted.sort_values("TS")
    want = expected_smooth(events)
    np.testing.assert_allclose(got["Vrfq_smooth"].to_numpy(), want, rtol=0, atol=1e-12)
    np.testing.assert_allclose(got["V"].to_numpy(), want * VCOOLDIV, rtol=0, atol=1e-8)
    assert frame.Cooler_faulty_readings == 0


def test_smooth_removes_the_aliased_ripple_and_keeps_the_drift():
    events, t_r, _ = cooler_run()
    frame = frame_for(events)
    frame.Compute_Voltages(cooler_correction="smooth")
    got = frame.Sorted.sort_values("TS")
    inner = (got["TS"] > t_r[0] + 5) & (got["TS"] < t_r[-1] - 5)
    drift = 30000.0 + 0.2 * (got["TS"] - 1_793_000_000.0) / 60

    held = got["Vrfq"] * VCOOLDIV - drift  # 'pbp': ripple at 0.31 V rms
    smooth = got["V"] - drift
    assert held[inner].std() == pytest.approx(0.31, rel=0.1)
    assert smooth[inner].std() < 0.05
    assert abs(smooth[inner].mean()) < 0.02


def test_smooth_drops_faulty_readings():
    events, _, _ = cooler_run(fault_at=300)
    frame = frame_for(events)
    with pytest.warns(CalibrationWarning, match="1 cooler readings further than 5 V .* dropped"):
        frame.Compute_Voltages(cooler_correction="smooth")
    assert frame.Cooler_faulty_readings == 1
    got = frame.Sorted.sort_values("TS")
    want = expected_smooth(events, drop=5.0)
    np.testing.assert_allclose(got["Vrfq_smooth"].to_numpy(), want, rtol=0, atol=1e-12)
    assert got["V"].min() > 29999.0


def test_pbp_reports_faulty_readings_but_uses_them():
    events, _, _ = cooler_run(fault_at=300)
    frame = frame_for(events)
    with pytest.warns(CalibrationWarning, match="'pbp' uses them as recorded"):
        frame.Compute_Voltages(cooler_correction="pbp")
    assert frame.Cooler_faulty_readings == 1
    np.testing.assert_allclose(frame.Sorted["V"].to_numpy(), events["Vrfq"].to_numpy() * VCOOLDIV)


def test_fault_check_can_be_disabled():
    events, _, _ = cooler_run(fault_at=300)
    frame = frame_for(events)
    frame.Compute_Voltages(cooler_correction="smooth", cooler_max_dev_v=None)
    assert frame.Cooler_faulty_readings == 0
    assert frame.Sorted["V"].min() < 29950.0  # the 26 kV reading is averaged in (~ -80 V)


def test_window_is_configurable():
    events, _, _ = cooler_run()
    frame = frame_for(events)
    frame.Compute_Voltages(cooler_correction="smooth", cooler_window_s=1.0)
    want = expected_smooth(events, window_s=1.0)
    np.testing.assert_allclose(
        frame.Sorted.sort_values("TS")["Vrfq_smooth"].to_numpy(), want, rtol=0, atol=1e-12
    )


def test_unknown_cooler_correction_is_rejected():
    events, _, _ = cooler_run(n_events=100)
    with pytest.raises(AttributeError, match="pbp, mean and smooth"):
        frame_for(events).Compute_Voltages(cooler_correction="interp")
