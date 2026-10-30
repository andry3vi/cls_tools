"""Tests for the PMT counting check (clstools.counting) and its use in CLSDataFrame: the Fano
factor of the counts per trigger, the afterpulse-burst indicators, the artificial per-PMT dead
time, and the warnings.

Runs are simulated trigger by trigger: a sweep of scan steps, each a block of triggers followed
by settling triggers without events, Poisson counts per PMT with a resonance-shaped rate, and
optionally afterpulses, an intensity shared by the PMTs, or a cap on the counts per trigger.
"""

from __future__ import annotations

import warnings

import numpy as np
import pytest

from clstools import CalibrationWarning, CountingWarning, check_counting
from clstools.counting import dead_time_keep, trigger_period_us

from .conftest import build_asdf_tree, write_asdf

PERIOD = 1000.0  # us
TS0 = 1_793_000_000.0


def simulate(
    *,
    n_steps=40,
    per_step=25,
    gap=5,
    sweeps=20,
    mean=0.5,
    pmts=(3, 4),
    afterpulse=None,
    shared_shape=None,
    cap=None,
    seed=0,
):
    """Raw events (TS, DV, Bunch, TDC, TOF, Vrfq) of a simulated run.

    mean: counts per trigger per PMT off resonance (a peak doubles it mid-scan).
    afterpulse: {pmt: m}, every event of that PMT is followed by Poisson(m) afterpulses
        0.5-3 us later.
    shared_shape: gamma shape of a per-trigger intensity common to all PMTs (mean 1).
    cap: at most this many events per PMT and trigger (counting losses).
    """
    rng = np.random.default_rng(seed)
    dv = np.arange(n_steps, dtype=float)
    rate = mean * (1 + np.exp(-0.5 * ((dv - n_steps / 2) / 3) ** 2))
    block = per_step + gap
    trig = np.arange(sweeps * n_steps * block)
    step = (trig // block) % n_steps
    live = trig % block < per_step
    intensity = np.ones(len(trig))
    if shared_shape is not None:
        intensity = rng.gamma(shared_shape, 1 / shared_shape, len(trig))
    columns = []
    for pmt in pmts:
        n = rng.poisson(rate[step] * intensity) * live
        if cap is not None:
            n = np.minimum(n, cap)
        bunch = np.repeat(trig, n)
        tof = rng.uniform(0.0, PERIOD, len(bunch))
        m = (afterpulse or {}).get(pmt, 0.0)
        if m:
            k = rng.poisson(m, len(bunch))
            ap_bunch = np.repeat(bunch, k)
            ap_tof = np.repeat(tof, k) + rng.uniform(0.5, 3.0, k.sum())
            inside = ap_tof < PERIOD
            bunch = np.r_[bunch, ap_bunch[inside]]
            tof = np.r_[tof, ap_tof[inside]]
        columns.append((bunch, np.full(len(bunch), pmt), tof))
    bunch = np.concatenate([c[0] for c in columns])
    tdc = np.concatenate([c[1] for c in columns]).astype(float)
    tof = np.concatenate([c[2] for c in columns])
    order = np.lexsort((tof, bunch))
    bunch, tdc, tof = bunch[order], tdc[order], tof[order]
    ts = TS0 + np.floor(bunch * PERIOD * 1e-6 / 0.01) * 0.01  # DAQ timestamps, 10 ms resolution
    dv_ev = dv[(bunch // block) % n_steps]
    return np.column_stack([ts, dv_ev, bunch.astype(float), tdc, tof, np.full(len(ts), 0.3)])


def check(raw):
    return check_counting(raw[:, 1], raw[:, 2], raw[:, 3], raw[:, 4], PERIOD)


def naive_fano(raw, pmt):
    """Fano factor over every trigger from a step visit's first to its last event (edges included),
    the estimate that is biased low when the events are sparse."""
    dv, bunch = raw[:, 1], raw[:, 2].astype(int)
    on = raw[:, 3] == pmt
    visit = np.r_[0, np.cumsum(np.diff(dv) != 0)]
    num = den = 0.0
    for v in np.unique(dv):
        counts = []
        for i in np.unique(visit[dv == v]):
            b = bunch[visit == i]
            counts.append(np.bincount(bunch[(visit == i) & on] - b.min(), minlength=np.ptp(b) + 1))
        c = np.concatenate(counts)
        num += c.var() * len(c)
        den += c.mean() * len(c)
    return num / den


# --------------------------------------------------------------------- statistics


def test_poisson_counting_gives_fano_one_and_no_issue():
    c = check(simulate())
    assert c.issues == ()
    for pmt in c.pmts.values():
        assert pmt.fano == pytest.approx(1.0, abs=max(0.03, 4 * pmt.fano_err))
        assert pmt.burst_fraction == pytest.approx(pmt.burst_expected, abs=0.002)
        assert pmt.fano_dead == pytest.approx(1.0, abs=0.04)
    assert c.fano_all == pytest.approx(1.0, abs=0.03)
    assert c.correlated_excess == pytest.approx(0.0, abs=0.03)
    assert c.period_us == PERIOD


def test_sparse_steps_are_not_biased_by_their_edges():
    # 0.1 counts per trigger, 6 triggers per step: the triggers holding a visit's first and
    # last event always count >= 1, so including them pulls the Fano factor well below 1.
    raw = simulate(mean=0.1, per_step=6, gap=3, sweeps=300)
    c = check(raw)
    assert c.pmts[3].fano == pytest.approx(1.0, abs=4 * c.pmts[3].fano_err + 0.02)
    assert naive_fano(raw, 3) < 0.85
    assert c.issues == ()


def test_one_trigger_per_step_gives_no_fano_and_no_warning():
    c = check(simulate(per_step=1, gap=0, sweeps=200))
    assert np.isnan(c.pmts[3].fano) and np.isnan(c.fano_all)
    assert c.n_fano_triggers == 0
    assert c.issues == ()


def test_afterpulsing_pmt_is_flagged_and_a_dead_time_cures_it():
    # compound Poisson: Fano = (1 + 3m + m^2) / (1 + m) = 1.53 for m = 0.3
    c = check(simulate(afterpulse={4: 0.3}))
    p3, p4 = c.pmts[3], c.pmts[4]
    assert p4.fano == pytest.approx(1.53, abs=0.05)
    assert p4.burst_excess > 0.2  # 0.3 / 1.3 of the events are afterpulses
    assert p4.fano_dead == pytest.approx(1.0, abs=0.05)
    assert p3.fano == pytest.approx(1.0, abs=0.04)
    assert [(i.kind, i.channel) for i in c.issues] == [("super_poisson", 4)]
    assert "afterpulse bursts" in c.issues[0].message
    assert "apply_dead_time" in c.issues[0].message


def test_shared_intensity_fluctuations_are_reported_as_correlated():
    # gamma intensity with variance 1/2: each PMT's Fano factor is 1 + 0.5 * its counts per
    # trigger, and the sum's exceeds theirs by the same amount (the covariance); a dead time
    # cannot fix it
    c = check(simulate(shared_shape=2.0, mean=0.3))
    single = np.mean([p.fano for p in c.pmts.values()])
    assert c.correlated_excess == pytest.approx(single - 1, abs=0.03)
    assert c.correlated_excess > 0.15
    assert {"correlated", "super_poisson"} <= c.kinds
    assert all("does not fix it" in i.message for i in c.issues if i.kind == "super_poisson")


def test_counting_losses_are_sub_poisson():
    c = check(simulate(mean=1.0, cap=1))
    assert c.pmts[3].fano < 0.6
    assert "sub_poisson" in c.kinds
    assert "apply_dead_time" not in " ".join(i.message for i in c.issues)


def test_few_events_on_a_channel_are_reported_but_not_warned():
    raw = simulate(afterpulse={4: 0.3})
    extra = raw[:5].copy()
    extra[:, 3] = 1.0  # a noise channel with 5 events
    c = check(np.r_[raw, extra])
    assert c.pmts[1].events == 5
    assert all(i.channel != 1 for i in c.issues)


def test_event_order_does_not_matter():
    raw = simulate(afterpulse={4: 0.3})
    shuffled = raw[np.random.default_rng(1).permutation(len(raw))]
    a, b = check(raw), check(shuffled)
    for ch in a.pmts:
        assert a.pmts[ch].fano == pytest.approx(b.pmts[ch].fano, rel=1e-12)
        assert a.pmts[ch].burst_fraction == b.pmts[ch].burst_fraction


# --------------------------------------------------------------------- dead time


def test_dead_time_is_extending_and_per_pmt():
    bunch = np.zeros(6)
    tdc = np.array([3, 3, 3, 3, 4, 4])
    tof = np.array([10.0, 13.0, 16.0, 19.0, 11.0, 14.0])
    # extending: 13, 16 and 19 each come 3 us after the previous event of PMT 3, kept or not
    np.testing.assert_array_equal(dead_time_keep(bunch, tdc, tof, PERIOD, 5.0), [1, 0, 0, 0, 1, 0])
    # one dead time per channel; channels not listed keep everything
    np.testing.assert_array_equal(
        dead_time_keep(bunch, tdc, tof, PERIOD, {4: 2.0}), [1, 1, 1, 1, 1, 1]
    )
    np.testing.assert_array_equal(
        dead_time_keep(bunch, tdc, tof, PERIOD, {3: 2.0}), [1, 1, 1, 1, 1, 1]
    )
    np.testing.assert_array_equal(
        dead_time_keep(bunch, tdc, tof, PERIOD, {3: 5.0}), [1, 0, 0, 0, 1, 1]
    )


def test_dead_time_spans_consecutive_triggers_and_any_order():
    bunch = np.array([1.0, 0.0, 0.0])
    tdc = np.array([3, 3, 3])
    tof = np.array([1.0, 998.0, 500.0])  # 1 us into trigger 1 is 3 us after 998 us of trigger 0
    np.testing.assert_array_equal(dead_time_keep(bunch, tdc, tof, PERIOD, 5.0), [0, 1, 1])


def test_trigger_period_from_timestamps():
    bunch = np.arange(0, 50_000, 7, dtype=float)
    ts = TS0 + np.floor(bunch * 1e-3 / 0.01) * 0.01
    assert trigger_period_us(ts, bunch, np.full(len(ts), 900.0)) == 1000.0
    # a 5 s run is too short for the 10 ms timestamps: max(1000 us, longest ToF)
    short = bunch < 5000
    assert trigger_period_us(ts[short], bunch[short], np.full(short.sum(), 1500.0)) == 1500.0


# --------------------------------------------------------------------- CLSDataFrame


def counting_warnings(caught):
    return [str(w.message) for w in caught if issubclass(w.category, CountingWarning)]


@pytest.fixture
def afterpulsing_run(tmp_path):
    raw = simulate(afterpulse={4: 0.3})
    cal_set = np.arange(-5.0, 6.0)
    tree = build_asdf_tree(
        run=8371, raw=raw, cal_set=cal_set, cal_readback=cal_set / 1000, scanning_ranges=[[0, 39]]
    )
    return write_asdf(tmp_path, tree), raw


def load(path, **kwargs):
    from clstools import CLSDataFrame

    frame = CLSDataFrame()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame.Load_Run(str(path), **kwargs)
    return frame, counting_warnings(caught)


def test_load_run_warns_about_the_afterpulsing_pmt(afterpulsing_run):
    path, raw = afterpulsing_run
    frame, messages = load(path)
    assert len(messages) == 1 and messages[0].startswith("run 8371: PMT 4 counts are not Poisson")
    assert frame.Trigger_period_us == PERIOD
    assert frame.Counting_check is frame.Counting_check_loaded
    assert frame.Counting_check.pmts[4].events == int(np.sum(raw[:, 3] == 4))


def test_load_run_can_skip_the_check(afterpulsing_run):
    frame, messages = load(afterpulsing_run[0], counting_check=False)
    assert messages == [] and frame.Counting_check is None
    with pytest.warns(CountingWarning, match="PMT 4"):  # still available on demand
        assert frame.Check_Counting().pmts[4].fano > 1.4


def test_apply_dead_time_removes_the_afterpulses(afterpulsing_run, capsys):
    frame, _ = load(afterpulsing_run[0])
    n_before = len(frame.Run)
    with warnings.catch_warnings():
        warnings.simplefilter("error", CountingWarning)  # cured: no new warning
        frame.apply_dead_time(5.0)
    assert frame.Dead_time_us == 5.0
    assert frame.Dead_time_removed[4] == pytest.approx(0.3 / 1.3, abs=0.02)
    assert frame.Dead_time_removed[3] < 0.01
    # PMT 4 has 1.3x the events of PMT 3, so 0.3 / 2.3 of all events are afterpulses
    assert len(frame.Run) == pytest.approx(n_before * (1 - 0.3 / 2.3), rel=0.01)
    assert frame.Counting_check.pmts[4].fano == pytest.approx(1.0, abs=0.05)
    assert frame.Counting_check_loaded.pmts[4].fano > 1.4

    frame.Frequency_stepsize = 1e6
    frame.Info()
    out = capsys.readouterr().out
    assert "PMT counting" in out and "Fano as loaded" in out and "Dead time" in out


def test_apply_dead_time_refreshes_sorted(afterpulsing_run):
    frame, _ = load(afterpulsing_run[0])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", CalibrationWarning)
        frame.Compute_Voltages()
    frame.apply_dead_time({4: 5.0})
    assert len(frame.Sorted) == len(frame.Run)
    assert frame.Dead_time_removed[3] == 0.0


def test_apply_filter_rechecks_the_counting(afterpulsing_run):
    frame, _ = load(afterpulsing_run[0])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", CountingWarning)
        frame.apply_filter(filter_window=10_000)
    assert frame.Counting_check is not frame.Counting_check_loaded
    assert frame.Counting_check.pmts[4].fano == pytest.approx(
        frame.Counting_check_loaded.pmts[4].fano
    )


def test_info_to_csv_has_the_counting_columns(afterpulsing_run, tmp_path):
    import pandas as pd

    frame, _ = load(afterpulsing_run[0])
    frame.apply_dead_time(5.0)
    out = tmp_path / "info.csv"
    frame.Info_to_csv(str(out))
    row = pd.read_csv(out).iloc[0]
    assert row["Trigger_period_us"] == PERIOD and row["Dead_time_us"] == 5.0
    assert row["Fano_pmt4"] == pytest.approx(1.0, abs=0.05)
    assert {"Events_pmt3", "Burst_fraction_pmt3", "Fano_dead_pmt3", "Fano_all"} <= set(row.index)
