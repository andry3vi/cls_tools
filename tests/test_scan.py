"""Tests for clstools.scan: the scan sequence and the dwells per set voltage reconstructed from
the events, against runs simulated as the DAQ takes them (conftest.simulate_scan)."""

from __future__ import annotations

import numpy as np
import pytest

from clstools import scan_dwells, scan_sequence

from .conftest import simulate_scan


def dwells_of(events, ranges=None, step=1.0, **kw):
    return scan_dwells(events["DV"].to_numpy(), events["Bunch"].to_numpy(), ranges, step, **kw)


def test_sequence_is_the_ranges_in_listed_order():
    seq = scan_sequence([[5.0, 7.0], [-2.0, -4.0], [10.0, 10.0]], 1.0)
    np.testing.assert_array_equal(seq, [5, 6, 7, -2, -3, -4, 10])
    np.testing.assert_array_equal(scan_sequence([[0.0, 1.0]], 0.5), [0, 0.5, 1])


def test_dense_up_down_scan_end_points_have_full_exposure():
    ranges = [[0.0, 9.0]]
    events, truth = simulate_scan(ranges, cycles=20, bunches_per_channel=20, settle=(2, 6))
    sd = dwells_of(events, ranges)
    assert sd.pattern == "updown" and sd.sequence_from == "ranges"
    assert sd.dwells == truth
    assert sd.dwells[0.0] == sd.dwells[5.0] == 40  # both turn dwells count
    assert set(sd.sweeps().values()) == {20.0}
    assert sd.cycles == pytest.approx(20, abs=0.05)
    assert sd.triggers_per_dwell == pytest.approx(24, abs=1.5)


def test_sparse_one_trigger_per_step_is_exact():
    # online runs: one trigger per step, ~1 event per visit, many dwells without events
    ranges = [[0.0, 30.0], [50.0, 60.0]]
    events, truth = simulate_scan(ranges, cycles=15, events_per_dwell=0.5, seed=3)
    sd = dwells_of(events, ranges)
    assert sd.pattern == "updown"
    assert sd.dwells == truth
    assert sd.residual == pytest.approx(0.0)


@pytest.mark.parametrize("events_per_dwell", [0.2, 0.5, 2.0])
def test_sparse_scan_with_settling_triggers(events_per_dwell):
    ranges = [[0.0, 40.0]]
    events, truth = simulate_scan(
        ranges,
        cycles=12,
        bunches_per_channel=15,
        settle=(1, 6),
        events_per_dwell=events_per_dwell,
        seed=5,
    )
    sd = dwells_of(events, ranges)
    assert sd.pattern == "updown"
    assert max(abs(sd.dwells[v] - truth[v]) for v in sd.dwells) <= 1


def test_sawtooth_scan():
    ranges = [[100.0, 120.0]]
    events, truth = simulate_scan(ranges, pattern="sawtooth", cycles=15, events_per_dwell=0.7)
    sd = dwells_of(events, ranges)
    assert sd.pattern == "sawtooth"
    assert sd.dwells == truth
    assert all(n == 1 for n in sd.per_cycle.values())


def test_ranges_out_of_order_and_descending():
    ranges = [[20.0, 30.0], [-5.0, -15.0], [0.0, 5.0]]
    events, truth = simulate_scan(ranges, cycles=8, events_per_dwell=0.6, seed=2)
    sd = dwells_of(events, ranges)
    assert sd.pattern == "updown" and sd.dwells == truth


def test_range_listed_twice_has_twice_the_exposure():
    ranges = [[0.0, 4.0], [10.0, 14.0], [10.0, 14.0]]
    events, truth = simulate_scan(ranges, cycles=10, events_per_dwell=3, seed=4)
    sd = dwells_of(events, ranges)
    assert sd.dwells == truth
    assert sd.per_cycle[2.0] == 2 and sd.per_cycle[12.0] == 4
    sweeps = sd.sweeps()
    assert sweeps[2.0] == pytest.approx(sweeps[12.0], abs=0.5)


def test_short_run_partial_cycle():
    ranges = [[0.0, 19.0]]
    events, truth = simulate_scan(ranges, cycles=1.25, events_per_dwell=4, seed=6)
    sd = dwells_of(events, ranges)
    assert sd.dwells == truth
    sweeps = sd.sweeps()
    # up, down, and up again to 9 V: 0-9 V were scanned 1.5 times, 10-19 V once
    assert sweeps[5.0] == 1.5 and sweeps[15.0] == 1.0 and sweeps[19.0] == 1.0


def test_without_ranges_the_sequence_comes_from_the_events():
    events, truth = simulate_scan([[0.0, 9.0]], cycles=10, bunches_per_channel=10, settle=(1, 3))
    sd = dwells_of(events, None, None)
    assert sd.sequence_from == "events" and sd.pattern == "updown"
    assert sd.dwells == truth


def test_events_outside_the_ranges_fall_back_to_the_events():
    events, truth = simulate_scan([[0.0, 9.0]], cycles=10, bunches_per_channel=10, settle=(1, 3))
    sd = dwells_of(events, [[0.0, 5.0]])
    assert sd.sequence_from == "events" and sd.dwells == truth


def test_descending_sawtooth_without_ranges():
    events, truth = simulate_scan([[9.0, 0.0]], pattern="sawtooth", cycles=10, settle=(0, 1))
    sd = dwells_of(events, None, None)
    assert sd.pattern == "sawtooth" and sd.dwells == truth


def test_pattern_can_be_forced_and_is_checked():
    events, _ = simulate_scan([[0.0, 9.0]], cycles=5)
    assert dwells_of(events, [[0.0, 9.0]], pattern="sawtooth").pattern == "sawtooth"
    with pytest.raises(ValueError, match="pattern"):
        dwells_of(events, [[0.0, 9.0]], pattern="zigzag")


def test_no_events():
    sd = scan_dwells(np.zeros(0), np.zeros(0), [[0.0, 9.0]], 1.0)
    assert sd.dwells == {} and sd.cycles == 0.0
