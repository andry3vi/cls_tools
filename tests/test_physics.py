"""Independent checks of the relativistic Doppler formulas.

These re-derive the expected factor from the standard relativistic
kinematics (total energy / momentum, then the textbook longitudinal Doppler
formula sqrt((1+beta)/(1-beta))) rather than calling `dopplerfactor` twice, so
a sign or algebra error in `DataFrame.py` would show up as a mismatch here.
"""

from __future__ import annotations

import numpy as np
import pytest

from clstools import CLSDataFrame

E = 1.602176634e-19  # C
C = 299792458.0  # m/s
MU = 1.66053904e-27  # kg, atomic mass unit


def independent_beta(voltage: float, mass_amu: float) -> float:
    """beta from relativistic energy-momentum relation, derived independently
    of CLSDataFrame.dopplerfactor's own expression."""
    m = mass_amu * MU
    rest_energy = m * C**2
    total_energy = E * voltage + rest_energy
    gamma = total_energy / rest_energy
    return np.sqrt(1.0 - 1.0 / gamma**2)


def independent_doppler_factor(voltage: float, mass_amu: float) -> float:
    beta = independent_beta(voltage, mass_amu)
    return np.sqrt((1.0 + beta) / (1.0 - beta))


@pytest.fixture
def frame() -> CLSDataFrame:
    return CLSDataFrame()


def test_dopplerfactor_zero_voltage_is_identity(frame):
    assert frame.dopplerfactor(voltage=0.0, mass=1.0) == pytest.approx(1.0)


@pytest.mark.xfail(
    reason=(
        "Known defect: at voltage=0.0, `m**2*self.c**4` and "
        "`(self.e*voltage+m*self.c**2)**2` are not always bit-identical, so the "
        "sqrt() argument can round to a tiny negative number and dopplerfactor "
        "returns nan instead of 1.0. Mass-dependent (e.g. reproduces for "
        "mass=87.0, not for mass=1.0 or 133.0)."
    ),
    strict=True,
)
def test_dopplerfactor_zero_voltage_known_nan_defect(frame):
    assert frame.dopplerfactor(voltage=0.0, mass=87.0) == pytest.approx(1.0)


@pytest.mark.parametrize("voltage", [10.0, 1000.0, 30000.0])
@pytest.mark.parametrize("mass", [1.0, 87.0, 133.0])
def test_dopplerfactor_matches_independent_relativistic_kinematics(frame, voltage, mass):
    # rel=1e-8, not machine epsilon: near beta=0 the two mathematically
    # equivalent formulas (this code's vs. the gamma/beta textbook form) have
    # different conditioning, so float noise at the ~1e-12 relative level is
    # expected and not itself evidence of a bug.
    expected = independent_doppler_factor(voltage, mass)
    assert frame.dopplerfactor(voltage=voltage, mass=mass) == pytest.approx(expected, rel=1e-8)


def test_dopplerfactor_classical_limit_at_low_voltage(frame):
    # eV << m c^2: beta should match the non-relativistic v = sqrt(2 e V / m),
    # and factor should match the first-order expansion 1 + beta.
    voltage = 10.0
    mass = 133.0
    m = mass * MU
    v_classical = np.sqrt(2 * E * voltage / m)
    beta_classical = v_classical / C
    factor = frame.dopplerfactor(voltage=voltage, mass=mass)
    assert factor == pytest.approx(1 + beta_classical, rel=1e-6)


def test_dopplerfactor_flag_symmetry(frame):
    voltage, mass = 5000.0, 87.0
    both_true = frame.dopplerfactor(voltage, mass, collinear=True, rest_to_lab=True)
    both_false = frame.dopplerfactor(voltage, mass, collinear=False, rest_to_lab=False)
    mixed_a = frame.dopplerfactor(voltage, mass, collinear=True, rest_to_lab=False)
    mixed_b = frame.dopplerfactor(voltage, mass, collinear=False, rest_to_lab=True)

    assert both_true == pytest.approx(both_false)
    assert mixed_a == pytest.approx(mixed_b)
    assert both_true == pytest.approx(1 / mixed_a)


def test_dopplerfactor_array_input(frame):
    # Voltage=0 excluded: see test_dopplerfactor_zero_voltage_known_nan_defect.
    voltages = np.array([100.0, 1000.0])
    factors = frame.dopplerfactor(voltage=voltages, mass=87.0)
    expected = np.array([independent_doppler_factor(v, 87.0) for v in voltages])
    np.testing.assert_allclose(factors, expected, rtol=1e-8)


def test_dopplershift_scales_frequency_by_the_doppler_factor(frame):
    frequency = 5.0e14
    voltage, mass = 8000.0, 133.0
    factor = frame.dopplerfactor(voltage, mass, collinear=False, rest_to_lab=False)
    shifted = frame.dopplershift(frequency, voltage, mass, collinear=False, rest_to_lab=False)
    assert shifted == pytest.approx(frequency * factor)


def test_dopplershift_round_trip_lab_to_rest_and_back(frame):
    frequency = 5.0e14
    voltage, mass = 8000.0, 133.0
    to_rest = frame.dopplershift(frequency, voltage, mass, collinear=False, rest_to_lab=False)
    back_to_lab = frame.dopplershift(to_rest, voltage, mass, collinear=False, rest_to_lab=True)
    assert back_to_lab == pytest.approx(frequency, rel=1e-12)
