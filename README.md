# CLS_tools

A lightweight utility package for loading and processing IGISOL collinear laser spectroscopy (CLS) run files.

This project provides a single main class, `CLSDataFrame`, which uses Dask DataFrames for initial sorting and
Pandas/Numpy for subsequent processing. The loader supports ASDF run files 

## Features

- Load ASDF run files and extract run metadata
- Compute calibrated voltages from divider calibrations
- Convert cooler/scan voltages into particle-frame frequencies using relativistic Doppler formulas
- Bin data in frequency or time-of-flight (ToF), with optional gating by PMT / voltage / ToF ranges

## Installation

This project uses a `pyproject.toml`-based build. From the project root directory run:

```powershell
pip install .
```
Or build a wheel and install it:

```powershell
# optional: create a wheel
python -m build
pip install dist\clstools-0.5.0-py3-none-any.whl
```
If you use `uv` you can run the example directly without creating a venv or installing:

```powershell
uv run python .\example.py
```

## Requirements

The core runtime dependencies (declared in `pyproject.toml`) are:

- pandas
- dask[complete]
- asdf
- numpy

## Quick usage

A runnable example is provided in `example.py` which demonstrates a typical analysis flow and plotting.
Every method and function is described in the manual, [`docs/MANUAL.md`](docs/MANUAL.md).

```python
import clstools as cls

data = cls.CLSDataFrame()
data.Load_Run(path)  # metadata, calibration, readback and PMT counting checks
data.Use_Scan_Calibration()  # bench table + this run's offset and scale (optional)
data.Compute_Voltages()  # cooler: moving average over the ripple's alias periods
data.Compute_WL(Mass=mass, ref=reference, harmonic=2)
data.Compute_Setpoint_Bins(TOF_gate=[54, 64], PMT_gate=[3, 4])  # one point per set voltage
data.Info()
```

## Scan voltage calibration

By default the scan voltage is calibrated with a polynomial fitted to each run's own
`CalSet`/`CalReadback`. That line cannot follow the DAC's nonlinearity (~0.25 V rms, reproducible to a
few mV), which moved Bl of 209Bi by 10.6 / 3.3 MHz in the two Bi runs of October 2026.
`Use_Scan_Calibration` applies the bench-measured table instead and fits only the run's offset and
scale to its readback:

    applied(DV) = scale * table(DV) + offset

- **Scale** (`gain=`): `'auto'` (default) follows the CLS systematics rule: fitted with the offset when
  the calibration spans at least 400 V, otherwise the campaign's gain (median of its wide calibrations,
  packaged with the table per `Experiment`), and 1 for unknown campaigns. `'free'`, `'campaign'`,
  `'bench'` (scale 1) or a number choose otherwise. The in-run readback reads 50–80 ppm below the bench,
  and which scale is right is still open.
- **Validity**: the packaged table describes the scan HV from run 5487 (4 Nov 2024) on. Earlier runs
  get a warning that the table does not describe them.
- **Dead or corrupt readback**: the run offset cannot be fitted; the table's bench offset is used, with
  a warning.
- `cls.available_tables()` lists the packaged tables (`clstools/data/*.json`, format documented in
  `clstools/calibration.py`). `model="cubic"` drops the nonlinearity (for comparisons), and
  `offset_v=` overrides the fitted offset.

Every `Load_Run` also checks the calibration readback and raises a `CalibrationWarning` for faults in the
file: mismatched `CalSet`/`CalReadback` lengths, non-finite values, too few points, a readback lagging
the set value by whole steps (7 % of the 2024–2026 files), unsettled readings after jumps, and a readback
scale off by more than 1 %. This changes no numbers; `filter_calibration="robust"` corrects the lag and
drops the unsettled readings before the polynomial fit. To turn the warnings into errors:
`warnings.simplefilter("error", cls.CalibrationWarning)`.

## Cooler voltage

`Compute_Voltages()` gives each event a moving average of the cooler readings by default
(`cooler_correction="smooth"`). The DAQ reads the cooler every ~0.1 s with a short aperture. That
aliases the 50 Hz ripple into a 1.7–2.9 s oscillation, which the per-event readings used before
(`"pbp"`) added to every event: 0.3–0.4 V rms, or 2.5–3.3 MHz, since May 2026, and 1–2 V rms before.

- The default window (`cooler_window_s="auto"`) is 4 alias periods of the run's own readings, and at
  least 15 s.
- Readings more than 5 V from the median are dropped.
- `"pbp"` and `"mean"` remain available.

`Info()` shows the alias period and the ripple's rms. The ripple is a real, run-dependent line
broadening that no correction can remove.

## Binning

`Compute_Setpoint_Bins` gives one point per scan set voltage: the counts of its gated events at
their mean frequency. The exposure of each set voltage comes from the reconstructed scan
(`scan_dwells`): the end points of up-down sweeps are dwelt on twice at each turn and are kept,
and dwells without events (sparse online runs) are counted; only set voltages left short by a
partial cycle at the end of a short run are dropped. `Compute_Bins` instead makes frequency bins of one step, which straddle set voltages
once the scan voltages are unevenly spaced and only partly cover the ends of the ranges. In the
Bi runs, binning by set voltage lowered χ²_red from 3.4/4.2 to 2.5/2.0 and moved Bl by 2.6 MHz.

## PMT counting statistics

`Load_Run` also checks that each PMT counts like a Poisson process. A healthy PMT has a Fano factor of
1 for its counts per trigger (variance / mean, taken per scan step and pooled). In the Bi offline runs
(October 2026) it was 1.2–2.6: the PMTs fire bursts of afterpulses a few µs apart, and PMT 4 was several
times worse than PMT 3. The extra scatter doubled the fits' χ²_red. When a Fano factor is off,
`Load_Run` raises a `CountingWarning` that names the PMT and the likely cause, and `Info()` prints the
indicators:

```text
--------------------PMT counting---------------------
     Trigger period          [us] ->  1000
     PMT   events  rate[cps]   Fano          bursts<5us (exp.)  Fano(10us dead)  pile-up(10us)
       3    78566      131.9    1.23 +- 0.01         2.1 % (0.1)    1.02             0.2 %
       4   103495      173.7    2.63 +- 0.00         6.9 % (0.1)    1.04             0.3 %
     sum                          2.02 +- 0.00   correlated excess -0.00
```

- **bursts**: the fraction of a PMT's events that come less than 5 µs after its previous event,
  against what uncorrelated counting with the run's ToF profile gives.
- **Fano(10us dead)**: the Fano factor that `apply_dead_time` would give.
- **pile-up(10us)**: the fraction of uncorrelated (real) events that dead time would remove: ~0.2 %
  for a DC beam, 1–5 % for a bunched one, and 7–17 % at the line peaks.
- **correlated excess**: how much more the summed counts fluctuate than the single PMTs. Beam or
  laser intensity changes and noise bursts on all channels show up here; `apply_filter` removes the
  noise bursts.

The artificial dead time is opt-in:

```python
data.Load_Run(path)
data.apply_dead_time()  # 10 us per PMT, or {4: 10.0} for one PMT only
data.Compute_Voltages()
```

An event is dropped when the previous event of the same PMT is closer than the dead time. In the Bi
runs 10 µs removed 5.0–5.2 % of the events and brought the Fano factor to 1.02–1.04. 5 and 20 µs give
the same hyperfine constants.

With a bunched beam the photons of a bunch arrive within a few µs, so a dead time removes real photons
and distorts the line shape. `apply_dead_time` warns when the pile-up loss is above 1 %; for bunched
beams use none, or 1 µs. Runs with one trigger per scan step (`BunchesPerChannel` 1) get no Fano
factor. `Load_Run(..., counting_check=False)` skips the check, which takes about 1 s per million events;
`Check_Counting()` runs it at any time.

## License

GPL-3.0 — see `LICENSE`.


