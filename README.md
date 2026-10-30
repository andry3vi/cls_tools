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

## Scan voltage calibration

By default the scan voltage is calibrated with a polynomial fitted to each run's own
`CalSet`/`CalReadback`. That line cannot follow the DAC's nonlinearity (~0.25 V rms, reproducible to a
few mV), which shifted hyperfine results by over 100 kHz in the Bi offline experiment (October 2026).
`Use_Scan_Calibration` applies a bench-measured table instead and fits only the run's offset to its
readback:

```python
data = cls.CLSDataFrame()
data.Load_Run(path)
fit = data.Use_Scan_Calibration()  # packaged table, offset from this run's readback
data.Compute_Voltages(cooler_correction="smooth")
data.Compute_WL(Mass=mass, ref=reference, harmonic=2)
data.Info()  # table, run offset, readback rms / lag / rejected points
```

- `cls.available_tables()` lists the packaged tables (`clstools/data/*.json`); `table=` picks one by id
  or path. The file format is documented in `clstools/calibration.py`. Runs taken before a table was
  measured are calibrated with it, with a `CalibrationWarning`.
- `model="cubic"` drops the nonlinearity (for comparisons); `offset_v=` overrides the fitted offset when
  a run's readback is known to be wrong.
- `cooler_correction="smooth"` drops faulty cooler readings (> 5 V from the run median), averages the
  rest over a centred 5 s window, and interpolates to each event. The DAQ's short-aperture readings alias
  the 50 Hz ripple into a ~2.4 s oscillation the events cannot follow.

Every `Load_Run` also checks the calibration readback and raises a `CalibrationWarning` for faults in the
file: mismatched `CalSet`/`CalReadback` lengths, non-finite values, too few points (no longer a crash), a
readback lagging the set value by whole steps, unsettled readings after jumps, and a readback scale off by
more than 1 %. This changes no numbers; `filter_calibration="robust"` corrects the lag and drops the
unsettled readings before the polynomial fit. To turn the warnings into errors:
`warnings.simplefilter("error", cls.CalibrationWarning)`.

## PMT counting statistics

`Load_Run` also checks that each PMT counts like a Poisson process. A healthy PMT has a Fano factor of
1 for its counts per trigger (variance / mean, taken per scan step and pooled). In the Bi offline runs
(October 2026) it was 1.2–3: the PMTs fire bursts of afterpulses a few µs apart, and PMT 4 was several
times worse than PMT 3. The extra scatter doubled the fits' χ²_red. When a Fano factor is off,
`Load_Run` raises a `CountingWarning` that names the PMT and the likely cause, and `Info()` prints the
indicators:

```text
--------------------PMT counting---------------------
     Trigger period          [us] ->  1000
     PMT   events  rate[cps]   Fano          bursts<5us (exp.)  Fano(5us dead)
       3    69117      154.4    1.23 +- 0.01         1.8 % (0.1)    1.03
       4    60043      134.1    2.49 +- 0.01         7.7 % (0.1)    1.07
     sum                          1.82 +- 0.00   correlated excess -0.00
```

- **bursts**: the fraction of a PMT's events that come less than 5 µs after its previous event,
  against what uncorrelated counting with the run's ToF profile gives.
- **Fano(5us dead)**: the Fano factor that `apply_dead_time` would give.
- **correlated excess**: how much more the summed counts fluctuate than the single PMTs. Beam or
  laser intensity changes and noise bursts on all channels show up here; `apply_filter` removes the
  noise bursts.

The artificial dead time is opt-in:

```python
data.Load_Run(path)
data.apply_dead_time(5.0)  # or {4: 5.0} for one PMT only
data.Compute_Voltages(cooler_correction="smooth")
```

An event is dropped when the previous event of the same PMT is closer than the dead time. In the Bi
runs 5 µs removed 2–5 % of the events and brought the Fano factor to about 1. With bunched beams, real
photons also arrive close together, so check the Fano(5us dead) column before using it. Runs with one
trigger per scan step (`BunchesPerChannel` 1) get no Fano factor. `Load_Run(..., counting_check=False)`
skips the check, which takes about 1 s per million events; `Check_Counting()` runs it at any time.

## License

GPL-3.0 — see `LICENSE`.


