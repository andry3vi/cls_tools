# clstools manual

Reference for every public class, method and function of `clstools`. Each entry gives what it
does, when to call it, its parameters, what it returns and stores, the warnings it raises, and
an example of its use. The examples continue the [typical session](#typical-session): `data` is
a `CLSDataFrame` with a run loaded and `cls` is `clstools`.
The physics behind the defaults comes from the Bi offline experiment (October 2026) and the CLS
systematics study (November 2026); the numbers quoted are from those runs.

- [Typical session](#typical-session)
- [CLSDataFrame](#clsdataframe): [loading](#loading), [scan calibration](#scan-voltage-calibration),
  [voltages and frequencies](#voltages-and-frequencies), [event filters](#event-filters),
  [counting checks](#pmt-counting-checks), [binning](#binning), [reporting](#reporting),
  [Doppler helpers](#doppler-helpers), [attributes](#attributes)
- [clstools.calibration](#clstoolscalibration)
- [clstools.counting](#clstoolscounting)
- [Warnings](#warnings)

## Typical session

```python
import clstools as cls

data = cls.CLSDataFrame()                      # VAccDiv=1000, VCoolDiv=10000, VCoolOffset=0
data.Load_Run("run_8244.asdf")                 # metadata, calibration, readback and counting checks
data.Use_Scan_Calibration()                    # optional: bench table + run offset and scale
data.apply_dead_time()                         # optional: 10 us per PMT, DC beams with afterpulsing
data.Compute_Voltages()                        # cooler: moving average over the ripple's alias periods
data.Compute_WL(Mass=208.980398599, ref=f_ref, harmonic=2)
data.Compute_Setpoint_Bins(TOF_gate=[..], PMT_gate=[3, 4])   # one point per set voltage
data.Info()                                    # everything above, with the checks
```

The methods must be called in this order: each one fills attributes the next one reads, and
there are no guards. `Load_Run` resets everything a previous run left behind.

## CLSDataFrame

### Loading

#### `CLSDataFrame(VAccDiv=1000, VCoolDiv=10000, VCoolOffset=0)`

Creates an empty frame.

| parameter | meaning |
|---|---|
| `VAccDiv` | divider ratio of the scan-voltage (LCR/CEC) monitor; the file stores HV / VAccDiv |
| `VCoolDiv` | divider ratio of the cooler-voltage monitor (`Vrfq` × VCoolDiv = cooler voltage in V) |
| `VCoolOffset` | added to the cooler voltage [V]; the effective acceleration voltage can differ from the reading by tens of volts (Yb+ I340: about −30 V, Bi offline about +13 V) |

```python
data = cls.CLSDataFrame()  # standard dividers
data = cls.CLSDataFrame(VCoolOffset=13.0)  # Bi offline: beam energy 13 V above the reading
```

#### `Load_Run(filename, cal_order=1, blocksize=25e6, filter_calibration=True, ignore_intercept=False, counting_check=True)`

Reads an ASDF run file, fits the in-run scan-voltage calibration, and checks the file.

| parameter | meaning |
|---|---|
| `filename` | path of `run_<number>.asdf` |
| `cal_order` | order (1–3) of the polynomial fitted to `CalReadback` vs `CalSet` |
| `blocksize` | Dask partition size of the event table |
| `filter_calibration` | `True`: drop calibration points beyond 2σ and refit. `'robust'`: correct a readback lag and drop unsettled readings first (see the readback check). `False`: fit all points |
| `ignore_intercept` | set the polynomial's intercept to 0 after the fit |
| `counting_check` | run the PMT counting check (about 1 s per million events) |

Stores the metadata (`run_number`, `Experiment`, `Date`, `Vcool_init`, `Laser_set`, `Step_Size`,
`ScanningRanges`, `Dwell_Time`), the calibration (`Cal`, `Cal_err`, `Cal_df`, `Cal_raw`,
`Dropped_calibration_points`), the events (`Run`, a Dask frame with columns
`TS, DV, Bunch, TDC, TOF, Vrfq`), `Readback_check`, `Trigger_period_us`, `Counting_check` and
`Counting_check_loaded`, `TSstart`, `TSstop`, `DAQTStime`, `Size`.

Checks:
- **Calibration readback** (`Readback_check`, always). Raises a `CalibrationWarning` for faults
  of the file: mismatched `CalSet`/`CalReadback` lengths, non-finite values, too few points, a
  readback lagging the set value by whole steps (7 % of 2024–2026 files), unsettled readings
  after jumps, and a readback scale off by more than 1 % (dead or corrupt readback). This
  changes no numbers unless `filter_calibration='robust'`.
- **PMT counting** (`Counting_check`, unless `counting_check=False`). Per PMT: Fano factor of the
  counts per trigger, afterpulse bursts, and the effect of a dead time. Raises a
  `CountingWarning` when a PMT's Fano factor is off. See
  [`Check_Counting`](#check_counting).

```python
data.Load_Run(path)  # defaults
data.Load_Run(path, filter_calibration="robust")  # lag-corrected polynomial
data.Load_Run(path, counting_check=False)  # skip the counting check
```

#### `Update_Cal(cal_order=1)`

Refits the polynomial calibration on the current `Cal_df` at another order. The outlier filter
and `ignore_intercept` are not applied again. Rerun `Compute_Voltages` afterwards.

```python
data.Update_Cal(cal_order=2)  # quadratic through the points kept by Load_Run
print(data.Cal, data.Cal_err)  # ascending: Cal[0] is the intercept; Cal_err are variances
data.Compute_Voltages()
data.Compute_WL(Mass=m, ref=f_ref)
```

#### `Update_V_divisions(VAccDiv=1000, VCoolDiv=10000, VcoolOffset=0)`

Sets the divider ratios and the cooler offset. Only the attributes change: rerun
`Compute_Voltages` and `Compute_WL` for the change to reach `V` and `F`.

```python
data.Update_V_divisions(VAccDiv=1000, VCoolDiv=10000, VcoolOffset=-30.0)
data.Compute_Voltages()
data.Compute_WL(Mass=m, ref=f_ref)
```

### Scan-voltage calibration

The scan HV (USB-231 DAC → Trek ×1000) has a reproducible nonlinearity of about 0.25 V rms
(up to ±0.4 V) that a straight line through the run's `CalSet`/`CalReadback` cannot follow. On
Bi 8244/8245 it moved Bl by 10.6 / 3.3 MHz. The bench table holds that shape. Each run then gets
its own offset (the 0 V level drifts by 0.1–0.5 V between runs) and scale.

#### `Use_Scan_Calibration(table='auto', model='lut', offset_v=None, gain='auto')`

Calibrates the scan voltage with a bench table instead of the polynomial:
`applied(DV) = divider_scale × (scale × shape(DV) + offset)`. Call it after `Load_Run`, before
`Compute_Voltages`.

| parameter | meaning |
|---|---|
| `table` | `'auto'`: the packaged table for the run's date. Otherwise a packaged id (`cls.available_tables()`), a path to a table file, or a `ScanCalibration` |
| `model` | `'lut'`: the table on its 1 V grid, its cubic off the grid. `'cubic'`: the cubic everywhere, without the nonlinearity (for comparisons) |
| `offset_v` | force the run offset [V]; the readback fit is still made and returned |
| `gain` | how the run scale is chosen, see below |

`gain`:

| value | scale |
|---|---|
| `'auto'` (default) | the CLS systematics rule. Fitted with the offset when the kept calibration points span ≥ 400 V (measured to 4–40 ppm). Narrower calibrations (errors of hundreds of ppm) take the campaign gain of the run's `Experiment` packaged with the table, and 1 for an unknown campaign |
| `'free'` | always fitted |
| `'campaign'` | the campaign gain (1 if unknown) |
| `'bench'` | 1: the bench scale. The in-run readback reads 50–80 ppm below the bench, which is 0.6–1.0 MHz on Yb-171 Al; which scale is right is not decided |
| a number | that scale, e.g. `0.99992` |

Returns the `ReadbackFit` (also in `Readback_fit`). Stores `Scan_calibration`, `Scan_model`,
`Scan_offset_v`, `Scan_scale`, `Scan_scale_err` and `Scan_scale_mode`, and installs the map used
by `Compute_Voltages` and `Frequency_ranges`.

Warnings (`CalibrationWarning`):
- The run is before the table's validity. The packaged table is valid from run 5487
  (4 Nov 2024), when the DAC chain changed. Use the polynomial or `model='cubic'` before that.
- The readback is noisy (> 0.3 V rms against the table), its gain is > 300 ppm from the table,
  or the offset is > 5 V from the bench.
- The campaign's gains scatter by more than 50 ppm (only IGI-V-Fe-Rh-Offline-CLS, Jan 2025).
- The readback is dead or corrupt. The bench offset is used then, uncertain by 0.1–0.5 V.
- Set voltages are off the table's grid or outside its range.

```python
fit = data.Use_Scan_Calibration()  # systematics rule
data.Use_Scan_Calibration(gain="bench")  # offset only, bench scale
data.Use_Scan_Calibration(model="cubic")  # without the DAC nonlinearity
print(data.Scan_offset_v, data.Scan_scale, data.Scan_scale_mode)
```

#### `Set_Scan_Voltage_Map(scan_map)`

Installs any function `DV [V] → applied scan voltage [V]` (numpy in, numpy out) in place of
the polynomial; `None` restores the polynomial. `Use_Scan_Calibration` uses this hook.

```python
data.Set_Scan_Voltage_Map(lambda dv: 1000.0 * (1.0002 * dv - 0.0004))  # own calibration [V]
data.Compute_Voltages()
data.Set_Scan_Voltage_Map(None)  # back to the polynomial
```

### Voltages and frequencies

#### `Compute_Voltages(cooler_correction='smooth', cooler_window_s='auto', cooler_max_dev_v=5.0)`

Adds `DV_cal` (applied scan voltage) and `V` (beam energy,
`cooler × VCoolDiv + VCoolOffset − DV_cal`) to `Run`, and refreshes `Sorted`.

| parameter | meaning |
|---|---|
| `cooler_correction` | how each event gets its cooler voltage, see below |
| `cooler_window_s` | averaging window of `'smooth'` [s], or `'auto'` |
| `cooler_max_dev_v` | readings further than this from the run median [V] are readback faults: `'smooth'` drops them, `'pbp'`/`'mean'` warn. `None` disables the check |

`cooler_correction`:

| value | per-event cooler voltage |
|---|---|
| `'smooth'` (default) | faulty readings dropped, centred moving average of the rest, interpolated to each event (column `Vrfq_smooth`) |
| `'pbp'` | the event's own reading. Adds the aliased mains ripple to every event: 0.3–0.4 V rms (2.5–3.3 MHz) since May 2026, 1–2 V rms before. The default before November 2026 |
| `'mean'` | the run's mean reading (ignores the slow drift, 0.1–0.3 V over a run) |

Why `'smooth'`: the DAQ reads the cooler every ~0.1 s with a short aperture. That aliases the
50 Hz ripple into a 1.7–2.9 s oscillation that is not the voltage the ions saw.
`cooler_window_s='auto'` averages over 4 alias periods of the run's own readings, and at least
15 s:
- usual readout (~0.1 s): the window is 15 s and leaves 11–55 mV of the alias;
- files read every 0.4–0.7 s: they alias into 8–18 s, so the window is 30–75 s.

Runs too short to measure the alias (< 120 s or < 300 readings) get 15 s.

Every mode measures the ripple (`Cooler_ripple`: alias period, fast rms, reading interval;
`Cooler_window_s` for `'smooth'`). The fast rms is the ripple the ions see. It is a line
broadening (× 9.04 MHz/V for Bi, 8.23 MHz/V for Yb+ at 30 kV) that no mode can correct, because
the readout is not synchronised with the bunches.

```python
data.Compute_Voltages()  # smooth, auto window
data.Compute_Voltages(cooler_window_s=20)
data.Compute_Voltages(cooler_correction="pbp")  # as before November 2026
```

#### `Compute_WL(Mass, ref=0, harmonic=2)`

Adds `WN` (rest-frame wavenumber, cm⁻¹) and `F` (Hz, minus `ref`) to `Run`, and refreshes
`Sorted`. The laser setpoint is multiplied by `harmonic`. Sets `Frequency_stepsize` (Hz per
voltage step at the initial cooler voltage).

| parameter | meaning |
|---|---|
| `Mass` | particle mass [u] (ions: atomic mass minus the electron) |
| `ref` | reference frequency [Hz] subtracted from `F` |
| `harmonic` | multiplier of `LaserSetpoint` |

```python
data.Compute_WL(Mass=208.980398599 - 5.48579909e-4, ref=0.0, harmonic=2)  # 209Bi+
print(data.Sorted[["V", "WN", "F"]].head(), data.Frequency_stepsize)
```

#### `Shift_Ref(ref=0)`

Recomputes `F` from `WN` with a new reference [Hz], without redoing the Doppler shift.

```python
data.Shift_Ref(ref=1.2345e15)  # F is now relative to 1.2345e15 Hz
data.Compute_Setpoint_Bins(TOF_gate=[67.0, 73.3])
```

#### `Frequency_ranges(Mass, ref=0, harmonic=2)`

Returns the scanned ranges converted to frequency, `[[f_lo, f_hi], …]` in MHz, using the
installed scan map if any.

```python
for f_lo, f_hi in data.Frequency_ranges(Mass=m, ref=f_ref):
    print(f"{f_lo:.1f} to {f_hi:.1f} MHz")
```

### Event filters

#### `apply_filter(filter_window=0)`

Drops all events of a timestamp (10 ms) holding more than `filter_window` events: noise bursts
on all channels. 0 does nothing. Reruns the counting check. Run 5028 went from Fano 475 to 1.06.

```python
data.Load_Run(path)
data.apply_filter(filter_window=5)  # drop timestamps with more than 5 events
data.Info()  # the PMT table now shows the filtered counts
```

#### `apply_dead_time(dead_time_us=10.0)`

Applies an artificial extending dead time per PMT. An event is dropped when the previous event
of the same PMT, kept or not, is less than the dead time earlier (event time =
`Bunch × Trigger_period_us + TOF`). It is never applied unless called. Call it after `Load_Run`
and before binning; it refreshes `Sorted` and reruns the counting check.

| parameter | meaning |
|---|---|
| `dead_time_us` | µs for every PMT, or `{channel: µs}`; unlisted channels keep all events |

Stores `Dead_time_us`, `Dead_time_removed` (fraction of each PMT's events removed) and
`Dead_time_pileup` (fraction of uncorrelated events removed, see
[`pileup_fraction`](#pileup_fractiondv-bunch-tdc-tof-dead_time_us10)).

When to use it:
- **DC beam with afterpulsing** (Bi offline): 10 µs removed 5.0–5.2 % of the events, Fano
  1.9–2.0 → 1.03, χ²_red 2.0–2.5 → 1.12–1.14. 5 and 20 µs give the same hyperfine constants.
- **Bunched beam** (Yb+ I340): the counts in the ToF gate are already Poisson, and the photons
  of a bunch come within a few µs. A dead time then removes 7–17 % of the real photons at the
  line peaks and distorts the line shape. A `CountingWarning` is raised when the pile-up loss is
  above 1 % for a PMT. Use none, or 1 µs.

Check the `Fano(10us dead)` and `pile-up(10us)` columns of `Info` before using it.

```python
data.apply_dead_time()  # 10 us on every PMT
data.apply_dead_time({4: 10.0})  # PMT 4 only
```

### PMT counting checks

#### `Check_Counting()`

Reruns the counting check on the current events (after any filter) and returns the
`CountingCheck`, also stored in `Counting_check`. `Load_Run`, `apply_filter` and
`apply_dead_time` call it. What it reports, per PMT:

| indicator | meaning | healthy |
|---|---|---|
| Fano | variance / mean of the counts per trigger, per set voltage and pooled; only the triggers strictly inside each visit (the edges bias it low) | 1 |
| bursts<5us (exp.) | fraction of events less than 5 µs after the previous event of the same PMT, and the uncorrelated expectation from the ToF profile | equal |
| Fano(10us dead) | the Fano factor after a 10 µs dead time | 1 |
| pile-up(10us) | fraction of uncorrelated events a 10 µs dead time removes | DC ~0.2 %, bunched several % |

On the summed counts, the **correlated excess**: how much more the sum fluctuates than the
single PMTs (beam or laser intensity, noise bursts). Healthy is 0. `BunchesPerChannel` 1 runs
get no Fano factor.

A `CountingWarning` is raised when |Fano − 1| > 0.1 and more than 5σ on a PMT with at least
1000 events. The message names the likely cause: afterpulse bursts, whether a dead time cures it
and how many real photons that would cost, counting losses, or correlated fluctuations
(`apply_filter`).

```python
check = data.Check_Counting()
for ch, pmt in check.pmts.items():
    print(ch, pmt.fano, pmt.fano_dead, pmt.pileup_dead)
print(check.kinds)  # e.g. {'super_poisson'}
```

### Binning

#### `Compute_Setpoint_Bins(TOF_gate=None, V_gate=None, F_gate=None, PMT_gate=None, min_visits=0.75)`

One point per scan set voltage: the counts of its gated events, at their mean frequency. This
is the recommended spectrum. Frequency bins of one step (`Compute_Bins`) straddle set voltages
once these are unevenly spaced (scan table, cooler drift). They also cover the ends of the
ranges only partly. On Bi 8244/8245, one point per set voltage lowered χ²_red from 3.4/4.2 to
2.5/2.0, moved Bl by 2.6 MHz, and removed 0.15–1 MHz jumps in cooler-offset scans. Needs
`Compute_WL`.

| parameter | meaning |
|---|---|
| `TOF_gate` | `[min, max]` ToF [µs] of the counted events (strictly inside) |
| `V_gate` | `[min, max]` set voltage `DV` [V] of the set voltages kept |
| `F_gate` | `[min, max]` mean frequency [Hz] of the set voltages kept |
| `PMT_gate` | PMT channels counted, e.g. `[3, 4]` |
| `min_visits` | set voltages scanned fewer times than this × the median are dropped and listed in `Setpoint_dropped` (default 0.75; 0 keeps all) |

Returns and stores `Setpoint_binned`, sorted by frequency, with these columns:

| column | content |
|---|---|
| `DV` | set voltage [V] |
| `Fcount` | counts of the gated events |
| `Fcount_err` | √counts, 1 for 0 |
| `Fmean`, `Fstd` | mean and standard deviation of the gated events' `F` [Hz]. A set voltage with no gated events gets the mean `F` of all its events |
| `Vmean`, `Vstd` | the same for `V` [V] |
| `visits` | dwells of the scan on the set voltage, with or without events (its exposure, in steps) |

The exposure comes from the reconstructed scan ([`scan_dwells`](#clstoolsscan), stored in
`Scan_dwells`), not from the events. The DAQ dwells twice in a row on the set voltages where an
up-down scan turns, so the sweep end points have the same exposure as the others (Bi 8244: 480
and 428 counts at 530 and −130 V, median 507). Dwells without events are counted too, which
matters below about 2 events per dwell (online runs). Counting visits from the events instead
dropped set voltages in 647 of 758 archive runs: the end points in all up-down runs, and up to
374 of 873 set voltages in a sparse Yb+ run (7549). The cut now drops set voltages only where a
short run ends in a partial cycle (31 of the 758 runs, all shorter than 3 cycles).

When the ranges list a range twice, its set voltages get twice the exposure; `visits` shows
it, and `Fcount / visits` gives comparable rates.

```python
sp = data.Compute_Setpoint_Bins(TOF_gate=[67.0, 73.3], PMT_gate=[3, 4])
x, y, dy = sp["Fmean"] / 1e6, sp["Fcount"], sp["Fcount_err"]
```

#### `Compute_Bins(TOF_gate=None, V_gate=None, F_gate=None, PMT_gate=None, bins=None)`

Frequency histogram of the gated events in `bins` equal bins (default: span / `Frequency_stepsize`),
stored in `Binned` with columns `bins`, `Fcount`, `Fmean`, `Fstd`, `Vmean`, `Vstd`,
`bins_center`, `bins_width`. `V_gate` applies to `DV`. Kept for compatibility; prefer
`Compute_Setpoint_Bins`.

```python
data.Compute_Bins(TOF_gate=[67.0, 73.3], PMT_gate=[3, 4])  # one frequency step per bin
data.Compute_Bins(TOF_gate=[67.0, 73.3], bins=200)  # 200 bins over the span
b = data.Binned
x, y = b["Fmean"] / 1e6, b["Fcount"]
```

#### `Compute_ToF(V_gate=None, PMT_gate=None)`

Counts per ToF value of the gated events, in `ToF_binned` (index `TOF`, column `counts`). Use
it to choose the ToF gate.

```python
data.Compute_ToF(PMT_gate=[3, 4])
data.ToF_binned["counts"].plot()  # pick TOF_gate around the bunch
```

#### `Compute_Raw_Bins(TOF_gate=None, V_gate=None, PMT_gate=None)`

Counts per set voltage `DV`, in `Raw_binned`. It needs no voltage or frequency computation.

```python
data.Load_Run(path)
data.Compute_Raw_Bins(TOF_gate=[67.0, 73.3], PMT_gate=[3, 4])  # a quick look, right after loading
print(data.Raw_binned)
```

### Reporting

#### `Info()`

Call it after `Compute_WL` (before that it stops at the frequency step size). Prints the run,
the calibration and its checks, the scan table with the run offset and scale,
the cooler (correction, window, faulty readings, alias period, ripple), the PMT counting table
and the dead time, the scanned ranges and setpoint bins, and the timings.
Example (Bi 8244):

```text
---------------Scan calibration table----------------
     Table / model           ->  scan_hv_2026-10-24 / lut
     Measured on             ->  2026-10-24
     Valid from              ->  2024-11-04 (run 5487)
     Run offset          [V] ->  -0.5137 (readback fit -0.5137 +- 0.0062)
     Run scale         [ppm] ->  -44 +- 19 (free; calibration span 660 V)
     Readback rms        [V] ->  0.0768
     Readback gain     [ppm] ->  -44 +- 19
     Readback lag    [steps] ->  0
     Rejected readings   [V] ->  []
-----------------------Cooler------------------------
     Cooler correction       ->  smooth
     Averaging window    [s] ->  15.0
     Faulty cooler readings  ->  0
     Reading interval    [s] ->  0.106
     Alias period        [s] ->  2.30
     Ripple rms          [V] ->  0.341 (line broadening, not corrected)
--------------------PMT counting---------------------
     Trigger period          [us] ->  1000
     PMT   events  rate[cps]   Fano          bursts<5us (exp.)  Fano(10us dead)  pile-up(10us)
       3    78566      131.9    1.23 +- 0.01         2.1 % (0.1)    1.02             0.2 %
       4   103495      173.7    2.63 +- 0.00         6.9 % (0.1)    1.04             0.3 %
     sum                          2.02 +- 0.00   correlated excess -0.00
     Counting check:  PMT 4 counts are not Poisson: Fano factor 2.63 +- 0.00; 6.9 % of its
                      events come less than 5 us after the previous one (0.1 % expected):
                      afterpulse bursts; a 10 us dead time gives 1.04 (apply_dead_time)
-------------------Scanning Ranges-------------------
     ...
     Setpoint bins               ->  293
     Scan pattern                ->  updown, 35.00 cycles, 29 triggers per step
```

After `apply_dead_time()` (5.2 % of the events removed) the Fano factors are 1.02–1.04.

```python
data.Info()  # after Compute_WL; optional steps not run are left out
```

#### `Info_to_csv(filename)`

Writes the same information as one CSV row: metadata, calibration coefficients, scan table
(`Scan_offset_v`, `Scan_scale`, `Scan_scale_mode`, readback rms, gain, lag), cooler
(`Cooler_window_s`, `Cooler_alias_period_s`, `Cooler_ripple_rms_v`, faulty readings), setpoint
bins, and counting (`Fano_pmt<c>`, `Burst_fraction_pmt<c>`, `Fano_dead_pmt<c>`,
`Pileup_dead_pmt<c>`, `Dead_time_us`).

```python
for path in run_files:
    data.Load_Run(path)
    data.Use_Scan_Calibration()
    data.Compute_Voltages()
    data.Info_to_csv("runs.csv")  # one row per run
```

### Doppler helpers

#### `dopplerfactor(voltage, mass, collinear=True, rest_to_lab=True)`

Relativistic Doppler factor `(1 + β) / √(1 − β²)` for a particle of `mass` [u] accelerated
through `voltage` [V], or its inverse when exactly one of `collinear`, `rest_to_lab` is False.
The pipeline uses `collinear=False, rest_to_lab=False`. It resolves F only in ~300 Hz steps at
30 kV (cancellation in β).

#### `dopplershift(frequency, voltage, mass, collinear=True, rest_to_lab=True)`

`frequency × dopplerfactor(...)`.

```python
# laser frequency seen by 209Bi+ at 30 kV, anticollinear: as in Compute_WL
f_rest = data.dopplershift(2 * data.Laser_set, 30000.0, m, collinear=False, rest_to_lab=False)
```

### Attributes

| attribute | set by | content |
|---|---|---|
| `Run` | `Load_Run`, filters | Dask event table (lazy) |
| `Sorted` | `Compute_Voltages`, `Compute_WL`, `Shift_Ref`, `apply_dead_time` | pandas snapshot of `Run` |
| `Cal`, `Cal_err`, `Cal_df`, `Cal_raw` | `Load_Run` | polynomial (ascending, `Cal[0]` intercept), variances, points used, points as recorded |
| `Readback_check` | `Load_Run` | `ReadbackFit` of the readback check |
| `Scan_calibration`, `Scan_model`, `Scan_offset_v`, `Scan_scale`, `Scan_scale_err`, `Scan_scale_mode`, `Readback_fit` | `Use_Scan_Calibration` | the table and the run's offset and scale |
| `Cooler_correction`, `Cooler_window_s`, `Cooler_ripple`, `Cooler_faulty_readings` | `Compute_Voltages` | cooler handling and diagnostics |
| `Trigger_period_us` | `Load_Run` | trigger period estimated from the timestamps |
| `Counting_check_loaded`, `Counting_check` | `Load_Run`, `Check_Counting` | counting check as loaded / after the last filter |
| `Dead_time_us`, `Dead_time_removed`, `Dead_time_pileup` | `apply_dead_time` | dead time and its effect per PMT |
| `Setpoint_binned`, `Setpoint_dropped`, `Scan_dwells` | `Compute_Setpoint_Bins` | spectrum per set voltage, dropped set voltages, reconstructed scan |
| `Binned`, `ToF_binned`, `Raw_binned` | `Compute_Bins`, `Compute_ToF`, `Compute_Raw_Bins` | histograms |

## clstools.calibration

### Tables

#### `available_tables()`

Ids of the packaged scan tables, oldest first (`['scan_hv_2026-10-24']`).

#### `load_table(name)`

A packaged table by id, or a table file by path (`.json`). Raises `ValueError` if neither.

#### `select_table(run_date, run_number=None)`

The packaged table for a run (`run_date` a `datetime.date`): the latest measured on or before
`run_date`; otherwise the earliest that describes the run; otherwise the oldest.

```python
import datetime

from clstools.calibration import select_table

print(cls.available_tables())
table = cls.load_table("scan_hv_2026-10-24")
run_date = datetime.date.fromtimestamp(data.TSstart)
table = select_table(run_date, int(data.run_number))  # what table='auto' picks
data.Use_Scan_Calibration(table=table)
```

#### `ScanCalibration`

A bench table (file format v1 in the module docstring). Fields: `id`, `measured`, `valid_from`,
`valid_from_run`, `electrode`, `set_v`, `shape_v`, `cubic`, `bench_offset_v`, `divider_scale`,
`repeat_rms_v`, `gain_spread_ppm`, `campaign_gains` (`{Experiment: CampaignGain}`),
`description`, `provenance`.

| method | does |
|---|---|
| `ScanCalibration.load(path)` / `from_dict(d)` | read and validate a table |
| `describes(run_date, run_number=None)` | whether the table applies to a run of that `datetime.date` (from `valid_from`; on that day by run number) |
| `covers(dv)` / `on_grid(dv)` | set voltages inside the table / on its grid |
| `shape(dv, model='lut')` | offset-free applied voltage [V] |
| `applied(dv, offset_v, model='lut', scale=1.0)` | applied voltage [V] |
| `scan_map(offset_v, model='lut', scale=1.0)` | function for `Set_Scan_Voltage_Map` |
| `fit_run_offset(cal_set, readback_v, model='lut')` | readback fit at scale 1: lag, unsettled readings, noise, gain and offset checks |
| `fit_run(cal_set, readback_v, model='lut', gain='auto', campaign=None)` | offset and scale by the rule of `Use_Scan_Calibration`; returns a `ReadbackFit` with `scale`, `scale_err`, `scale_mode` |

`readback_v` is at the HV side: `CalReadback × VAccDiv`.

```python
table = cls.load_table("scan_hv_2026-10-24")
print(table.describes(datetime.date(2025, 3, 10), 6100), table.covers(data.Cal_raw["Set"]).all())
fit = table.fit_run(
    cal_set, readback_v=cal_readback * 1000.0, gain="auto", campaign=data.Experiment
)
applied = table.applied(dv, fit.offset_v, scale=fit.scale)  # applied scan voltage [V]
```

#### `CampaignGain`

`gain`, `gain_sigma` (1.4826 × MAD), `n_wide` (calibrations ≥ 400 V it comes from),
`unstable`.

```python
g = cls.load_table("scan_hv_2026-10-24").campaign_gains.get(data.Experiment)
if g is not None:
    print(f"campaign scale {g.gain:.6f} +- {g.gain_sigma:.6f} from {g.n_wide} calibrations")
```

### Readback

#### `analyse_readback(cal_set, readback_v, reference=None, order=1, model=None)`

Checks and fits a calibration readback. It tries lags of 1–3 steps and takes one only when it
lowers the residual rms below 0.7×. It rejects unsettled readings (beyond max(0.5 V, 5 robust
σ)) and flags noise, gain and readback-scale problems. With `reference` (a table shape) it fits
`readback = offset + table`; otherwise a polynomial of `order`.

```python
fit = cls.analyse_readback(cal_set, cal_readback * 1000.0)  # line, lag search, rejections
print(fit.lag_steps, fit.residual_rms_v, fit.rejected_set_v, fit.kinds)
```

#### `ReadbackFit`

| field / property | content |
|---|---|
| `offset_v`, `offset_err_v` | run offset [V] (at `scale`) |
| `gain_ppm`, `gain_err_ppm` | readback gain against the table (or the nominal 1) |
| `scale`, `scale_err`, `scale_mode` | run scale applied with the table and how it was chosen |
| `residual_rms_v`, `n_points`, `lag_steps` | fit quality, points used, lag found |
| `set_v`, `readback_v`, `keep` | lag-shifted points and the ones used |
| `issues`, `kinds` | `ReadbackIssue`s and their kinds |
| `usable` | False for a dead or corrupt readback (no offset from it) |
| `span_v` | span of the set voltages used [V] |
| `rejected_set_v` | set voltages of the rejected readings |

```python
fit = data.Readback_check  # from Load_Run
if not fit.usable:
    print("dead or corrupt readback:", [i.message for i in fit.issues])
```

### Cooler

#### `cooler_readings(ts, vrfq)`

The distinct cooler readings and when each first appears, from the event stream (the DAQ
latches the latest reading onto every event). Inputs in time order.

#### `faulty_readings(v, max_dev)`

Boolean mask of readings further than `max_dev` from their median.

#### `cooler_ripple(t, v)` → `CoolerRipple`

Alias period, fast rms and reading interval of fault-free readings (`t` [s], `v` [V]). The
readings are put on a 0.1 s grid and a 30 s running mean is subtracted; the alias period is the
strongest line between 0.05 and 4.9 Hz. `CoolerRipple.window_s()` gives the `'auto'` window.

#### `smooth_readings(t, v, window_s)`

Centred moving average over `window_s` seconds.

```python
from clstools.calibration import cooler_readings, faulty_readings, smooth_readings

ev = data.Run[["TS", "Vrfq"]].compute().sort_values("TS")
t, v = cooler_readings(ev["TS"].to_numpy(), ev["Vrfq"].to_numpy() * data.VCoolDiv)
ok = ~faulty_readings(v, 5.0)
rip = cls.cooler_ripple(t[ok], v[ok])
print(rip.alias_period_s, rip.fast_rms_v, rip.window_s())
v_smooth = smooth_readings(t[ok], v[ok], rip.window_s())  # what 'smooth' uses
```

## clstools.scan

How often the scan stood on each set voltage. The DAQ steps through the `ScanningRanges` in
their listed order, each from its first to its second value, and then either runs the same
sequence backwards, dwelling twice in a row on the turning points (`'updown'`, most runs since
November 2024), or starts again (`'sawtooth'`, most runs before, and some later ones). Every
dwell takes `BunchesPerChannel` triggers plus a settling time. Checked on 758 archive runs: on
the runs with one trigger per step this matches every event's set voltage to its trigger
number exactly.

#### `scan_dwells(dv, bunch, ranges=None, step=None, pattern='auto')` → `ScanDwells`

Reconstructs the scan from the events (`dv`, `bunch` in time order) and counts the dwells of
every set voltage between the first and the last event. Each visit (a run of events with one
`DV`) is placed in the cycle at the position closest to what the triggers since the previous
visit predict (median triggers per step, as the trigger counter also counts settling
triggers). `pattern='auto'` takes the pattern whose positions follow the triggers best. Without
`ranges`/`step`, or when events lie outside them, the sequence is the observed set voltages in
ascending order. About 0.1–0.3 s for a run.

`ScanDwells` holds `dwells` and `per_cycle` (`{DV: n}`), `pattern`, `sequence_from`
(`'ranges'` or `'events'`), `triggers_per_dwell`, `cycles`, and `residual` (mean distance in
steps between the placed and the predicted positions: 0 for one trigger per step, 0.1–0.5
usually, larger only for runs with < 0.2 events per step). `sweeps()` gives dwells / dwells
per cycle, which is equal for all set voltages of a run up to the partial cycles at its ends.

#### `scan_sequence(ranges, step)`

The set voltages of one sweep: the ranges in listed order, each from its first to its second
value.

```python
ev = data.Run[["DV", "Bunch", "TOF"]].compute().sort_values(["Bunch", "TOF"])
sd = cls.scan_dwells(ev["DV"], ev["Bunch"], data.ScanningRanges, data.Step_Size)
print(sd.pattern, sd.cycles, sd.triggers_per_dwell, sd.residual)
print(sd.dwells[-130.0], sd.sweeps()[-130.0])
print(cls.scan_sequence([[5.0, 7.0], [-2.0, -4.0]], 1.0))  # [ 5.  6.  7. -2. -3. -4.]
```

## clstools.counting

The functions behind `Check_Counting` and `apply_dead_time`, on plain numpy arrays of the events
(for example from `data.Run[["DV", "Bunch", "TDC", "TOF", "TS"]].compute()`). `check_counting`
and `pileup_fraction` are exported by `clstools`; the others are imported from
`clstools.counting`.

```python
from clstools.counting import dead_time_keep, trigger_period_us

ev = data.Run[["TS", "DV", "Bunch", "TDC", "TOF"]].compute()
a = {c: ev[c].to_numpy() for c in ev}
period = trigger_period_us(a["TS"], a["Bunch"], a["TOF"])
check = cls.check_counting(a["DV"], a["Bunch"], a["TDC"], a["TOF"], period)
loss = cls.pileup_fraction(a["DV"], a["Bunch"], a["TDC"], a["TOF"], dead_time_us=1.0)
keep = dead_time_keep(a["Bunch"], a["TDC"], a["TOF"], period, {4: 10.0})
```

#### `check_counting(dv, bunch, tdc, tof, period_us, dead_time_us=10.0)`

The counting check on raw event arrays in any order (see `Check_Counting`). `dead_time_us` is
the dead time of the `fano_dead` and `pileup_dead` indicators. Returns a `CountingCheck`.

#### `trigger_period_us(ts, bunch, tof)`

Trigger period [µs]: timestamp span / trigger span, to 3 significant digits. Falls back to
max(1000, max ToF) for runs shorter than 10 s.

#### `event_time_us(bunch, tof, period_us)`

`bunch × period_us + tof`.

#### `dead_time_keep(bunch, tdc, tof, period_us, dead_time_us)`

Mask of the events an extending dead time per PMT keeps (`dead_time_us`: number or
`{channel: µs}`).

#### `pileup_fraction(dv, bunch, tdc, tof, dead_time_us=10)`

Per PMT, the fraction of uncorrelated (real) events a dead time removes. The previous trigger of
the same set-voltage visit has the same rate and ToF profile but no afterpulse correlation. So
the fraction of events with a same-PMT event less than the dead time earlier in ToF *in the
previous trigger* is the pile-up loss. Bi 8244/8245: 0.15–0.27 % at 10 µs; Yb+ I340: 1.0–4.5 %.

#### `CountingCheck`, `PMTCounting`, `CountingIssue`

These are the results.
- `CountingCheck` holds `period_us`, `n_triggers`, `n_fano_triggers`, `pmts` (`{channel:
  PMTCounting}`), `fano_all`, `fano_all_err`, `correlated_excess`, `dead_time_us`,
  `issues` and `kinds`.
- `PMTCounting` holds `channel`, `events`, `rate_cps`, `fano`, `fano_err`, `burst_fraction`,
  `burst_expected`, `burst_excess`, `fano_dead` and `pileup_dead`.

## Warnings

| class | raised by | turn into errors |
|---|---|---|
| `CalibrationWarning` | readback check, `Use_Scan_Calibration`, cooler faults | `warnings.simplefilter("error", cls.CalibrationWarning)` |
| `CountingWarning` | counting check, `apply_dead_time` on bunched beams | `warnings.simplefilter("error", cls.CountingWarning)` |

Warnings never change numbers, except where the text says so: dropped faulty cooler readings
with `'smooth'`, and the bench offset for a dead readback.
