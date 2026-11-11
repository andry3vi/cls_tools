import datetime
import time
from collections.abc import Callable

import asdf
import dask.dataframe as dd
import numpy as np
import pandas as pd

from .calibration import (
    COOLER_MAX_DEV_V,
    DATAFILE_ISSUES,
    SCAN_MODELS,
    CoolerRipple,
    ReadbackFit,
    ScanCalibration,
    analyse_readback,
    cooler_readings,
    cooler_ripple,
    faulty_readings,
    load_table,
    select_table,
    smooth_readings,
    warn,
)
from .counting import (
    BURST_WINDOW_US,
    DEAD_TIME_US,
    MIN_EVENTS,
    PILEUP_WARN,
    CountingCheck,
    check_counting,
    dead_time_keep,
    pileup_fraction,
    trigger_period_us,
)
from .counting import warn as warn_counting


class CLSDataFrame:
    e = 1.602176634e-19  # C
    c = 299792458  # m/s
    u = 931.4941024  # MeV/C^2
    mu = 1.66053904 * 10 ** (-27)  # kg

    WN_to_f = 1e2 * c

    def dopplerfactor(
        self,
        voltage: float | np.ndarray,
        mass: float,
        collinear: bool = True,
        rest_to_lab: bool = True,
    ) -> float | np.ndarray:
        """
        Calculate the relativistic Doppler factor for a particle given its acceleration voltage
        and mass.

        Parameters
        ----------
        voltage : float or np.ndarray
            Acceleration voltage in volts.
        mass : float
            Particle mass in atomic mass units (amu).
        collinear : bool, optional
            If True, assumes collinear geometry (default is True).
        rest_to_lab : bool, optional
            If True, calculates the Doppler factor from rest frame to laboratory frame
            (default is True).

        Returns
        -------
        float or np.ndarray
            The relativistic Doppler factor. If `collinear` and `rest_to_lab` are both True or
            both False, returns the factor for transformation from rest to lab frame. Otherwise,
            returns the inverse factor.
        """
        m = mass * self.mu
        beta = np.sqrt(1 - (m**2 * self.c**4) / (self.e * voltage + m * self.c**2) ** 2)
        factor = (1 + beta) / np.sqrt(1 - beta**2)
        # numpy's stubs don't preserve the float|ndarray union through this arithmetic, so
        # both branches type as Any -- the float|ndarray return annotation is still accurate
        # at runtime (voltage's type determines it), mypy just can't verify it here.
        if (collinear and rest_to_lab) or (not collinear and not rest_to_lab):
            return factor  # type: ignore[no-any-return]
        else:
            return 1 / factor  # type: ignore[no-any-return]

    def dopplershift(
        self,
        frequency: float | np.ndarray,
        voltage: float | np.ndarray,
        mass: float,
        collinear: bool = True,
        rest_to_lab: bool = True,
    ) -> float | np.ndarray:
        """
        Calculate the Doppler-shifted frequency for a particle given its acceleration voltage
        and mass.

        Parameters
        ----------
        frequency : float or np.ndarray
            The rest frame frequency in Hz or wavenumbers.
        voltage : float or np.ndarray
            Acceleration voltage in volts.
        mass : float
            Particle mass in atomic mass units (amu).
        collinear : bool, optional
            If True, assumes collinear geometry (default is True).
        rest_to_lab : bool, optional
            If True, calculates the shift from rest frame to laboratory frame (default is True).

        Returns
        -------
        float or np.ndarray
            The Doppler-shifted frequency.
        """
        return frequency * self.dopplerfactor(voltage, mass, collinear, rest_to_lab)

    def __init__(self, VAccDiv: int = 1000, VCoolDiv: int = 10000, VCoolOffset: float = 0) -> None:
        """
        Initialize the CLSDataFrame object with voltage division settings.

        Parameters
        ----------
        VAccDiv : int, optional
            LCR voltage monitor scaling factor (default is 1000).
        VCoolDiv : int, optional
            Cooler voltage monitor scaling factor (default is 10000).
        VCoolOffset : float, optional
            Cooler voltage offset in volts (default is 0).
        """
        self.VCoolDiv = VCoolDiv
        self.VAccDiv = VAccDiv
        self.VCoolOffset = VCoolOffset
        # These are typed as the value Load_Run (or Compute_Voltages/Compute_WL) actually
        # populates them with, not Optional -- the class has no call-order guard (see
        # CLAUDE.md), so `None` here is a placeholder the rest of the pipeline is never
        # meant to observe, and typing it Optional would just push a `is not None` check
        # into every method instead of catching a genuine call-order bug at its use site.
        self.Vcool_init: float = None  # type: ignore[assignment]
        self.Laser_set: float = None  # type: ignore[assignment]
        self.Reference: float = None  # type: ignore[assignment]
        self.Step_Size: float = None  # type: ignore[assignment]
        self.ScanningRanges: list[list[float]] = None  # type: ignore[assignment]
        # (pandas ships no stubs -- see the mypy override in pyproject.toml -- so
        # pd.DataFrame itself resolves to Any and these four don't need an ignore.)
        self.Cal_df: pd.DataFrame = None
        self.Cal: list[float] = []
        self.Cal_err: list[float] = []
        self.Cal_order: int = None  # type: ignore[assignment]
        self.Cal_raw: pd.DataFrame = None  # CalSet/CalReadback as recorded, acquisition order
        self.Dropped_calibration_points: list[float] | None = None
        self.Readback_check: ReadbackFit = None  # type: ignore[assignment]
        # Optional scan-voltage map (set voltage DV [V] -> applied scan voltage [V]). When set it
        # replaces the Cal polynomial in Compute_Voltages and Frequency_ranges; Load_Run clears it.
        self.ScanVoltageMap: Callable[[np.ndarray], np.ndarray] | None = None
        self.Scan_calibration: ScanCalibration | None = None
        self.Scan_model: str | None = None
        self.Scan_offset_v: float | None = None
        # run scale applied with the table (1 = bench scale) and how it was chosen
        self.Scan_scale: float | None = None
        self.Scan_scale_err: float | None = None
        self.Scan_scale_mode: str | None = None
        self.Readback_fit: ReadbackFit | None = None
        self.Cooler_correction: str | None = None
        self.Cooler_faulty_readings = 0
        self.Cooler_window_s: float | None = None  # averaging window used by 'smooth'
        self.Cooler_ripple: CoolerRipple | None = None  # alias period and ripple of the readings
        # PMT counting statistics (clstools.counting): as loaded, and after the last filter.
        self.Trigger_period_us: float | None = None
        self.Counting_check_loaded: CountingCheck | None = None
        self.Counting_check: CountingCheck | None = None
        self.Dead_time_us: float | dict[int, float] | None = None
        self.Dead_time_removed: dict[int, float] = {}
        self.Dead_time_pileup: dict[int, float] = {}  # uncorrelated events it removes, per PMT
        self.Run: dd.DataFrame = None  # type: ignore[assignment]
        self.Binned: pd.DataFrame = None
        self.Setpoint_binned: pd.DataFrame = None
        self.Setpoint_dropped: list[float] = []  # under-visited set voltages left out
        self.ToF_binned: pd.DataFrame = None
        self.Raw_binned: pd.DataFrame = None
        self.Size: int = None  # type: ignore[assignment]
        self.Sorted: pd.DataFrame = None
        self.DAQTStime: float = None  # type: ignore[assignment]
        self.TSstart: float = None  # type: ignore[assignment]
        self.TSstop: float = None  # type: ignore[assignment]
        self.Harmonic = 2
        self.Dwell_Time: float = None  # type: ignore[assignment]
        self.Experiment: str = None  # type: ignore[assignment]
        self.Date: str = None  # type: ignore[assignment]
        self.Frequency_stepsize: float = None  # type: ignore[assignment]
        self.LoadingTime = 0.0
        self.ComputationVTime = 0.0
        self.ComputationWLTime = 0.0
        self.ComputationBinTime = 0.0

    def Info(self) -> None:
        """
        Display comprehensive information about the loaded CLS data run.

        This method prints detailed information including run parameters, voltage settings,
        calibration data, scanning ranges, timing information, and computation times.
        """
        print("\n")
        print("-----------------------------------------------------")
        print("     Run number  -> ", self.run_number)
        print("     Experiment  -> ", self.Experiment)
        print("     Date        -> ", self.Date)
        print("     Filename    -> ", self.run_filename)
        print("-----------------V-division settings-----------------")
        print("     Cooler voltage monitor scaling -> ", self.VCoolDiv)
        print("     Cooler voltage offset          -> ", self.VCoolOffset)
        print("     LCR voltage monitor scaling    -> ", self.VAccDiv)
        print("---------------------Calibration---------------------")
        print("     Initial Cooler Voltage [V] -> ", self.Vcool_init * self.VCoolDiv)
        print("     Laser Setpoint      [cm-1] -> ", self.Laser_set)
        print("     Calibration [p0 p1 p2 ...] -> ", [self.VAccDiv * i for i in self.Cal])
        print("     Calibration [e0 e1 e2 ...] -> ", [self.VAccDiv * i for i in self.Cal_err])
        if self.Dropped_calibration_points is not None:
            print("     Outlier calibration points found [V] -> ", self.Dropped_calibration_points)
        for issue in self.Readback_check.issues if self.Readback_check is not None else ():
            print("     Readback check: ", issue.message)
        if self.Scan_calibration is not None and self.Readback_fit is not None:
            fit = self.Readback_fit
            print("---------------Scan calibration table----------------")
            print(
                "     Table / model           -> ",
                f"{self.Scan_calibration.id} / {self.Scan_model}",
            )
            cal = self.Scan_calibration
            print("     Measured on             -> ", cal.measured)
            valid = f"{cal.first_valid_date}" + (
                f" (run {cal.valid_from_run})" if cal.valid_from_run is not None else ""
            )
            print("     Valid from              -> ", valid)
            print(
                "     Run offset          [V] -> ",
                f"{self.Scan_offset_v:+.4f} "
                f"(readback fit {fit.offset_v:+.4f} +- {fit.offset_err_v:.4f})",
            )
            scale = self.Scan_scale if self.Scan_scale is not None else 1.0
            scale_err = self.Scan_scale_err if self.Scan_scale_err is not None else 0.0
            print(
                "     Run scale         [ppm] -> ",
                f"{1e6 * (scale - 1):+.0f} +- {1e6 * scale_err:.0f} "
                f"({self.Scan_scale_mode}; calibration span {fit.span_v:.0f} V)",
            )
            print("     Readback rms        [V] -> ", f"{fit.residual_rms_v:.4f}")
            print(
                "     Readback gain     [ppm] -> ", f"{fit.gain_ppm:+.0f} +- {fit.gain_err_ppm:.0f}"
            )
            print("     Readback lag    [steps] -> ", fit.lag_steps)
            print("     Rejected readings   [V] -> ", fit.rejected_set_v)
        elif self.ScanVoltageMap is not None:
            print("     Scan voltage map        -> external (Set_Scan_Voltage_Map)")
        if self.Cooler_correction is not None:
            print("-----------------------Cooler------------------------")
            print("     Cooler correction       -> ", self.Cooler_correction)
            if self.Cooler_window_s is not None:
                print("     Averaging window    [s] -> ", f"{self.Cooler_window_s:.1f}")
            print("     Faulty cooler readings  -> ", self.Cooler_faulty_readings)
            ripple = self.Cooler_ripple
            if ripple is not None:
                print(
                    "     Reading interval    [s] -> ",
                    f"{ripple.reading_interval_s:.3f}",
                )
                print(
                    "     Alias period        [s] -> ",
                    "n/a (run too short)"
                    if np.isnan(ripple.alias_period_s)
                    else f"{ripple.alias_period_s:.2f}",
                )
                print(
                    "     Ripple rms          [V] -> ",
                    f"{ripple.fast_rms_v:.3f} (line broadening, not corrected)",
                )
        if self.Counting_check is not None:
            self._print_counting()
        print("-------------------Scanning Ranges-------------------")
        print("     Voltage Step Size        [V] -> ", self.Step_Size)
        print("     Frequency Step Size    [MHz] -> ", self.Frequency_stepsize / 1e6)
        for range in self.ScanningRanges:
            print(f"         from {range[0]} V to {range[1]} V")
        if self.Setpoint_binned is not None:
            print("     Setpoint bins               -> ", len(self.Setpoint_binned))
            if self.Setpoint_dropped:
                print(
                    "     Under-visited, left out [V] -> ",
                    self.Setpoint_dropped[:10],
                    "..." if len(self.Setpoint_dropped) > 10 else "",
                )
        print("--------------------General info---------------------")
        print("     Entries              -> ", self.Size)
        print("     DAQ Time         [s] -> ", self.DAQTStime)
        print("     start TS [Unix Time] -> ", self.TSstart)
        print("     stop  TS [Unix Time] -> ", self.TSstop)
        print("     start date           -> ", datetime.datetime.fromtimestamp(self.TSstart))
        print("     stop date            -> ", datetime.datetime.fromtimestamp(self.TSstop))
        print("----------------------------------------------------")
        print("     Loading time                [s] -> ", self.LoadingTime)
        print("     Voltage Computation time    [s] -> ", self.ComputationVTime)
        print("     Wavelenght Computation time [s] -> ", self.ComputationWLTime)
        print("     Binning Computation time    [s] -> ", self.ComputationBinTime)
        print("----------------------------------------------------")
        print("\n")

    def Info_to_csv(self, filename: str) -> None:
        """
        Export run information to a CSV file.

        Parameters
        ----------
        filename : str
            Path to the output CSV file where run information will be saved.
        """
        output = {
            "VCoolDiv": [self.VCoolDiv],
            "VAccDiv": [self.VAccDiv],
            "VCoolOffset": [self.VCoolOffset],
            "Vcool_init": [self.Vcool_init],
            "DAQTStime": [self.DAQTStime],
            "TSstart": [self.TSstart],
            "TSstop": [self.TSstop],
            "Laser_set": [self.Laser_set],
            "Harmonic": [self.Harmonic],
            "Dwell_Time": [self.Dwell_Time],
            "Step_Size": [self.Step_Size],
            "Frequency_stepsize": [self.Frequency_stepsize],
            "Experiment": [self.Experiment],
        }
        for ix, range in enumerate(self.ScanningRanges):
            output[f"Scan_range_low_{ix}"] = [min(range)]
            output[f"Scan_range_high_{ix}"] = [max(range)]

        output["Cal_order"] = [self.Cal_order]
        for ix, C in enumerate(zip(self.Cal, self.Cal_err, strict=True)):
            output[f"Cal_p{ix}"] = [C[0]]
            output[f"Cal_dp{ix}"] = [C[1]]

        if self.Scan_calibration is not None and self.Readback_fit is not None:
            output["Scan_calibration"] = [self.Scan_calibration.id]
            output["Scan_model"] = [self.Scan_model]
            output["Scan_offset_v"] = [self.Scan_offset_v]
            output["Scan_scale"] = [self.Scan_scale]
            output["Scan_scale_err"] = [self.Scan_scale_err]
            output["Scan_scale_mode"] = [self.Scan_scale_mode]
            output["Readback_offset_err_v"] = [self.Readback_fit.offset_err_v]
            output["Readback_rms_v"] = [self.Readback_fit.residual_rms_v]
            output["Readback_gain_ppm"] = [self.Readback_fit.gain_ppm]
            output["Readback_lag_steps"] = [self.Readback_fit.lag_steps]
            output["Readback_rejected"] = [len(self.Readback_fit.rejected_set_v)]
        if self.Cooler_correction is not None:
            output["Cooler_correction"] = [self.Cooler_correction]
            output["Cooler_faulty_readings"] = [self.Cooler_faulty_readings]
            output["Cooler_window_s"] = [self.Cooler_window_s]
            if self.Cooler_ripple is not None:
                output["Cooler_alias_period_s"] = [self.Cooler_ripple.alias_period_s]
                output["Cooler_ripple_rms_v"] = [self.Cooler_ripple.fast_rms_v]
        if self.Setpoint_binned is not None:
            output["Setpoint_bins"] = [len(self.Setpoint_binned)]
            output["Setpoint_dropped"] = [len(self.Setpoint_dropped)]

        if self.Counting_check is not None:
            check = self.Counting_check
            output["Trigger_period_us"] = [check.period_us]
            output["Fano_all"] = [check.fano_all]
            output["Fano_correlated_excess"] = [check.correlated_excess]
            for c, pmt in check.pmts.items():
                output[f"Events_pmt{c}"] = [pmt.events]
                output[f"Fano_pmt{c}"] = [pmt.fano]
                output[f"Burst_fraction_pmt{c}"] = [pmt.burst_fraction]
                output[f"Fano_dead_pmt{c}"] = [pmt.fano_dead]
                output[f"Pileup_dead_pmt{c}"] = [pmt.pileup_dead]
            for split, key in zip(
                check.splits, ("Split_odd_even", "Split_blocks", "Split_halves"), strict=False
            ):
                output[key] = [split.chi2_red]
            output["Dead_time_us"] = [self.Dead_time_us]
        df_out = pd.DataFrame(output)
        df_out.to_csv(filename, index=False)
        pass

    def Set_Scan_Voltage_Map(self, scan_map: Callable[[np.ndarray], np.ndarray] | None) -> None:
        """
        Install an external scan-voltage calibration.

        Parameters
        ----------
        scan_map : callable or None
            Maps the recorded set voltage DV [V] (numpy array) to the applied scan voltage [V].
            It replaces the CalSet/CalReadback polynomial in Compute_Voltages and
            Frequency_ranges. None restores the polynomial. Use_Scan_Calibration installs a
            packaged table through this hook.
        """
        self.ScanVoltageMap = scan_map
        self.Scan_calibration = None
        self.Scan_model = None
        self.Scan_offset_v = None
        self.Scan_scale = None
        self.Scan_scale_err = None
        self.Scan_scale_mode = None
        self.Readback_fit = None

    def Use_Scan_Calibration(
        self,
        table: str | ScanCalibration = "auto",
        model: str = "lut",
        offset_v: float | None = None,
        gain: str | float = "auto",
    ) -> ReadbackFit:
        """
        Calibrate the scan voltage with a bench-measured table instead of the per-run polynomial.

        The table holds the scan HV's shape including the DAC nonlinearity; the run's offset and
        scale are fitted to its CalSet/CalReadback, after correcting a readback lag and dropping
        unsettled readings:  applied(DV) = divider_scale * (scale * shape(DV) + offset). Call
        after Load_Run and before Compute_Voltages. Problems are reported as CalibrationWarning.

        Parameters
        ----------
        table : str or ScanCalibration, optional
            'auto' (default) picks the packaged table measured most recently before the run;
            otherwise a packaged table id, a path to a table file, or a ScanCalibration.
            A run before the table's validity (valid_from, e.g. before the DAC chain changed)
            gets a warning that the table does not describe it.
        model : str, optional
            'lut' (default): the table on its grid, its cubic fit off the grid. 'cubic': the cubic
            everywhere, i.e. without the DAC nonlinearity (for comparisons).
        offset_v : float, optional
            Use this run offset [V] instead of the readback fit, e.g. when the run's readback is
            known to be wrong. The fit is still made and returned as a diagnostic.
        gain : str or float, optional
            How the run scale is chosen (see ScanCalibration.fit_run):
            'auto' (default, the CLS systematics rule): fitted with the offset when the
            calibration spans >= 400 V, otherwise the campaign gain of the run's Experiment
            (packaged with the table), otherwise 1;
            'free': always fitted; 'campaign': the campaign gain (1 if unknown);
            'bench': 1, the bench scale (the in-run readback reads 50-80 ppm below the bench,
            which scale is right is not decided); or a number, the scale itself.

        A dead or corrupt readback (reading ~0 V, or a scale far from 1) gives no offset: the
        table's bench offset is used instead, with a warning.

        Returns
        -------
        ReadbackFit
            The readback fit against the table (also stored as Readback_fit); scale, scale_err
            and scale_mode give the run scale, also stored in Scan_scale, Scan_scale_err,
            Scan_scale_mode.
        """
        if model not in SCAN_MODELS:
            raise ValueError(f"unknown scan model {model!r}, choose from {SCAN_MODELS}")
        run_date = datetime.date.fromtimestamp(self.TSstart)
        run_number = self._run_number_int()
        if isinstance(table, ScanCalibration):
            cal = table
        elif table == "auto":
            cal = select_table(run_date, run_number)
        else:
            cal = load_table(table)
        if not cal.describes(run_date, run_number):
            if cal.valid_from is None and cal.valid_from_run is None:
                warn(
                    f"run {self.run_number} ({run_date}) predates scan calibration table {cal.id}, "
                    f"measured on {cal.measured}: the scan HV is assumed unchanged since the run"
                )
            else:
                since = f"{cal.first_valid_date}" + (
                    f", run {cal.valid_from_run}" if cal.valid_from_run is not None else ""
                )
                warn(
                    f"run {self.run_number} ({run_date}) predates scan calibration table {cal.id} "
                    f"(valid from {since}): the DAC nonlinearity was different then and the "
                    "table does not describe this run; use the polynomial calibration, or "
                    "model='cubic'"
                )

        fit = cal.fit_run(
            self.Cal_raw["Set"].to_numpy(float),
            self.Cal_raw["Read"].to_numpy(float) * self.VAccDiv,
            model,
            gain=gain,
            campaign=self.Experiment,
        )
        # Load_Run already reported the faults of the file it found on the same readback
        reported = (
            self.Readback_check.kinds & DATAFILE_ISSUES
            if self.Readback_check is not None
            else set()
        )
        for issue in fit.issues:
            if issue.kind not in reported:
                warn(f"run {self.run_number}: {issue.message}")
        if offset_v is not None:
            offset = float(offset_v)
        elif fit.usable:
            offset = fit.offset_v
        else:
            offset = cal.bench_offset_v
            why = (
                "it is dead or corrupt (readback scale far from 1)"
                if "readback_scale" in fit.kinds
                else "too few usable points"
            )
            warn(
                f"run {self.run_number}: the calibration readback cannot give the run offset "
                f"({why}); the bench offset of table {cal.id} ({offset:+.3f} V) is used, "
                "uncertain by the run-to-run spread (0.1-0.5 V within a campaign)"
            )

        dv = np.concatenate(
            [
                np.round(np.arange(min(r), max(r) + 0.5 * self.Step_Size, self.Step_Size), 6)
                for r in self.ScanningRanges
            ]
        )
        outside = ~cal.covers(dv)
        if outside.any():
            warn(
                f"run {self.run_number}: scanned set voltages down to {dv[outside].min():g} / "
                f"up to {dv[outside].max():g} V are outside table {cal.id} "
                f"[{cal.set_v[0]:g}, {cal.set_v[-1]:g}] V: cubic extrapolation there"
            )
        off_grid = cal.covers(dv) & ~cal.on_grid(dv)
        if model == "lut" and off_grid.any():
            warn(
                f"run {self.run_number}: {int(off_grid.sum())} scanned set voltages are not on the "
                f"grid of table {cal.id} (e.g. {dv[off_grid][0]:g} V): the cubic is used there, "
                "without the DAC nonlinearity"
            )

        self.Set_Scan_Voltage_Map(cal.scan_map(offset, model, fit.scale))
        self.Scan_calibration = cal
        self.Scan_model = model
        self.Scan_offset_v = offset
        self.Scan_scale = fit.scale
        self.Scan_scale_err = fit.scale_err
        self.Scan_scale_mode = fit.scale_mode
        self.Readback_fit = fit
        return fit

    def _run_number_int(self) -> int | None:
        try:
            return int(self.run_number)
        except (AttributeError, TypeError, ValueError):
            return None

    def _check_cooler_faults(self, max_dev_v: float | None) -> None:
        """Count (and warn about) faulty cooler readings in self.Sorted and measure the ripple
        of the fault-free ones; nothing is dropped."""
        if not {"TS", "Vrfq"} <= set(self.Sorted.columns):
            return
        ev = self.Sorted[["TS", "Vrfq"]].sort_values("TS", kind="stable")
        t_r, v_r = cooler_readings(ev["TS"].to_numpy(float), ev["Vrfq"].to_numpy(float))
        bad = faulty_readings(v_r * self.VCoolDiv, max_dev_v)
        self.Cooler_faulty_readings = int(bad.sum())
        self.Cooler_ripple = cooler_ripple(t_r[~bad], v_r[~bad] * self.VCoolDiv)
        if bad.any():
            warn(
                f"run {self.run_number}: {int(bad.sum())} cooler readings are further than "
                f"{max_dev_v:g} V from the run median (e.g. {v_r[bad][0] * self.VCoolDiv:.1f} V); "
                f"cooler_correction='{self.Cooler_correction}' uses them as recorded, "
                "'smooth' drops them"
            )

    def _smoothed_cooler(self, window_s: float | str, max_dev_v: float | None) -> dd.Series:
        """Per-event cooler readback (Vrfq units): faulty readings dropped, the rest averaged over
        the window and interpolated to each event's time. window_s 'auto': from the alias period
        of the run's readings (CoolerRipple.window_s)."""
        ev = self.Run[["TS", "Vrfq"]].compute().sort_values("TS", kind="stable")
        t_r, v_r = cooler_readings(ev["TS"].to_numpy(float), ev["Vrfq"].to_numpy(float))
        bad = faulty_readings(v_r * self.VCoolDiv, max_dev_v)
        self.Cooler_faulty_readings = int(bad.sum())
        if bad.any():
            warn(
                f"run {self.run_number}: {int(bad.sum())} cooler readings further than "
                f"{max_dev_v:g} V from the run median dropped "
                f"(e.g. {v_r[bad][0] * self.VCoolDiv:.1f} V)"
            )
        t_r, v_r = t_r[~bad], v_r[~bad]
        self.Cooler_ripple = cooler_ripple(t_r, v_r * self.VCoolDiv)
        if isinstance(window_s, str):
            if window_s != "auto":
                raise ValueError(f"cooler_window_s must be a number or 'auto', not {window_s!r}")
            window_s = self.Cooler_ripple.window_s()
        self.Cooler_window_s = float(window_s)
        v_s = smooth_readings(t_r, v_r, float(window_s))
        smoothed: dd.Series = self.Run["TS"].map_partitions(
            lambda ts: pd.Series(np.interp(ts.to_numpy(float), t_r, v_s), index=ts.index),
            meta=("Vrfq_smooth", "f8"),
        )
        return smoothed

    def Compute_Voltages(
        self,
        cooler_correction: str = "smooth",
        cooler_window_s: float | str = "auto",
        cooler_max_dev_v: float | None = COOLER_MAX_DEV_V,
    ) -> None:
        """
        Compute calibrated voltages using the scan calibration and cooler voltage correction.

        The scan voltage comes from the map installed by Use_Scan_Calibration or
        Set_Scan_Voltage_Map if there is one, otherwise from the CalSet/CalReadback polynomial.

        Parameters
        ----------
        cooler_correction : str, optional
            How each event gets its cooler voltage:
            - 'smooth' (default): drop faulty cooler readings, average the rest over a centred
              window and interpolate to each event's time. The DAQ's short-aperture readings
              (every ~0.1 s) alias the 50 Hz ripple into a 1.7-2.9 s oscillation the events
              cannot follow; the moving average removes it and keeps the slow drift
              (0.1-0.3 V over minutes). Adds the column 'Vrfq_smooth'.
            - 'pbp': each event's own reading (point by point), as before November 2026: adds
              the aliased ripple, 0.3-0.4 V rms (2.5-3.3 MHz on Bi and Yb+; 1-2 V rms before
              May 2026), to every event, and faulty readings go straight into V.
            - 'mean': the run's mean reading for all events (no slow drift).
        cooler_window_s : float or 'auto', optional
            Averaging window for 'smooth' [s]. 'auto' (default): 4 alias periods of the run's
            readings, at least 15 s (10-20 s for the usual ~0.1 s reading interval, 30-75 s for
            files read every >= 0.4 s); 15 s when the run is too short to measure the alias.
        cooler_max_dev_v : float or None, optional
            Cooler readings further than this from the run median [V at the cooler] are readback
            faults (default 5 V). 'smooth' drops them; 'pbp' and 'mean' only warn. None disables
            the check.

        Every mode also measures the ripple of the readings (Cooler_ripple: alias period and
        fast rms). The rms is the ripple the ions see, a run-dependent line broadening that no
        mode can correct (the readout is not synchronised with the bunches); Info shows it.

        Raises
        ------
        AttributeError
            If cooler_correction parameter is not 'pbp', 'mean' or 'smooth'.
        """
        start = time.time()
        if cooler_correction not in ("pbp", "mean", "smooth"):
            raise AttributeError(
                "Error in cooler_correction parameter. Use one between pbp, mean and smooth"
            )
        self.Cooler_correction = cooler_correction
        self.Cooler_window_s = None
        self.Cooler_ripple = None
        # self.Run["DV_cal"]=(self.Run["DV"]+(random()-0.5)*self.data.Step_Size)
        #                    *self.Cal_m+self.Cal_q
        if self.ScanVoltageMap is not None:
            scan_map = self.ScanVoltageMap
            self.Run["DV_cal"] = self.Run["DV"].map_partitions(
                lambda s: pd.Series(scan_map(s.to_numpy(float)), index=s.index),
                meta=("DV_cal", "f8"),
            )
        elif self.Cal_order == 1:
            self.Run["DV_cal"] = (self.Run["DV"] * self.Cal[1] + self.Cal[0]) * self.VAccDiv
        elif self.Cal_order == 2:
            self.Run["DV_cal"] = (
                self.Cal[2] * self.Run["DV"] ** 2 + self.Run["DV"] * self.Cal[1] + self.Cal[0]
            ) * self.VAccDiv
        elif self.Cal_order == 3:
            self.Run["DV_cal"] = (
                self.Cal[3] * self.Run["DV"] ** 3
                + self.Cal[2] * self.Run["DV"] ** 2
                + self.Run["DV"] * self.Cal[1]
                + self.Cal[0]
            ) * self.VAccDiv

        # if cooler_correction in ['ptp','mean','linear']:

        if cooler_correction == "pbp":
            self.Run["V"] = self.Run["Vrfq"] * self.VCoolDiv + self.VCoolOffset - self.Run["DV_cal"]
        elif cooler_correction == "mean":
            self.Run["V"] = (
                self.Run["Vrfq"].mean().compute() * self.VCoolDiv
                + self.VCoolOffset
                - self.Run["DV_cal"]
            )
        else:
            self.Run["Vrfq_smooth"] = self._smoothed_cooler(cooler_window_s, cooler_max_dev_v)
            self.Run["V"] = (
                self.Run["Vrfq_smooth"] * self.VCoolDiv + self.VCoolOffset - self.Run["DV_cal"]
            )

        self.Sorted = self.Run.compute()
        if cooler_correction != "smooth":
            self._check_cooler_faults(cooler_max_dev_v)
        self.ComputationVTime = time.time() - start

    def Compute_Setpoint_Bins(
        self,
        TOF_gate: list[float] | None = None,
        V_gate: list[float] | None = None,
        F_gate: list[float] | None = None,
        PMT_gate: list[int] | None = None,
        min_visits: float = 0.75,
    ) -> pd.DataFrame:
        """
        One point per scan set voltage: the counts of its gated events at their mean frequency.

        Frequency bins of one step (Compute_Bins) straddle set voltages once the scan voltages
        are unevenly spaced (scan table, cooler drift) and cover the ends of the scanned ranges
        only partly. In the CLS systematics study (2026-11-02) one point per set voltage lowered
        chi2_red of Bi 8244/8245 from 3.4/4.2 to 2.5/2.0, moved Bl by 2.6 MHz, and removed the
        0.15-1 MHz jumps of the fits when the cooler offset is scanned.

        Needs Compute_WL (columns F and V). Set voltages with fewer visits than min_visits times
        the median (the sweep end points, visited half as often by an up-down scan) are left out
        and listed in Setpoint_dropped. The x value of a set voltage without gated events is the
        mean frequency of all its events.

        Parameters
        ----------
        TOF_gate : list of float, optional
            [min, max] ToF [us] of the events counted (strictly inside, as Compute_Bins).
        V_gate : list of float, optional
            [min, max] set voltage DV [V] of the set voltages kept (strictly inside).
        F_gate : list of float, optional
            [min, max] mean frequency [Hz, relative to ref] of the set voltages kept.
        PMT_gate : list of int, optional
            PMT channels (TDC, 1-4) counted. All channels if None.
        min_visits : float, optional
            Minimum number of visits relative to the median (default 0.75).

        Returns
        -------
        pandas.DataFrame
            Also stored in Setpoint_binned, sorted by frequency, columns: DV (set voltage [V]),
            Fcount (counts), Fcount_err (sqrt(counts), 1 for 0), Fmean, Fstd (mean and
            standard deviation of the gated events' F [Hz]), Vmean, Vstd (same for V [V]),
            visits (visits of the set voltage).
        """
        start = time.time()
        ev = self.Run[["Bunch", "TOF", "DV", "TDC", "F", "V"]].compute()
        order = np.lexsort((ev["TOF"].to_numpy(), ev["Bunch"].to_numpy()))
        dv_t = ev["DV"].to_numpy()[order]
        first = np.r_[0, np.flatnonzero(np.diff(dv_t) != 0) + 1] if len(dv_t) else []
        visits = pd.Series(dv_t[first]).value_counts()

        sel = np.ones(len(ev), dtype=bool)
        if TOF_gate is not None:
            tof = ev["TOF"].to_numpy()
            sel &= (tof > min(TOF_gate)) & (tof < max(TOF_gate))
        if PMT_gate is not None:
            sel &= ev["TDC"].isin(PMT_gate).to_numpy()
        gated = ev[sel].groupby("DV")
        table = pd.DataFrame(
            {
                "Fcount": gated.size(),
                "Fmean": gated["F"].mean(),
                "Fstd": gated["F"].std(),
                "Vmean": gated["V"].mean(),
                "Vstd": gated["V"].std(),
            }
        )
        every = ev.groupby("DV")[["F", "V"]].mean()
        table = table.reindex(every.index)
        table["Fcount"] = table["Fcount"].fillna(0).astype(int)
        table["Fmean"] = table["Fmean"].fillna(every["F"])
        table["Vmean"] = table["Vmean"].fillna(every["V"])
        table["visits"] = visits.reindex(table.index).fillna(0).astype(int).to_numpy()

        dv = table.index.to_numpy(float)
        if V_gate is not None:
            table = table[(dv > min(V_gate)) & (dv < max(V_gate))]
        enough = table["visits"] >= min_visits * np.median(table["visits"]) if len(table) else []
        self.Setpoint_dropped = [float(v) for v in table.index[~np.asarray(enough, bool)]]
        table = table[np.asarray(enough, bool)]
        if F_gate is not None:
            table = table[(table["Fmean"] > min(F_gate)) & (table["Fmean"] < max(F_gate))]
        table["Fcount_err"] = np.where(table["Fcount"] > 0, np.sqrt(table["Fcount"]), 1.0)
        table = table.rename_axis("DV").reset_index().sort_values("Fmean", ignore_index=True)
        self.Setpoint_binned = table[
            ["DV", "Fcount", "Fcount_err", "Fmean", "Fstd", "Vmean", "Vstd", "visits"]
        ]
        self.ComputationBinTime = time.time() - start
        return self.Setpoint_binned

    def Compute_WL(self, Mass: float, ref: float = 0, harmonic: int = 2) -> None:
        """
        Compute wavelengths and frequencies using Doppler shift calculations.

        Parameters
        ----------
        Mass : float
            Particle mass in atomic mass units (amu).
        ref : float, optional
            Reference frequency offset in Hz (default is 0).
        harmonic : int, optional
            Harmonic number for the laser frequency (default is 2).
        """
        start = time.time()
        self.Mass = Mass
        self.Reference = ref
        self.Harmonic = harmonic
        self.Frequency_stepsize = float(
            np.abs(
                self.dopplershift(
                    harmonic * self.Laser_set,
                    self.Vcool_init * self.VCoolDiv,
                    self.Mass,
                    collinear=False,
                    rest_to_lab=False,
                )
                - self.dopplershift(
                    harmonic * self.Laser_set,
                    self.Vcool_init * self.VCoolDiv + self.Step_Size,
                    self.Mass,
                    collinear=False,
                    rest_to_lab=False,
                )
            )
        )
        self.Frequency_stepsize = self.Frequency_stepsize * self.WN_to_f
        self.Run["WN"] = self.dopplershift(
            harmonic * self.Laser_set, self.Run["V"], self.Mass, collinear=False, rest_to_lab=False
        )
        self.Run["F"] = (self.WN_to_f * self.Run["WN"]) - self.Reference
        self.Sorted = self.Run.compute()

        self.ComputationWLTime = time.time() - start

        return

    def Frequency_ranges(self, Mass: float, ref: float = 0, harmonic: int = 2) -> list[list[float]]:
        """
        Return the voltage scanning ranges in doppler shifted frequency

        Parameters
        ----------
        Mass : float
            Particle mass in atomic mass units (amu).
        ref : float, optional
            Reference frequency offset in Hz (default is 0).
        harmonic : int, optional
            Harmonic number for the laser frequency (default is 2).

        return: list of [float, float]
            list of ranges in frequency value [MHz]
        """
        self.Mass = Mass
        self.Reference = ref
        self.Harmonic = harmonic

        returnlist: list[list[float]] = []
        for range in self.ScanningRanges:
            lo, hi = float(min(range)), float(max(range))
            if self.ScanVoltageMap is not None:
                lo, hi = (float(v) for v in self.ScanVoltageMap(np.array([lo, hi])))
            maxV = self.Vcool_init * self.VCoolDiv + self.VCoolOffset - hi
            minV = self.Vcool_init * self.VCoolDiv + self.VCoolOffset - lo
            WN_min = self.dopplershift(
                harmonic * self.Laser_set, maxV, self.Mass, collinear=False, rest_to_lab=False
            )
            WN_max = self.dopplershift(
                harmonic * self.Laser_set, minV, self.Mass, collinear=False, rest_to_lab=False
            )
            returnlist.append(
                [
                    float((self.WN_to_f * WN_min) - self.Reference) / 1e6,
                    float((self.WN_to_f * WN_max) - self.Reference) / 1e6,
                ]
            )

        return returnlist

    def Shift_Ref(self, ref: float = 0) -> None:
        """
        Shift the reference frequency for all frequency calculations.

        Parameters
        ----------
        ref : float, optional
            New reference frequency offset in Hz (default is 0).
        """
        self.Reference = ref
        self.Run["F"] = (self.WN_to_f * self.Run["WN"]) - self.Reference
        self.Sorted = self.Run.compute()

        return

    def apply_filter(self, filter_window: int = 0) -> None:
        """
        Apply a filter to remove timestamps with too many events.

        Parameters
        ----------
        filter_window : int, optional
            Maximum number of events allowed per timestamp. Events exceeding this
            threshold will be filtered out. If 0, no filtering is applied (default is 0).
        """
        tmp = self.Run

        if filter_window > 0:
            tmp = tmp.compute().copy()
            tmp["filter"] = True
            tmp["filter"] = tmp.groupby("TS")["filter"].transform(
                lambda x: not x.size > filter_window
            )
            tmp = tmp[tmp["filter"]]
            self.Run = dd.from_pandas(tmp, npartitions=6)
            if self.Counting_check is not None:
                self.Check_Counting()

        return

    def apply_dead_time(self, dead_time_us: float | dict[int, float] = DEAD_TIME_US) -> None:
        """
        Apply an artificial dead time per PMT, against afterpulse bursts. Never applied unless
        called.

        An event is dropped when the previous event of the same PMT (TDC channel), kept or not,
        is closer than the dead time (extending dead time); event time = Bunch * trigger period
        + TOF. Call it after Load_Run and before binning; Sorted is refreshed if it exists. The
        fraction of events removed per PMT is stored in Dead_time_removed, and the counting
        statistics are checked again (Counting_check).

        DC beam (Bi offline, October 2026; CLS systematics 2026-11-02): 10 us removed 5.0-5.2 %
        of the events, brought the Fano factor from 1.9-2.0 to 1.03 and chi2_red from 2.0-2.5 to
        1.12-1.14; 5 and 20 us gave the same hyperfine constants.

        Bunched beam (Yb+ I340): the photons of a bunch arrive within a few us, so a dead time
        also removes real photons, 7-17 % at the line peaks, which distorts the line shape. The
        fraction of uncorrelated events the dead time removes (pile-up, see
        clstools.counting.pileup_fraction) is stored in Dead_time_pileup, and a CountingWarning
        is raised when it is above 1 % for a PMT. Check Fano(dead) and the pile-up column of
        Info first; for bunched beams use 1 us or none.

        Parameters
        ----------
        dead_time_us : float or dict, optional
            Dead time in us for every PMT (default 10), or {channel: us}; channels not listed
            keep all their events.
        """
        period = self._trigger_period()
        tmp = self.Run.compute()
        tdc = tmp["TDC"].to_numpy()
        bunch, tof = tmp["Bunch"].to_numpy(), tmp["TOF"].to_numpy()
        keep = dead_time_keep(bunch, tdc, tof, period, dead_time_us)
        self.Dead_time_removed = {int(c): float(1 - keep[tdc == c].mean()) for c in np.unique(tdc)}
        self.Dead_time_pileup = pileup_fraction(tmp["DV"].to_numpy(), bunch, tdc, tof, dead_time_us)
        lost = {
            c: f
            for c, f in self.Dead_time_pileup.items()
            if f > PILEUP_WARN and int((tdc == c).sum()) >= MIN_EVENTS
        }
        if lost:
            listed = ", ".join(
                f"PMT {c} {100 * f:.1f} % (of {100 * self.Dead_time_removed[c]:.1f} % removed)"
                for c, f in lost.items()
            )
            warn_counting(
                f"run {self.run_number}: the dead time also removes uncorrelated (real) events, "
                f"by pile-up: {listed}. With a bunched beam or a high rate real photons are lost "
                "at the line peaks and the line shape is distorted; use a shorter dead time "
                "(1 us) or none"
            )
        self.Dead_time_us = dead_time_us
        tmp = tmp[keep]
        self.Run = dd.from_pandas(tmp, npartitions=max(1, self.Run.npartitions))
        if self.Sorted is not None:
            self.Sorted = tmp
        self.Check_Counting()

        return

    def Check_Counting(self) -> CountingCheck:
        """
        Check the PMT counting statistics of the current events (after any filter).

        Per PMT: the Fano factor (variance / mean) of the counts per trigger, per scan step and
        pooled, which is 1 for Poisson counting; the fraction of events less than 5 us after
        the previous event of the same PMT (afterpulse bursts) and its uncorrelated expectation;
        the Fano factor after a 10 us dead time, and the fraction of uncorrelated events that
        dead time would remove (pile-up). For the summed counts, how much more they fluctuate
        than the single PMTs (beam or laser intensity, noise bursts on all channels), and the
        split tests of the counts per set voltage: odd/even triggers, alternating 10 s blocks,
        first/second half of the run (chi2/dof, 1 for Poisson; see clstools.counting).
        A CountingWarning is raised when a Fano factor is off.

        Returns
        -------
        CountingCheck
            Also stored in Counting_check and shown by Info.
        """
        tmp = self.Run[["TS", "DV", "Bunch", "TDC", "TOF"]].compute()
        return self._check_counting(
            tmp["DV"].to_numpy(),
            tmp["Bunch"].to_numpy(),
            tmp["TDC"].to_numpy(),
            tmp["TOF"].to_numpy(),
            tmp["TS"].to_numpy(),
        )

    def _trigger_period(self) -> float:
        if self.Trigger_period_us is None:
            tmp = self.Run[["TS", "Bunch", "TOF"]].compute()
            self.Trigger_period_us = trigger_period_us(
                tmp["TS"].to_numpy(), tmp["Bunch"].to_numpy(), tmp["TOF"].to_numpy()
            )
        return self.Trigger_period_us

    def _check_counting(
        self,
        dv: np.ndarray,
        bunch: np.ndarray,
        tdc: np.ndarray,
        tof: np.ndarray,
        ts: np.ndarray | None = None,
    ) -> CountingCheck:
        self.Counting_check = check_counting(dv, bunch, tdc, tof, self._trigger_period(), ts=ts)
        for issue in self.Counting_check.issues:
            warn_counting(f"run {self.run_number}: {issue.message}")
        return self.Counting_check

    def _print_counting(self) -> None:
        check = self.Counting_check
        assert check is not None

        def num(x: float, fmt: str) -> str:
            return "n/a" if np.isnan(x) else format(x, fmt)

        w = f"{BURST_WINDOW_US:g}us"
        d = f"{check.dead_time_us:g}us"
        print("--------------------PMT counting---------------------")
        print("     Trigger period          [us] -> ", f"{check.period_us:g}")
        print(
            f"     PMT   events  rate[cps]   Fano          bursts<{w} (exp.)  "
            f"Fano({d} dead)  pile-up({d})"
        )
        for pmt in check.pmts.values():
            bursts = (
                f"{num(100 * pmt.burst_fraction, '.1f')} % ({num(100 * pmt.burst_expected, '.1f')})"
            )
            print(
                f"     {pmt.channel:3d} {pmt.events:8d} {pmt.rate_cps:10.1f}   "
                f"{num(pmt.fano, '.2f'):>5s} +- {num(pmt.fano_err, '.2f'):<5s}  "
                f"{bursts:>17s}   {num(pmt.fano_dead, '.2f'):>5s}           "
                f"{num(100 * pmt.pileup_dead, '.1f'):>5s} %"
            )
        print(
            "     sum                        ",
            f"{num(check.fano_all, '.2f'):>5s} +- {num(check.fano_all_err, '.2f'):<5s}  "
            f"correlated excess {num(check.correlated_excess, '+.2f')}",
        )
        if np.isnan(check.fano_all):
            print("     (no Fano factor: too few triggers per scan step)")
        if check.splits:
            print("     Split tests, counts per set voltage (chi2/dof, 1 = Poisson):")
            for split in check.splits:
                value = (
                    "n/a (too few set voltages with >= 3 events per visit)"
                    if np.isnan(split.chi2_red)
                    else f"{split.chi2_red:.2f} +- {split.chi2_red_err:.2f} "
                    f"({split.n_setpoints} set voltages)"
                )
                print(f"       {split.name:<24s} {value}")
        if self.Dead_time_us is not None:
            removed = ", ".join(
                f"PMT {c} {100 * f:.1f} %" for c, f in self.Dead_time_removed.items()
            )
            print(
                "     Dead time               [us] -> ", self.Dead_time_us, f"(removed {removed})"
            )
            if self.Dead_time_pileup:
                print(
                    "     of which pile-up (real)      -> ",
                    ", ".join(
                        f"PMT {c} {num(100 * f, '.1f')} %" for c, f in self.Dead_time_pileup.items()
                    ),
                )
        loaded = self.Counting_check_loaded
        if loaded is not None and loaded is not check:
            print(
                "     Fano as loaded               -> ",
                ", ".join(f"PMT {c} {num(p.fano, '.2f')}" for c, p in loaded.pmts.items()),
            )
        for issue in check.issues:
            print("     Counting check: ", issue.message)

    def Compute_ToF(
        self, V_gate: list[float] | None = None, PMT_gate: list[int] | None = None
    ) -> None:
        """
        Compute time-of-flight binned data with optional gating.

        Parameters
        ----------
        V_gate : list of float, optional
            Voltage gate range [min_voltage, max_voltage]. If None, no voltage gating is applied.
        PMT_gate : list of int, optional
            List of PMT channels to include (1-4). If None, all PMT channels are included.
        """
        tmp = self.Run[["TOF", "DV", "TDC"]]
        tmp["counts"] = 1
        if V_gate is not None:
            tmp = tmp[max(V_gate) > tmp.DV]
            tmp = tmp[min(V_gate) < tmp.DV]

        if PMT_gate is not None:
            PMTS = [1, 2, 3, 4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[pmt != tmp.TDC]
        tmp = tmp[["TOF", "counts"]].groupby("TOF").sum()
        self.ToF_binned = tmp.compute()

        return

    def Compute_Bins(
        self,
        TOF_gate: list[float] | None = None,
        V_gate: list[float] | None = None,
        F_gate: list[float] | None = None,
        PMT_gate: list[int] | None = None,
        bins: int | None = None,
    ) -> None:
        """
        Compute frequency-binned data with optional gating and custom binning.

        Parameters
        ----------
        TOF_gate : list of float, optional
            Time-of-flight gate range [min_tof, max_tof]. If None, no ToF gating is applied.
        V_gate : list of float, optional
            Voltage gate range [min_voltage, max_voltage]. If None, no voltage gating is applied.
        F_gate : list of float, optional
            Frequency gate range [min_frequency, max_frequency]. If None, no frequency gating is
            applied.
        PMT_gate : list of int, optional
            List of PMT channels to include (1-4). If None, all PMT channels are included.
        bins : int, optional
            Number of frequency bins to use. If None, automatically calculated from frequency
            step size.
        """
        start = time.time()
        tmp = self.Run[["TS", "F", "TOF", "DV", "TDC", "V"]]

        if TOF_gate is not None:
            tmp = tmp[max(TOF_gate) > tmp.TOF]
            tmp = tmp[min(TOF_gate) < tmp.TOF]

        if V_gate is not None:
            tmp = tmp[max(V_gate) > tmp.DV]
            tmp = tmp[min(V_gate) < tmp.DV]

        if F_gate is not None:
            tmp = tmp[max(F_gate) > tmp.F]
            tmp = tmp[min(F_gate) < tmp.F]

        if PMT_gate is not None:
            PMTS = [1, 2, 3, 4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[pmt != tmp.TDC]

        self.Binned = tmp.compute()
        maxF = self.Binned["F"].max()
        minF = self.Binned["F"].min()
        indexedbins = bins if bins is not None else int((maxF - minF) / self.Frequency_stepsize)

        self.Binned["bins"] = pd.cut(x=self.Binned["F"], bins=indexedbins)
        self.Binned = (
            self.Binned.groupby(by="bins", as_index=False, observed=False)
            .aggregate({"F": ["count", "mean", "std"], "V": ["mean", "std"]})
            .pipe(lambda x: x.set_axis(x.columns.map("".join), axis=1))
        )
        self.Binned = self.Binned.sort_values(by=["bins"])
        self.Binned["bins_center"] = self.Binned["bins"].apply(lambda x: x.mid).astype(float)
        self.Binned["bins_width"] = self.Binned["bins"].apply(lambda x: x.length).astype(float)
        self.Binned["Fmean"] = self.Binned[["Fmean", "bins_center"]].apply(
            lambda x: x["bins_center"] if np.isnan(x["Fmean"]) else x["Fmean"], axis=1
        )
        self.Binned["Fstd"] = self.Binned[["Fstd", "bins_width"]].apply(
            lambda x: x["bins_width"] * 0.5 if np.isnan(x["Fstd"]) else x["Fstd"], axis=1
        )
        self.ComputationBinTime = time.time() - start

        return

    def Compute_Raw_Bins(
        self,
        TOF_gate: list[float] | None = None,
        V_gate: list[float] | None = None,
        PMT_gate: list[int] | None = None,
    ) -> None:
        """
        Compute raw voltage-binned data with optional gating.

        Parameters
        ----------
        TOF_gate : list of float, optional
            Time-of-flight gate range [min_tof, max_tof]. If None, no ToF gating is applied.
        V_gate : list of float, optional
            Voltage gate range [min_voltage, max_voltage]. If None, no voltage gating is applied.
        PMT_gate : list of int, optional
            List of PMT channels to include (1-4). If None, all PMT channels are included.
        """
        tmp = self.Run[["TOF", "DV", "TDC"]]
        tmp["counts"] = 1
        if TOF_gate is not None:
            tmp = tmp[max(TOF_gate) > tmp.TOF]
            tmp = tmp[min(TOF_gate) < tmp.TOF]

        if V_gate is not None:
            tmp = tmp[max(V_gate) > tmp.DV]
            tmp = tmp[min(V_gate) < tmp.DV]

        if PMT_gate is not None:
            PMTS = [1, 2, 3, 4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[pmt != tmp.TDC]

        tmp = tmp[["DV", "counts"]].groupby("DV").sum()
        self.Raw_binned = tmp.compute()

        return

    def Load_Run(
        self,
        filename: str,
        cal_order: int = 1,
        blocksize: float = 25e6,
        filter_calibration: bool | str = True,
        ignore_intercept: bool = False,
        counting_check: bool = True,
    ) -> None:
        """
        Load CLS data run from an ASDF file and perform initial calibration.

        Parameters
        ----------
        filename : str
            Path to the ASDF file containing the CLS data run.
        cal_order : int, optional
            Order of polynomial calibration (1-3). Default is 1 (linear).
        blocksize : float, optional
            Block size for Dask DataFrame partitioning. Default is 25e6.
        filter_calibration : bool or 'robust', optional
            True (default): drop calibration points beyond 2 sigma of the fit residuals and refit.
            'robust': correct a readback lag and drop unsettled readings as found by the
            readback check (see below), then fit. False: fit all points.
        ignore_intercept : bool, optional
            Whether to ignore the intercept in calibration fitting. Default is False.
        counting_check : bool, optional
            Check the PMT counting statistics (default True; ~1 s per million events).
            Check_Counting runs it later.

        Notes
        -----
        The calibration readback is always checked (clstools.calibration.analyse_readback) and
        the result is stored in Readback_check. Faults of the file itself are reported as
        CalibrationWarning: mismatched CalSet/CalReadback lengths, non-finite values, too few
        points, a readback lagging the set value by whole steps, unsettled readings after
        jumps, and a readback scale off by more than 1 %. How well the scan table matches the
        run (noise, gain, offset) is only reported by Use_Scan_Calibration.

        The readback is compared with the shape of the packaged scan table when the set
        voltages are inside it: only the table's DAC nonlinearity makes a lag visible, since on
        a monotonic sweep a straight line absorbs it as a one-step offset. Otherwise it is
        compared with a polynomial of cal_order. With filter_calibration True or False the check
        changes no numbers.

        The PMT counting statistics are checked too (Counting_check, see Check_Counting) and a
        CountingWarning is raised when a PMT's counts per trigger are not Poisson; the result
        as loaded stays in Counting_check_loaded.
        """
        start = time.time()

        if isinstance(filter_calibration, str) and filter_calibration != "robust":
            raise ValueError(
                f"filter_calibration must be True, False or 'robust', not {filter_calibration!r}"
            )
        self.blocksize = blocksize
        self.Cal_order = cal_order
        self.run_filename = filename
        self.Set_Scan_Voltage_Map(None)
        self.Dropped_calibration_points = None
        self.Dead_time_us = None
        self.Dead_time_removed = {}
        self.Dead_time_pileup = {}
        self.Setpoint_binned = None
        self.Setpoint_dropped = []
        self.Cooler_correction = None
        self.Cooler_window_s = None
        self.Cooler_ripple = None

        with asdf.open(self.run_filename) as af:
            self.run_number = af.tree["Run"]
            self.Vcool_init = af.tree["CoolerVoltage"]
            self.Laser_set = af.tree["LaserSetpoint"]
            self.Dwell_Time = af.tree["DwellTime"]
            self.Experiment = af.tree["Experiment"]
            self.Date = af.tree["Date"]
            self.Step_Size = af.tree["StepSize"]
            self.ScanningRanges = af.tree["ScanningRanges"]

            cal_set = np.asarray(af["CalSet"], dtype=float)
            cal_read = np.asarray(af["CalReadback"], dtype=float)
            if len(cal_set) != len(cal_read):
                n = min(len(cal_set), len(cal_read))
                warn(
                    f"run {self.run_number}: CalSet has {len(cal_set)} values but CalReadback "
                    f"{len(cal_read)}; only the first {n} pairs are used"
                )
                cal_set, cal_read = cal_set[:n], cal_read[:n]
            self.Cal_raw = pd.DataFrame({"Set": cal_set, "Read": cal_read})
            raw = np.array(af.tree["raw"])
            ts = raw[:, 0] if raw.ndim == 2 and len(raw) else np.array([time.time()])
            self.Readback_check = self._check_readback(
                cal_set, cal_read, datetime.date.fromtimestamp(float(np.nanmin(ts)))
            )
            for issue in self.Readback_check.issues:
                if issue.kind in DATAFILE_ISSUES:
                    warn(f"run {self.run_number}: {issue.message}")

            if filter_calibration == "robust":
                check = self.Readback_check
                self.Cal_df = pd.DataFrame(
                    {
                        "Set": check.set_v[check.keep],
                        "Read": check.readback_v[check.keep] / self.VAccDiv,
                    }
                )
                self.Dropped_calibration_points = check.rejected_set_v or None
            else:
                finite = np.isfinite(cal_set) & np.isfinite(cal_read)
                self.Cal_df = self.Cal_raw[finite].reset_index(drop=True)

            self.Cal, self.Cal_err = self._fit_cal(self.Cal_df["Set"], self.Cal_df["Read"])

            self.Cal_df["Fit"] = self.Cal_df["Set"].apply(lambda x: np.polyval(self.Cal[::-1], x))
            self.Cal_df["Residual"] = self.Cal_df["Read"] - self.Cal_df["Fit"]

            if filter_calibration is True:
                std_residual = self.Cal_df["Residual"].std()
                menan_residual = self.Cal_df["Residual"].mean()
                self.Cal_df_filtered = self.Cal_df[
                    np.abs(self.Cal_df["Residual"]) <= 2 * std_residual
                ]
                indexes_to_drop = self.Cal_df.index.difference(self.Cal_df_filtered.index)
                if self.Cal_df_filtered.size != self.Cal_df.size:
                    print(
                        "Warning, calibration points outside 2 sigma of the residuals have "
                        "been filtered out."
                    )
                    print(
                        f"Mean of voltage calibration residuals: {menan_residual:.5f}, "
                        f"Standard deviation of residuals: {std_residual:.5f}"
                    )
                    n_dropped = len(indexes_to_drop)
                    plural = "s" if n_dropped > 1 else ""
                    print(f"{n_dropped} point{plural} removed.")
                    self.Dropped_calibration_points = self.Cal_df.loc[
                        indexes_to_drop, "Set"
                    ].tolist()
                    print(f"Removed point{plural} [V]: {self.Dropped_calibration_points}")
                    self.Cal, self.Cal_err = self._fit_cal(
                        self.Cal_df_filtered["Set"], self.Cal_df_filtered["Read"]
                    )

                    self.Cal_df = self.Cal_df_filtered
                    self.Cal_df["Fit"] = self.Cal_df["Set"].apply(
                        lambda x: np.polyval(self.Cal[::-1], x)
                    )
                    self.Cal_df["Residual"] = self.Cal_df["Read"] - self.Cal_df["Fit"]

            if ignore_intercept:
                self.Cal[0] = 0
                self.Cal_err[0] = 0
                self.Cal_df["Fit"] = self.Cal_df["Set"].apply(
                    lambda x: np.polyval(self.Cal[::-1], x)
                )
                self.Cal_df["Residual"] = self.Cal_df["Read"] - self.Cal_df["Fit"]

            self.Run = dd.from_array(raw, columns=["TS", "DV", "Bunch", "TDC", "TOF", "Vrfq"])

        self.Trigger_period_us = None
        self.Counting_check = self.Counting_check_loaded = None
        if raw.ndim == 2 and len(raw):
            self.Trigger_period_us = trigger_period_us(raw[:, 0], raw[:, 2], raw[:, 4])
        if counting_check and self.Trigger_period_us is not None:
            self.Counting_check_loaded = self._check_counting(
                raw[:, 1], raw[:, 2], raw[:, 3], raw[:, 4], raw[:, 0]
            )

        self.TSstart = self.Run["TS"].min().compute()
        self.TSstop = self.Run["TS"].max().compute()

        self.DAQTStime = self.TSstop - self.TSstart
        self.Size = len(self.Run)

        self.LoadingTime = time.time() - start
        return

    def Update_Cal(self, cal_order: int = 1) -> None:
        """
        Update the calibration coefficients with a new polynomial order.

        Parameters
        ----------
        cal_order : int, optional
            Order of polynomial calibration (1-3). Default is 1 (linear).
        """
        self.Cal_order = cal_order
        self.Cal, self.Cal_err = self._fit_cal(self.Cal_df["Set"], self.Cal_df["Read"])

        return

    def _check_readback(
        self, cal_set: np.ndarray, cal_read: np.ndarray, run_date: datetime.date
    ) -> ReadbackFit:
        """Readback check against the packaged table's shape if it covers the set voltages,
        otherwise against a polynomial of Cal_order."""
        table = select_table(run_date, self._run_number_int())
        finite = cal_set[np.isfinite(cal_set)]
        if len(finite) and table.covers(finite).all():
            return table.fit_run_offset(cal_set, cal_read * self.VAccDiv)
        return analyse_readback(cal_set, cal_read * self.VAccDiv, order=self.Cal_order)

    def _fit_cal(self, set_v: pd.Series, read: pd.Series) -> tuple[list[float], list[float]]:
        """
        Polynomial fit of Read vs Set at Cal_order, in ascending order (Cal[0] is the
        intercept); errors are the covariance diagonal (variances).

        Too few points for the covariance give NaN errors; too few for the fit itself give the
        nominal calibration (applied voltage = set voltage), both with a CalibrationWarning.
        """
        order = self.Cal_order
        n = len(set_v)
        try:
            if n > order + 1:  # np.polyfit(cov=True) needs n > deg + 1
                values, cov = np.polyfit(set_v, read, order, cov=True)
                return list(values[::-1]), list(np.diag(cov)[::-1])
            if n > order:
                warn(
                    f"run {self.run_number}: {n} calibration points are too few for the "
                    f"uncertainties of an order-{order} fit; Cal_err is NaN"
                )
                return list(np.polyfit(set_v, read, order)[::-1]), [np.nan] * (order + 1)
        except np.linalg.LinAlgError:
            pass
        warn(
            f"run {self.run_number}: no order-{order} calibration fit is possible on {n} point"
            f"{'s' if n != 1 else ''}; the nominal calibration (applied = set voltage) is used"
        )
        return [0.0, 1.0 / self.VAccDiv] + [0.0] * (order - 1), [np.nan] * (order + 1)

    def Update_V_divisions(
        self, VAccDiv: int = 1000, VCoolDiv: int = 10000, VcoolOffset: float = 0
    ) -> None:
        """
        Update voltage division settings for data processing.

        Parameters
        ----------
        VAccDiv : int, optional
            LCR voltage monitor scaling factor (default is 1000).
        VCoolDiv : int, optional
            Cooler voltage monitor scaling factor (default is 10000).
        VcoolOffset : float, optional
            Cooler voltage offset in volts (default is 0).
        """
        self.VAccDiv = VAccDiv
        self.VCoolDiv = VCoolDiv
        self.VCoolOffset = VcoolOffset
        return
