import datetime
import time
from collections.abc import Callable

import asdf
import dask.dataframe as dd
import numpy as np
import pandas as pd

from .calibration import (
    COOLER_MAX_DEV_V,
    COOLER_WINDOW_S,
    DATAFILE_ISSUES,
    SCAN_MODELS,
    ReadbackFit,
    ScanCalibration,
    analyse_readback,
    cooler_readings,
    faulty_readings,
    load_table,
    select_table,
    smooth_readings,
    warn,
)


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
        self.Readback_fit: ReadbackFit | None = None
        self.Cooler_correction: str | None = None
        self.Cooler_faulty_readings = 0
        self.Run: dd.DataFrame = None  # type: ignore[assignment]
        self.Binned: pd.DataFrame = None
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
            print("     Measured on             -> ", self.Scan_calibration.measured)
            print(
                "     Run offset          [V] -> ",
                f"{self.Scan_offset_v:+.4f} "
                f"(readback fit {fit.offset_v:+.4f} +- {fit.offset_err_v:.4f})",
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
            print("     Cooler correction       -> ", self.Cooler_correction)
            print("     Faulty cooler readings  -> ", self.Cooler_faulty_readings)
        print("-------------------Scanning Ranges-------------------")
        print("     Voltage Step Size        [V] -> ", self.Step_Size)
        print("     Frequency Step Size    [MHz] -> ", self.Frequency_stepsize / 1e6)
        for range in self.ScanningRanges:
            print(f"         from {range[0]} V to {range[1]} V")
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
            output["Readback_offset_err_v"] = [self.Readback_fit.offset_err_v]
            output["Readback_rms_v"] = [self.Readback_fit.residual_rms_v]
            output["Readback_gain_ppm"] = [self.Readback_fit.gain_ppm]
            output["Readback_lag_steps"] = [self.Readback_fit.lag_steps]
            output["Readback_rejected"] = [len(self.Readback_fit.rejected_set_v)]
        if self.Cooler_correction is not None:
            output["Cooler_correction"] = [self.Cooler_correction]
            output["Cooler_faulty_readings"] = [self.Cooler_faulty_readings]

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
        self.Readback_fit = None

    def Use_Scan_Calibration(
        self,
        table: str | ScanCalibration = "auto",
        model: str = "lut",
        offset_v: float | None = None,
    ) -> ReadbackFit:
        """
        Calibrate the scan voltage with a bench-measured table instead of the per-run polynomial.

        The table holds the scan HV's shape including the DAC nonlinearity; only its offset is
        fitted to this run's CalSet/CalReadback, after correcting a readback lag and dropping
        unsettled readings:  applied(DV) = divider_scale * (shape(DV) + offset). Call after
        Load_Run and before Compute_Voltages. Problems are reported as CalibrationWarning.

        Parameters
        ----------
        table : str or ScanCalibration, optional
            'auto' (default) picks the packaged table measured most recently before the run;
            otherwise a packaged table id, a path to a table file, or a ScanCalibration.
            A run taken before the table's measurement date gets a warning.
        model : str, optional
            'lut' (default): the table on its grid, its cubic fit off the grid. 'cubic': the cubic
            everywhere, i.e. without the DAC nonlinearity (for comparisons).
        offset_v : float, optional
            Use this run offset [V] instead of the readback fit, e.g. when the run's readback is
            known to be wrong. The fit is still made and returned as a diagnostic.

        Returns
        -------
        ReadbackFit
            The readback fit against the table (also stored as Readback_fit).
        """
        if model not in SCAN_MODELS:
            raise ValueError(f"unknown scan model {model!r}, choose from {SCAN_MODELS}")
        run_date = datetime.date.fromtimestamp(self.TSstart)
        if isinstance(table, ScanCalibration):
            cal = table
        elif table == "auto":
            cal = select_table(run_date)
        else:
            cal = load_table(table)
        if run_date < cal.measured:
            warn(
                f"run {self.run_number} ({run_date}) predates scan calibration table {cal.id}, "
                f"measured on {cal.measured}: the scan HV is assumed unchanged since the run"
            )

        fit = cal.fit_run_offset(
            self.Cal_raw["Set"].to_numpy(float),
            self.Cal_raw["Read"].to_numpy(float) * self.VAccDiv,
            model,
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
        elif np.isfinite(fit.offset_v):
            offset = fit.offset_v
        else:
            offset = cal.bench_offset_v
            warn(
                f"run {self.run_number}: no usable calibration readback, the bench offset of "
                f"table {cal.id} ({offset:+.3f} V) is used"
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

        self.Set_Scan_Voltage_Map(cal.scan_map(offset, model))
        self.Scan_calibration = cal
        self.Scan_model = model
        self.Scan_offset_v = offset
        self.Readback_fit = fit
        return fit

    def _check_cooler_faults(self, max_dev_v: float | None) -> None:
        """Count (and warn about) faulty cooler readings in self.Sorted; nothing is dropped."""
        if max_dev_v is None or not {"TS", "Vrfq"} <= set(self.Sorted.columns):
            return
        ev = self.Sorted[["TS", "Vrfq"]].sort_values("TS", kind="stable")
        _, v_r = cooler_readings(ev["TS"].to_numpy(), ev["Vrfq"].to_numpy())
        bad = faulty_readings(v_r * self.VCoolDiv, max_dev_v)
        self.Cooler_faulty_readings = int(bad.sum())
        if bad.any():
            warn(
                f"run {self.run_number}: {int(bad.sum())} cooler readings are further than "
                f"{max_dev_v:g} V from the run median (e.g. {v_r[bad][0] * self.VCoolDiv:.1f} V); "
                f"cooler_correction='{self.Cooler_correction}' uses them as recorded, "
                "'smooth' drops them"
            )

    def _smoothed_cooler(self, window_s: float, max_dev_v: float | None) -> dd.Series:
        """Per-event cooler readback (Vrfq units): faulty readings dropped, the rest averaged over
        window_s and interpolated to each event's time."""
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
        v_s = smooth_readings(t_r, v_r, window_s)
        smoothed: dd.Series = self.Run["TS"].map_partitions(
            lambda ts: pd.Series(np.interp(ts.to_numpy(float), t_r, v_s), index=ts.index),
            meta=("Vrfq_smooth", "f8"),
        )
        return smoothed

    def Compute_Voltages(
        self,
        cooler_correction: str = "pbp",
        cooler_window_s: float = COOLER_WINDOW_S,
        cooler_max_dev_v: float | None = COOLER_MAX_DEV_V,
    ) -> None:
        """
        Compute calibrated voltages using the scan calibration and cooler voltage correction.

        The scan voltage comes from the map installed by Use_Scan_Calibration or
        Set_Scan_Voltage_Map if there is one, otherwise from the CalSet/CalReadback polynomial.

        Parameters
        ----------
        cooler_correction : str, optional
            Type of cooler voltage correction to apply. Options are:
            - 'pbp': Point-by-point correction (default)
            - 'mean': Use mean cooler voltage for all points
            - 'smooth': drop faulty cooler readings, average the rest over cooler_window_s
              (centred) and interpolate to each event's time. The DAQ's ~0.1 s short-aperture
              readings alias the 50 Hz ripple into a ~2.4 s oscillation the events cannot
              follow; 5 s removes it and keeps the slow drift. Adds the column 'Vrfq_smooth'.
        cooler_window_s : float, optional
            Averaging window for 'smooth' [s] (default 5).
        cooler_max_dev_v : float or None, optional
            Cooler readings further than this from the run median [V at the cooler] are readback
            faults (default 5 V). 'smooth' drops them; 'pbp' and 'mean' only warn. None disables
            the check.

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

        return

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
        table = select_table(run_date)
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
