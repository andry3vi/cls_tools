from typing import Union, List, Optional
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import asdf
import datetime

class CLSDataFrame:

    e = 1.602176634e-19 #C
    c = 299792458 #m/s
    u = 931.4941024	#MeV/C^2
    mu = 1.66053904 * 10**(-27) #kg

    WN_to_f = 1e2*c 

    def dopplerfactor(self,voltage: Union[float, np.ndarray], mass: float, collinear: bool = True, rest_to_lab: bool = True) -> Union[float,np.ndarray]:
        """
        Calculate the relativistic Doppler factor for a particle given its acceleration voltage and mass.

        Parameters
        ----------
        voltage : float or np.ndarray
            Acceleration voltage in volts.
        mass : float
            Particle mass in atomic mass units (amu).
        collinear : bool, optional
            If True, assumes collinear geometry (default is True).
        rest_to_lab : bool, optional
            If True, calculates the Doppler factor from rest frame to laboratory frame (default is True).

        Returns
        -------
        float or np.ndarray
            The relativistic Doppler factor. If `collinear` and `rest_to_lab` are both True or both False,
            returns the factor for transformation from rest to lab frame. Otherwise, returns the inverse factor.
        """
        m = mass * self.mu
        beta = np.sqrt(1 - (m**2 * self.c**4)/(self.e*voltage + m*self.c**2)**2)
        factor = (1+beta)/np.sqrt(1-beta**2)
        if (collinear and rest_to_lab) or (not collinear and not rest_to_lab):
            return factor
        else:
            return 1/factor

    def dopplershift(self, frequency: Union[float, np.ndarray], voltage: Union[float, np.ndarray], mass: float, collinear: bool = True, rest_to_lab: bool = True) -> Union[float, np.ndarray]:
        """
        Calculate the Doppler-shifted frequency for a particle given its acceleration voltage and mass.

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
        self.Vcool_init = None
        self.Laser_set = None
        self.Reference = None
        self.Step_Size = None
        self.ScanningRanges = None
        self.Cal_df = None
        self.Cal = []
        self.Cal_err = []
        self.Cal_order = None
        self.Run = None
        self.Binned = None
        self.ToF_binned = None
        self.Raw_binned = None
        self.Size = None
        self.Sorted = None
        self.Bins = None
        self.DAQTStime = None
        self.TSstart = None
        self.TSstop = None
        self.Scans = None
        self.Harmonic = 2
        self.Dwell_Time = None
        self.Experiment = None
        self.Date = None
        self.Frequency_stepsize = None
        self.LoadingTime = 0
        self.ComputationVTime = 0
        self.ComputationWLTime = 0
        self.ComputationBinTime = 0
    
    def Info(self) -> None:
        """
        Display comprehensive information about the loaded CLS data run.

        This method prints detailed information including run parameters, voltage settings,
        calibration data, scanning ranges, timing information, and computation times.
        """
        print("\n")
        print("-----------------------------------------------------")
        print("     Run number  -> ",self.run_number)
        print("     Experiment  -> ",self.Experiment)
        print("     Date        -> ",self.Date)
        print("     Filename    -> ",self.run_filename)
        print("-----------------V-division settings-----------------")
        print("     Cooler voltage monitor scaling -> ",self.VCoolDiv)
        print("     Cooler voltage offset          -> ",self.VCoolOffset)
        print("     LCR voltage monitor scaling    -> ",self.VAccDiv)
        print("---------------------Calibration---------------------")
        print("     Initial Cooler Voltage [V] -> ",self.Vcool_init*self.VCoolDiv)
        print("     Laser Setpoint      [cm-1] -> ",self.Laser_set)
        print("     Calibration [p0 p1 p2 ...] -> ", [self.VAccDiv*i for i in self.Cal])
        print("     Calibration [e0 e1 e2 ...] -> ", [self.VAccDiv*i for i in self.Cal_err])
        if self.Dropped_calibration_points is not None:
            print("     Outlier calibration points found [V] -> ", self.Dropped_calibration_points)
        print("-------------------Scanning Ranges-------------------")
        print("     Voltage Step Size        [V] -> ",self.Step_Size)
        print("     Frequency Step Size    [MHz] -> ",self.Frequency_stepsize/1e6)
        for range in self.ScanningRanges:
            print("         from {} V to {} V".format(range[0],range[1]))
        print("--------------------General info---------------------")
        print("     Entries              -> ",self.Size)
        print("     DAQ Time         [s] -> ",self.DAQTStime)  
        print("     start TS [Unix Time] -> ",self.TSstart)
        print("     stop  TS [Unix Time] -> ",self.TSstop)
        print("     start date           -> ",datetime.datetime.fromtimestamp(self.TSstart))  
        print("     stop date            -> ",datetime.datetime.fromtimestamp(self.TSstop))  
        print("----------------------------------------------------")
        print("     Loading time                [s] -> ",self.LoadingTime)
        print("     Voltage Computation time    [s] -> ",self.ComputationVTime)
        print("     Wavelenght Computation time [s] -> ",self.ComputationWLTime)
        print("     Binning Computation time    [s] -> ",self.ComputationBinTime)
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
                'VCoolDiv':[self.VCoolDiv], 
                'VAccDiv':[self.VAccDiv], 
                'VCoolOffset':[self.VCoolOffset], 
                'Vcool_init':[self.Vcool_init], 
                'DAQTStime':[self.DAQTStime], 
                'TSstart':[self.TSstart], 
                'TSstop':[self.TSstop], 
                'Laser_set':[self.Laser_set], 
                'Harmonic':[self.Harmonic], 
                'Dwell_Time':[self.Dwell_Time], 
                'Step_Size':[self.Step_Size], 
                'Frequency_stepsize':[self.Frequency_stepsize], 
                'Experiment':[self.Experiment], 
                }
        for ix,range in enumerate(self.ScanningRanges):
            output['Scan_range_low_{}'.format(ix)] = [min(range)]
            output['Scan_range_high_{}'.format(ix)] = [max(range)]
        
        output['Cal_order'] = [self.Cal_order]
        for ix,C in enumerate(zip(self.Cal,self.Cal_err)):
            output['Cal_p{}'.format(ix)] = [C[0]]
            output['Cal_dp{}'.format(ix)] = [C[1]]

        df_out = pd.DataFrame(output)
        df_out.to_csv(filename,index=False)
        pass

    def Compute_Voltages(self, cooler_correction: str = 'pbp') -> None:
        """
        Compute calibrated voltages using polynomial calibration and cooler voltage correction.

        Parameters
        ----------
        cooler_correction : str, optional
            Type of cooler voltage correction to apply. Options are:
            - 'pbp': Point-by-point correction (default)
            - 'mean': Use mean cooler voltage for all points

        Raises
        ------
        AttributeError
            If cooler_correction parameter is not 'pbp' or 'mean'.
        """
        start = time.time()
        # self.Run["DV_cal"]=(self.Run["DV"]+(random()-0.5)*self.data.Step_Size)*self.Cal_m+self.Cal_q
        if self.Cal_order == 1:
            self.Run["DV_cal"] = (self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
        elif self.Cal_order == 2:
            self.Run["DV_cal"] = (self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
        elif self.Cal_order == 3:
            self.Run["DV_cal"] = (self.Cal[3]*self.Run["DV"]**3+self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv

        # if cooler_correction in ['ptp','mean','linear']:
            
        if cooler_correction == 'pbp':
            self.Run['V'] = self.Run["Vrfq"]*self.VCoolDiv+self.VCoolOffset - self.Run["DV_cal"]
        elif cooler_correction == 'mean':
            self.Run['V'] = self.Run["Vrfq"].mean().compute()*self.VCoolDiv+self.VCoolOffset - self.Run["DV_cal"]
        else:
            raise AttributeError("Error in cooler_correction parameter. Use one between pbp and mean")        
        
        self.Sorted = self.Run.compute()
        self.ComputationVTime = time.time()-start
        
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
        self.Frequency_stepsize = np.abs(self.dopplershift(harmonic*self.Laser_set,self.Vcool_init*self.VCoolDiv,self.Mass,collinear=False,rest_to_lab=False)
                                        -self.dopplershift(harmonic*self.Laser_set,self.Vcool_init*self.VCoolDiv+self.Step_Size,self.Mass,collinear=False,rest_to_lab=False))
        self.Frequency_stepsize = self.Frequency_stepsize*self.WN_to_f
        self.Run["WN"] = self.dopplershift(harmonic*self.Laser_set,self.Run["V"],self.Mass,collinear=False,rest_to_lab=False)
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"])-self.Reference
        self.Sorted = self.Run.compute()

        self.ComputationWLTime = time.time()-start

        return
    def Frequency_ranges(self, Mass: float, ref: float = 0, harmonic: int = 2) -> tuple:
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
        
        return: tuple
            list of ranges in frequency value [MHz]
        """
        self.Mass = Mass
        self.Reference = ref
        self.Harmonic = harmonic

        returnlist = []
        for range in self.ScanningRanges:
            maxV = self.Vcool_init*self.VCoolDiv+self.VCoolOffset - max(range)
            minV = self.Vcool_init*self.VCoolDiv+self.VCoolOffset - min(range)
            WN_min = self.dopplershift(harmonic*self.Laser_set,maxV,self.Mass,collinear=False,rest_to_lab=False)
            WN_max = self.dopplershift(harmonic*self.Laser_set,minV,self.Mass,collinear=False,rest_to_lab=False)
            returnlist.append([((self.WN_to_f*WN_min)-self.Reference)/1e6,((self.WN_to_f*WN_max)-self.Reference)/1e6])


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
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"])-self.Reference
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

        if filter_window>0:
            tmp = tmp.compute()
            tmp['filter'] = True
            tmp['filter'] = tmp.groupby("TS")['filter'].transform(lambda x: (False if x.size>filter_window else True))
            tmp = tmp[tmp['filter']]
            self.Run = dd.from_pandas(tmp,npartitions=6)
        
        return
    
    def Compute_ToF(self, V_gate: Optional[List[float]] = None, PMT_gate: Optional[List[int]] = None) -> None:
        """
        Compute time-of-flight binned data with optional gating.

        Parameters
        ----------
        V_gate : list of float, optional
            Voltage gate range [min_voltage, max_voltage]. If None, no voltage gating is applied.
        PMT_gate : list of int, optional
            List of PMT channels to include (1-4). If None, all PMT channels are included.
        """
        tmp = self.Run[['TOF','DV','TDC']]
        tmp["counts"] = 1
        if V_gate != None:  
            
            tmp = tmp[tmp.DV<max(V_gate)]
            tmp = tmp[tmp.DV>min(V_gate)]
            
        if PMT_gate != None:
            PMTS = [1,2,3,4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[tmp.TDC != pmt]
        tmp = tmp[["TOF","counts"]].groupby('TOF').sum()
        self.ToF_binned = tmp.compute()

        return

    def Compute_Bins(self, TOF_gate: Optional[List[float]] = None, V_gate: Optional[List[float]] = None, F_gate: Optional[List[float]] = None, PMT_gate: Optional[List[int]] = None, bins: Optional[int] = None) -> None:
        """
        Compute frequency-binned data with optional gating and custom binning.

        Parameters
        ----------
        TOF_gate : list of float, optional
            Time-of-flight gate range [min_tof, max_tof]. If None, no ToF gating is applied.
        V_gate : list of float, optional
            Voltage gate range [min_voltage, max_voltage]. If None, no voltage gating is applied.
        F_gate : list of float, optional
            Frequency gate range [min_frequency, max_frequency]. If None, no frequency gating is applied.
        PMT_gate : list of int, optional
            List of PMT channels to include (1-4). If None, all PMT channels are included.
        bins : int, optional
            Number of frequency bins to use. If None, automatically calculated from frequency step size.
        """
        start = time.time()
        tmp = self.Run[['TS','F','TOF','DV','TDC','V']]
       
        if TOF_gate != None:
            
            tmp = tmp[tmp.TOF<max(TOF_gate)]
            tmp = tmp[tmp.TOF>min(TOF_gate)]

        if V_gate != None:  
            
            tmp = tmp[tmp.DV<max(V_gate)]
            tmp = tmp[tmp.DV>min(V_gate)]

        if F_gate != None:  
            
            tmp = tmp[tmp.F<max(F_gate)]
            tmp = tmp[tmp.F>min(F_gate)]

        if PMT_gate != None:
            PMTS = [1,2,3,4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[tmp.TDC != pmt]
        
        self.Binned = tmp.compute()
        maxF = self.Binned['F'].max()
        minF = self.Binned['F'].min()
        indexedbins = None
        if bins != None:
            indexedbins = bins
        else:
            indexedbins = int((maxF-minF)/self.Frequency_stepsize)
           

        self.Binned['bins'] = pd.cut(x=self.Binned['F'],bins=indexedbins)
        self.Binned = self.Binned.groupby(by='bins',as_index=False,observed=False).aggregate({'F': ['count','mean', 'std'],'V':['mean','std']}).pipe(lambda x: x.set_axis(x.columns.map(''.join), axis=1))
        self.Binned = self.Binned.sort_values(by=['bins'])
        self.Binned["bins_center"] =self.Binned["bins"].apply(lambda x: x.mid).astype(float)
        self.Binned["bins_width"] =self.Binned["bins"].apply(lambda x: x.length).astype(float)
        self.Binned['Fmean'] = self.Binned[['Fmean','bins_center']].apply(lambda x: x['bins_center'] if np.isnan(x['Fmean']) else x['Fmean'],axis=1)
        self.Binned['Fstd'] = self.Binned[['Fstd','bins_width']].apply(lambda x: x['bins_width']*0.5 if np.isnan(x['Fstd']) else x['Fstd'],axis=1)
        self.ComputationBinTime = time.time()-start

        return

    def Compute_Raw_Bins(self, TOF_gate: Optional[List[float]] = None, V_gate: Optional[List[float]] = None, PMT_gate: Optional[List[int]] = None) -> None:
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
        tmp = self.Run[['TOF','DV','TDC']]
        tmp["counts"] = 1
        if TOF_gate != None:
            
            tmp = tmp[tmp.TOF<max(TOF_gate)]
            tmp = tmp[tmp.TOF>min(TOF_gate)]

        if V_gate != None:  
            
            tmp = tmp[tmp.DV<max(V_gate)]
            tmp = tmp[tmp.DV>min(V_gate)]

        if PMT_gate != None:
            PMTS = [1,2,3,4]
            excluded = [i for i in PMTS if i not in PMT_gate]
            for pmt in excluded:
                tmp = tmp[tmp.TDC != pmt]

        tmp = tmp[["DV","counts"]].groupby('DV').sum()
        self.Raw_binned = tmp.compute()

        return

    def Load_Run(self, filename: str, cal_order: int = 1, blocksize: float = 25e6, filter_calibration: bool = True) -> None:
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
        filter_calibration : bool, optional
            Whether to filter calibration points based on residuals. Default is True.
        """
        start = time.time()

        self.blocksize = blocksize
        self.Cal_order = cal_order
        self.run_filename = filename

        with asdf.open(self.run_filename) as af:
                
            self.run_number = af.tree['Run'] 
            self.Vcool_init = af.tree['CoolerVoltage']
            self.Laser_set = af.tree['LaserSetpoint']
            self.Dwell_Time = af.tree['DwellTime']
            self.Experiment = af.tree['Experiment']
            self.Date = af.tree['Date']
            self.Step_Size = af.tree['StepSize']
            self.ScanningRanges = af.tree['ScanningRanges']

            cal = [[set,read] for set,read in zip(af['CalSet'],af['CalReadback'])]
            self.Cal_df = pd.DataFrame(cal, columns=["Set","Read"])
            # self.Cal_df = pd.DataFrame({"Set":af['CalSet'], "Read":af['CalReadback']})

            values, cov = np.polyfit(self.Cal_df['Set'], self.Cal_df['Read'], self.Cal_order,cov = True)  
            

            self.Cal = []
            self.Cal_err = []
            for i,v in enumerate(values):
                self.Cal.append(v)
                self.Cal_err.append(cov[i,i])
            self.Cal.reverse()
            self.Cal_err.reverse()

            self.Cal_df['Fit'] = self.Cal_df['Set'].apply(lambda x: np.polyval(self.Cal[::-1], x))
            self.Cal_df['Residual'] = self.Cal_df['Read'] - self.Cal_df['Fit']

            if filter_calibration:
                # if self.Cal_order == 1:
                #     self.Run["DV_cal"] = (self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
                # elif self.Cal_order == 2:
                #     self.Run["DV_cal"] = (self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
                # elif self.Cal_order == 3:
                #     self.Run["DV_cal"] = (self.Cal[3]*self.Run["DV"]**3+self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv

                std_residual = self.Cal_df['Residual'].std()
                menan_residual = self.Cal_df['Residual'].mean()
                self.Cal_df_filtered = self.Cal_df[np.abs(self.Cal_df['Residual']) <= 2 * std_residual]
                indexes_to_drop = self.Cal_df.index.difference(self.Cal_df_filtered.index)
                self.Dropped_calibration_points = None
                if self.Cal_df_filtered.size != self.Cal_df.size:
                    # self.Cal_df_filtered = self.Cal_df_filtered.reset_index(drop=True)
                    print(f"Warning, calibration points outside 2 sigma of the residuals have been filtered out.")
                    print(f"Mean of voltage calibration residuals: {menan_residual:.5f}, Standard deviation of residuals: {std_residual:.5f}")
                    print(f"{len(indexes_to_drop)} point{('s' if len(indexes_to_drop) > 1 else '')} removed.")
                    # print(f"indexes of removed points: {list(indexes_to_drop)}")
                    self.Dropped_calibration_points = self.Cal_df.loc[indexes_to_drop, 'Set'].tolist()
                    print(f"Removed point{('s' if len(indexes_to_drop) > 1 else '')} [V]: {self.Dropped_calibration_points}")
                    values, cov = np.polyfit(self.Cal_df_filtered['Set'], self.Cal_df_filtered['Read'], self.Cal_order,cov = True)  



                    self.Cal = []
                    self.Cal_err = []
                    for i,v in enumerate(values):
                        self.Cal.append(v)
                        self.Cal_err.append(cov[i,i])
                    self.Cal.reverse()
                    self.Cal_err.reverse()

                    self.Cal_df = self.Cal_df_filtered
                    self.Cal_df['Fit'] = self.Cal_df['Set'].apply(lambda x: np.polyval(self.Cal[::-1], x))
                    self.Cal_df['Residual'] = self.Cal_df['Read'] - self.Cal_df['Fit']


            self.Run = dd.from_array(np.array(af.tree['raw']),columns=["TS","DV","Bunch","TDC","TOF","Vrfq"])
        
        self.TSstart = self.Run['TS'].min().compute()
        self.TSstop = self.Run['TS'].max().compute()
        
        self.DAQTStime = self.TSstop-self.TSstart
        self.Size = len(self.Run)

        self.LoadingTime = time.time()-start
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

        values, cov = np.polyfit(self.Cal_df['Set'], self.Cal_df['Read'], self.Cal_order,cov = True)  

        self.Cal = []
        self.Cal_err = []
        for i,v in enumerate(values):
            self.Cal.append(v)
            self.Cal_err.append(cov[i,i])
        self.Cal.reverse()
        self.Cal_err.reverse()

        return

    def Update_V_divisions(self, VAccDiv: int = 1000, VCoolDiv: int = 10000, VcoolOffset: float = 0) -> None:
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