import dask.dataframe as dd
import pandas as pd
import numpy as np
from numpy import sqrt
import time
import math
import asdf
import datetime
class CLSDataFrame:

    e = 1.602176634e-19 #C
    c = 299792458 #m/s
    u = 931.4941024	#MeV/C^2
    mu = 1.66053904 * 10**(-27) #kg

    WN_to_f = 1e2*c 

    def dopplerfactor(self,voltage, mass, collinear = True, rest_to_lab = True):
        """Voltage in volts, mass in amu"""
        m = mass * self.mu
        beta = np.sqrt(1 - (m**2 * self.c**4)/(self.e*voltage + m*self.c**2)**2)
        factor = (1+beta)/np.sqrt(1-beta**2)
        if (collinear and rest_to_lab) or (not collinear and not rest_to_lab):
            return factor
        else:
            return 1/factor

    def dopplershift(self,frequency, voltage, mass, collinear = True, rest_to_lab = True):
        """Voltage in volts, mass in amu, works for frequency or wavenumber"""
        return frequency * self.dopplerfactor(voltage, mass, collinear, rest_to_lab)
    
    def __init__(self,VAccDiv = 1000,VCoolDiv = 10000, VCoolOffset = 0):
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
    
    def Info(self):

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

    def Compute_Voltages(self):

        start = time.time()
        # self.Run["DV_cal"]=(self.Run["DV"]+(random()-0.5)*self.data.Step_Size)*self.Cal_m+self.Cal_q
        if self.Cal_order == 1:
            self.Run["DV_cal"] = (self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
        elif self.Cal_order == 2:
            self.Run["DV_cal"] = (self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
        elif self.Cal_order == 3:
            self.Run["DV_cal"] = (self.Cal[3]*self.Run["DV"]**3+self.Cal[2]*self.Run["DV"]**2+self.Run["DV"]*self.Cal[1]+self.Cal[0])*self.VAccDiv
 
        self.Run['V'] = self.Run["Vrfq"]*self.VCoolDiv+self.VCoolOffset - self.Run["DV_cal"]
        # self.Run['V'] = 3*self.VCoolDiv+self.VCoolOffset - self.Run["DV_cal"]
        
        
        self.Sorted = self.Run.compute()
        self.ComputationVTime = time.time()-start
        
    def Compute_WL(self,Mass,ref=0,harmonic = 2):
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

    def Shift_Ref(self,ref=0):
        self.Reference = ref
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"])-self.Reference
        self.Sorted = self.Run.compute()

            
        return

    def apply_filter(self,filter_window=0):
        tmp = self.Run

        if filter_window>0:
            tmp = tmp.compute()
            tmp['filter'] = True
            tmp['filter'] = tmp.groupby("TS")['filter'].transform(lambda x: (False if x.size>filter_window else True))
            tmp = tmp[tmp['filter']]
            self.Run = dd.from_pandas(tmp,npartitions=6)
        
        return
    
    def Compute_ToF(self,V_gate = None, F_gate= None,PMT_gate = None):
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

    def Compute_Bins(self,TOF_gate = None, V_gate = None, F_gate= None, PMT_gate = None, bins=None):
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

    def Compute_Raw_Bins(self,TOF_gate = None, V_gate = None,PMT_gate = None): 
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

    def Load_Run(self,filename,cal_order = 1,blocksize=25e6):
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

            self.Run = dd.from_array(np.array(af.tree['raw']),columns=["TS","DV","Bunch","TDC","TOF","Vrfq"])
        
        self.TSstart = self.Run['TS'].min().compute()
        self.TSstop = self.Run['TS'].max().compute()
        
        self.DAQTStime = self.TSstop-self.TSstart
        self.Size = len(self.Run)

        self.LoadingTime = time.time()-start
        return

    def Update_Cal(self, cal_order = 1):

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

    def Update_V_divisions(self,VAccDiv = 1000,VCoolDiv = 10000, VcoolOffset = 0 ):
        self.VAccDiv = VAccDiv
        self.VCoolDiv = VCoolDiv
        self.VCoolOffset = VcoolOffset
        return