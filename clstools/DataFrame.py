import dask.dataframe as dd
import pandas as pd
import numpy as np
from numpy import sqrt
import time
import math
import asdf

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
        self.Laser_ref = None
        self.Step_Size = None
        self.Cal_df = None
        self.Cal = []
        self.Cal_err = []
        self.Run = None
        self.Binned = None
        self.ToF_binned = None
        self.Raw_binned = None
        self.Size = None
        self.Sorted = None
        self.Size_sorted = None
        self.Max = None
        self.Min = None
        self.Bin = None
        self.DAQTime = None
        self.Step_Size = None
        self.Scans = None
        self.Cal_order = None
        self.Harmonic = 2
        self.LoadingTime = 0
        self.VtoF_q = 0
        self.VtoF_m = 1
        self.ComputationVTime = 0
        self.ComputationWLTime = 0
        self.ComputationBinTime = 0
    
    def Info(self):

        print("\n")
        print("----------------------------------------------------")
        print("     Run number  -> ",self.run_number)
        print("----------------------Settings----------------------")
        print("     Cooler voltage monitor scaling -> ",self.VCoolDiv)
        print("     Cooler voltage offset          -> ",self.VCoolOffset)
        print("     LCR voltage monitor scaling    -> ",self.VAccDiv)
        print("------------------Calibration file------------------")
        print("     Initial Cooler Voltage    -> ",self.Vcool_init*self.VCoolDiv)
        print("     Laser Setpoint            -> ",self.Laser_set)
        print("     Calibration [p0 p1 p2 ...] -> ", [self.VAccDiv*i for i in self.Cal])
        print("     Calibration [e0 e1 e2 ...] -> ", [self.VAccDiv*i for i in self.Cal_err])
        print("     Voltage Step Size         -> ",self.Step_Size)
        print("     Voltage Bins              -> ",self.Bin)
        print("     Voltage Min               -> ",self.Min)
        print("     Voltage Max               -> ",self.Max)
        print("----------------------Run file----------------------")
        print("     Entries          -> ",self.Size)
        print("     Reduced Entries  -> ",self.Size_sorted)
        print("     DAQ Time         -> ",self.DAQTime)        
        print("     Scans            -> ",self.Scans)
        print("----------------------------------------------------")
        print("     Loading time                [s] -> ",self.LoadingTime)
        print("     Voltage Computation time    [s] -> ",self.ComputationVTime)
        print("     Wavelenght Computation time [s] -> ",self.ComputationWLTime)
        print("     Bin Computation time        [s] -> ",self.ComputationBinTime)
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
        
        
        self.Sorted = self.Run.compute()
        self.Size_sorted = len(self.Sorted)
        self.ComputationVTime = time.time()-start
        # self.data.DAQTime = (self.data.Run['TS'].max().compute() - self.data.Run['TS'].min().compute())*1.0e-6
        # self.data.Run.compute()
        # self.data.Scans = round(self.data.Run['Bunch'].max()/(self.data.Bin+1))

    def Compute_WL(self,Mass,ref=0,harmonic = 2, VtoF_cal=False):
        start = time.time()
        self.Mass = Mass
        self.Laser_ref = ref
        self.Harmonic = harmonic
        self.Run["WN"] = self.dopplershift(harmonic*self.Laser_set,self.Run["V"],self.Mass,collinear=False,rest_to_lab=False)
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"])/1.0e6-ref
        self.Sorted = self.Run.compute()
        self.Size_sorted = len(self.Sorted)

        if VtoF_cal:
            i_max = self.Sorted['F'].idxmax()
            i_min = self.Sorted['F'].idxmin()
            F = [self.Sorted.iloc[i_min]['F'],self.Sorted.iloc[i_max]['F']]
            V = [self.Sorted.iloc[i_min]['DV'],self.Sorted.iloc[i_max]['DV']]
            self.VtoF_m = (F[1]-F[0])/(V[1]-V[0])
            self.VtoF_q =  F[0]-self.VtoF_m*V[0]

        self.ComputationWLTime = time.time()-start

        return

    def VtoF(self,x):
        #Don't use this to convert voltage to frequency, it is only for a fast rough visualization purpose!!!!
        return x*self.VtoF_m + self.VtoF_q

    def FtoV(self,x):
        #Don't use this to convert voltage to frequency, it is only for a fast rough visualization purpose!!!!
        return (x-self.VtoF_q)/self.VtoF_m

    def Shift_Ref(self,ref=0,VtoF_cal=False):
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"])/1.0e6-ref
        self.Sorted = self.Run.compute()
        if VtoF_cal:
            i_max = self.Sorted['F'].idxmax()
            i_min = self.Sorted['F'].idxmin()
            F = [self.Sorted.iloc[i_min]['F'],self.Sorted.iloc[i_max]['F']]
            V = [self.Sorted.iloc[i_min]['DV'],self.Sorted.iloc[i_max]['DV']]
            self.VtoF_m = (F[1]-F[0])/(V[1]-V[0])
            self.VtoF_q =  F[0]-self.VtoF_m*V[0]
            
        return

    def Compute_ToF(self,V_gate = None, F_gate= None,PMT_gate = None):
        tmp = self.Run[['F','TOF','DV','TDC']]
        tmp["counts"] = 1
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

        tmp = tmp[["TOF","counts"]].groupby('TOF').sum()
        self.ToF_binned = tmp.compute()

        return

    def Compute_Bins(self,TOF_gate = None, V_gate = None, F_gate= None, PMT_gate = None, noise_filter = False):
        start = time.time()
        tmp = self.Run[['TS','F','TOF','DV','TDC']]
        tmp["counts"] = 1
        
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

        tmp = tmp[["F","counts"]].groupby('F').sum()
        
        self.Binned = tmp.compute()

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

    def Load_Run(self,dir,run,cal_order = 1,blocksize=25e6):
        start = time.time()

        self.blocksize = blocksize
        self.dir = dir 
        self.run_number = str(run) 
        self.Cal_order = cal_order
        self.run_filename = dir+"/run_"+str(run)+".asdf"

        with asdf.open(self.run_filename, copy_arrays=True) as af:
                            
            self.Vcool_init = af.tree['CoolerVoltage']
            self.Laser_set = af.tree['LaserSetpoint']
            
            cal = [[set,read] for set,read in zip(af['CalSet'],af['CalReadback'])]
        
            self.Cal_df = pd.DataFrame(cal, columns=["Set","Read"])

            self.Step_Size = af.tree['StepSize']
                    
            values, cov = np.polyfit(self.Cal_df['Set'], self.Cal_df['Read'], self.Cal_order,cov = True)  


            self.Cal = []
            self.Cal_err = []
            for i,v in enumerate(values):
                self.Cal.append(v)
                self.Cal_err.append(cov[i,i])
            self.Cal.reverse()
            self.Cal_err.reverse()

            self.Max = self.Cal_df["Set"].max()
            self.Min = self.Cal_df["Set"].min()

            self.Bin = math.floor((self.Max - self.Min)/self.Step_Size)

            self.Run = dd.from_array(np.array(af.tree['raw']),columns=["TS","DV","Bunch","TDC","TOF","Vrfq"])
        
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