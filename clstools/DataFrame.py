import dask.dataframe as dd
import pandas as pd
import numpy as np
from numpy import sqrt
import time
import math

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
    
    def __init__(self,VAccDiv = 1000,VCoolDiv = 10000 ):
        self.VCoolDiv = VCoolDiv
        self.VAccDiv = VAccDiv
        self.Vcool_init = None
        self.Laser_set = None
        self.Laser_ref = None
        self.Step_Size = None
        self.Cal_df = None
        self.Cal = []
        self.Cal_err = []
        self.Run = None
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
        self.LoadingTime = 0
        self.ComputationVTime = 0
        self.ComputationWLTime = 0
        self.ComputationBinTime = 0
    
    def Info(self):

        print("\n")
        print("----------------------------------------------------")
        print("     Run number  -> ",self.run_number)
        print("----------------------Settings----------------------")
        print("     Cooler voltage monitor scaling -> ",self.VCoolDiv)
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
 
        self.Run['V'] = self.Run["Vrfq"]*self.VCoolDiv - self.Run["DV_cal"]
        
        
        self.Sorted = self.Run.compute()
        self.Size_sorted = len(self.Sorted)
        self.ComputationVTime = time.time()-start
        # self.data.DAQTime = (self.data.Run['TS'].max().compute() - self.data.Run['TS'].min().compute())*1.0e-6
        # self.data.Run.compute()
        # self.data.Scans = round(self.data.Run['Bunch'].max()/(self.data.Bin+1))

    def Compute_WL(self,Mass,ref,harmonic = 2):
        start = time.time()
        self.Mass = Mass
        self.Laser_ref = ref
        self.Run["WN"] = self.dopplershift(harmonic*self.Laser_set,self.Run["V"],self.Mass,collinear=False,rest_to_lab=False)
        self.Run["F"]  = (self.WN_to_f*self.Run["WN"]-ref)/1.0e6
        self.Sorted = self.Run.compute()
        self.Size_sorted = len(self.Sorted)
        self.ComputationWLTime = time.time()-start

    def Compute_Bins(self,TOF_gate = None, V_gate = None, F_gate= None):
        start = time.time()
        self.Run["counts"] = 1
        tmp = self.Run
        if TOF_gate != None:
            tmp = tmp[tmp.TOF<max(TOF_gate)][tmp.TOF>min(TOF_gate)]

        if V_gate != None:  
            
            tmp = tmp[tmp.DV<max(V_gate)][tmp.DV>min(V_gate)]
        
        if F_gate != None:  
            
            tmp = tmp[tmp.F<max(F_gate)][tmp.F>min(F_gate)]

        tmp = tmp[["F","counts"]].groupby('F').sum()
        tmp = tmp.compute()

        self.ComputationBinTime = time.time()-start

        return tmp.index.to_list(), tmp.values.tolist()

    
    def Load_Run(self,dir,run,cal_order = 1,blocksize=25e6):
        start = time.time()

        self.blocksize = blocksize
        self.dir = dir 
        self.run_number = str(run) 
        self.Cal_order = cal_order
        self.run_filename = dir+"/run_"+str(run)+".csv"
        self.calib_filename = dir+"/calib_"+str(run)+".csv"
        
        #Read cooler and Laser set
        file = open(self.calib_filename, 'rt',)
        conf = file.readlines(100)  # Returns a list object
       
        self.Vcool_init = float(conf[0].split('Cooler:')[1])
        self.Laser_set = float(conf[1].split('Laser:')[1])
        file.close()
    
        self.Cal_df = pd.read_csv(self.calib_filename, skiprows = 2, names=["Set","Read"],skip_blank_lines = True)

        self.Step_Size = abs(self.Cal_df.at[1, 'Set']-self.Cal_df.at[0, 'Set'])
                
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

        self.Run = dd.read_csv(self.run_filename, blocksize=self.blocksize,names=["TS","DV","Bunch","TDC","TOF","Vrfq"],skip_blank_lines = True)  # reading in 25MB chunks

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