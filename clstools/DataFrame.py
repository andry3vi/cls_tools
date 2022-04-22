import numpy as np
from numpy import sqrt

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
    
    def __init__(self):
        self.Vcool_init = None
        self.Laser_set = None
        self.Laser_ref = None
        self.Step_Size = None
        self.Cal_m = None
        self.Cal_q = None
        self.Cal_m_stderr = None
        self.Cal_q_stderr = None
        self.Run = None
        self.Size = None
        self.Sorted = None
        self.Size_sorted = None
        self.Max = None
        self.Min = None
        self.Bin = None
        self.DAQTime = None
        self.Set_V = None
        self.Step_Size = None
        self.Scans = None
    
    def info(self):
        print("Initial Cooler Voltage: ",self.Vcool_init)
        print("Laser Setpoint        : ",self.Laser_set)
        print("Voltage Step Size     : ",self.Step_Size)
        print("Entries               : ",self.Size)
        print("Reduced Entries       : ",self.Size_sorted)
        print("Calibration Slope     : ",self.Cal_m," +- ",self.Cal_m_stderr)
        print("Calibration Intercept : ",self.Cal_q," +- ",self.Cal_q_stderr)
        print("DAQ Time              : ",self.DAQTime)        
        print("Bins                  : ",self.Bin)
        print("Min                   : ",self.Min)
        print("Max                   : ",self.Max)
        print("Step Size             : ",self.Step_Size)
        print("Scans                 : ",self.Scans)
        

    def computeWL(self,Mass,ref):
        self.Mass = Mass
        self.Laser_ref = ref
        self.Sorted["WN"] = self.dopplershift(2*self.Laser_set,self.Sorted["V"],self.Mass)
        self.Sorted["F"]  = (self.WN_to_f*self.Sorted["WN"]-ref)/1.0e6
        # 2*self.Laser_set*