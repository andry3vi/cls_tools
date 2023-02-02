from .DataFrame import CLSDataFrame
import pandas as pd 
import math
from scipy import stats
from random import seed
from random import random
import numpy as np
# seed random number generator
# seed(1)

class Importer:

    VCoolDiv = 10000
    VAccDiv = 1000

    def __init__(self,dir,run,data):
        self.dir = dir 
        self.run = str(run) 
        self.data = data
        self.run_filename = dir+"/run_"+str(run)+".csv"
        self.calib_filename = dir+"/calib_"+str(run)+".csv"
        # print("hello importer")
        self.loadCal()
        self.loadData()
        self.fillAccVoltage()
    
    def loadCal(self):
        #Read cooler and Laser set
        file = open(self.calib_filename, 'r')
        conf = file.readlines()  # Returns a list object
        self.data.Vcool_init = self.VCoolDiv*float(conf[0][7:])
        self.data.Laser_set = float(conf[1][6:])
        file.close()
    
        df = pd.read_csv(self.calib_filename, skiprows = 2, names=["Set","Read"],skip_blank_lines = True)

        self.data.Step_Size = abs(df.at[1, 'Set']-df.at[0, 'Set'])

        df["Read"] = df["Read"]*self.VAccDiv
        
        result = stats.linregress(df["Set"],df["Read"])
        
        # coeffs = np.polyfit(df['Set'], df['Read'], 2)
        # self.Cal_a = coeffs[2]
        # self.Cal_b = coeffs[1]
        # self.Cal_c = coeffs[0]

        self.data.Cal_m = result.slope
        self.data.Cal_q = result.intercept

        self.data.Cal_m_stderr = result.stderr
        self.data.Cal_q_stderr = result.intercept_stderr

        self.data.Set_V = df["Set"].tolist()

        self.data.Max = df["Set"].max()
        self.data.Min = df["Set"].min()

        self.data.Bin = math.floor((self.data.Max - self.data.Min)/self.data.Step_Size)

    def loadData(self):
        self.data.Run = pd.read_csv(self.run_filename,names=["TS","DV","Bunch","TDC","TOF","Vrfq"],skip_blank_lines = True)
        self.data.Size = len(self.data.Run)

        self.data.DAQTime = (self.data.Run["TS"].max() - self.data.Run["TS"].min())*1.0e-6
        self.data.Scans = round(self.data.Run["Bunch"].max()/(self.data.Bin+1))


    def fillAccVoltage(self):
        # self.data.Run["DV_cal"]=(self.data.Run["DV"]+(random()-0.5)*self.data.Step_Size)*self.data.Cal_m+self.data.Cal_q
        self.data.Run["DV_cal"]=self.data.Run["DV"]*self.data.Cal_m+self.data.Cal_q

        tmp = dict()
        tmp["TS"] = list()
        tmp["V"] = list()
        tmp["DV"] = list()
        tmp["Vrfq"] = list()
        tmp["TOF"] = list()
        tmp["TDC"] = list()

        for i in range(self.data.Size):

            tmp["TS"].append(self.data.Run["TS"][i])
            tmp["V"].append(self.data.Run["Vrfq"][i]*self.VCoolDiv - self.data.Run["DV_cal"][i])
            # tmp["V"].append(self.data.Run["Vrfq"][i]*self.VCoolDiv - self.data.Cal[self.data.Run["DV"][i]])
            tmp["Vrfq"].append(self.data.Run["Vrfq"][i]*self.VCoolDiv)
            tmp["DV"].append(self.data.Run["DV"][i])
            tmp["TOF"].append(self.data.Run["TOF"][i])
            tmp["TDC"].append(self.data.Run["TDC"][i])
        
        self.data.Sorted = pd.DataFrame(data=tmp)
        self.data.Size_sorted = len(tmp["TS"])


