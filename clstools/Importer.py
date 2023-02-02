from .DataFrame import CLSDataFrame
import dask.dataframe as dd
import pandas as pd 
import numpy as np
import math
import time


class Importer:


    def __init__(self,dir,run,data,cal_order = 1,blocksize=25e6):
        
        self.blocksize = blocksize
        self.dir = dir 
        self.run = str(run) 
        self.data = data
        self.data.Cal_order = cal_order
        self.run_filename = dir+"/run_"+str(run)+".csv"
        self.calib_filename = dir+"/calib_"+str(run)+".csv"
        self.loadCal()
        self.loadData()
    
    def loadCal(self):
        start = time.time()
        #Read cooler and Laser set
        file = open(self.calib_filename, 'rt',)
        conf = file.readlines(100)  # Returns a list object
        # tmp_str=conf[0].split('Cooler:','')
        # tmp_str=conf[0].split('Cooler:','')
        # print(conf[0].split('Cooler:'))
        self.data.Vcool_init = float(conf[0].split('Cooler:')[1])
        self.data.Laser_set = float(conf[1].split('Laser:')[1])
        file.close()
    
        df = pd.read_csv(self.calib_filename, skiprows = 2, names=["Set","Read"],skip_blank_lines = True)

        self.data.Step_Size = abs(df.at[1, 'Set']-df.at[0, 'Set'])
                
        values, cov = np.polyfit(df['Set'], df['Read'], self.data.Cal_order,cov = True)  


        self.data.Cal = []
        self.data.Cal_err = []
        for i,v in enumerate(values):
            self.data.Cal.append(v)
            self.data.Cal_err.append(cov[i,i])
        self.data.Cal.reverse()
        self.data.Cal_err.reverse()

        self.data.Max = df["Set"].max()
        self.data.Min = df["Set"].min()

        self.data.Bin = math.floor((self.data.Max - self.data.Min)/self.data.Step_Size)
        self.data.LoadingTime = time.time()-start

    def loadData(self):
        start = time.time()
        self.data.Run = dd.read_csv(self.run_filename, blocksize=self.blocksize,names=["TS","DV","Bunch","TDC","TOF","Vrfq"],skip_blank_lines = True)  # reading in 25MB chunks

        self.data.Size = len(self.data.Run)

        self.data.LoadingTime += time.time()-start



