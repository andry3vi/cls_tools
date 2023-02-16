import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

def main():


    c = 299792458 #m/s
    Elow=0
    Eup = 34612.820
    WN_to_f = 1e2*c 
    Frequency = (Eup-Elow)*WN_to_f*1e-6 #MHz

    data = cls.CLSDataFrame()
    start = time.time()
    data.Load_Run("/data/CLS/Data",3783,blocksize=15e6,cal_order=1)
    data.Compute_Voltages()
    data.Compute_WL(238,Frequency)
    data.Compute_Bins(TOF_gate=[100,130])
    print(data.Binned)
    data.Info()
    print('total = ', time.time()-start)
    
    plt.figure()
    
    plt.plot(data.Binned.index,data.Binned.values,drawstyle='steps')
    plt.show()

if __name__ == '__main__':
    main()
