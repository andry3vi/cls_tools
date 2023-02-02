import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

def main():

    data = cls.CLSDataFrame()
    start = time.time()
    cls.Importer("/data/CLS/Data",3783,data,blocksize=10e6)
    # data.info()
    data.Compute_Voltages()

    c = 299792458 #m/s
    Elow=0
    Eup = 34612.820
    WN_to_f = 1e2*c 

    Frequency = (Eup-Elow)*WN_to_f
    data.Compute_WL(238,Frequency)
    print(data.Sorted)
    x,y = data.Compute_Bins(TOF_gate=[100,130] ,V_gate=None)

    data.info()
    print('total = ', time.time()-start)
    
    # print(data.Sorted)
    # print(data.Run)
    #  # data.sorted host the calibrated data with frequency info
    # df = data.Sorted[(data.Sorted['TOF']>48) & (data.Sorted['TOF']<58 )]  

    # bins = round(abs(df["V"].max()-df["V"].min()))
    # count, bin = np.histogram(data.Sorted['V'].tolist(), bins = bins)

    plt.figure()
    
    plt.plot(x,y)
    plt.show()

if __name__ == '__main__':
    main()
