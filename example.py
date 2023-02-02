import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

def main():

    data = cls.CLSDataFrame()
    start = time.time()
    cls.Importer("/data/CLS/Data",4098,data,blocksize=10e6)
    # data.info()
    data.Compute_Voltages()
    data.Compute_WL(56,0)

    data.info()
    print('total = ', time.time()-start)
    #  # data.sorted host the calibrated data with frequency info
    # df = data.Sorted[(data.Sorted['TOF']>48) & (data.Sorted['TOF']<58 )]  

    # bins = round(abs(df["V"].max()-df["V"].min()))
    # count, bin = np.histogram(data.Sorted['V'].tolist(), bins = bins)

    # # plt.figure()
    # # sett = []
    # # read = []
    # # for key in data.Cal:
    # #     sett.append(key)
    # #     read.append(data.Cal[key])
    # # # plt.plot(sett,read,'yo')
    # # # plt.plot([-1400,1780],[-1400,1780],'r--')

    
    # plt.figure()
    # plt.plot(bin[:-1],count)
    # plt.show()

if __name__ == '__main__':
    main()
