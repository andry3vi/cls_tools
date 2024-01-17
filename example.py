import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

def main():


    c = 299792458 #m/s
    Elow=0
    Eup = 42658.2444
    WN_to_f = 1e2*c 
    Frequency = (Eup-Elow)*WN_to_f*1e-6 #MHz

    data = cls.CLSDataFrame()
    data.Load_Run("/data/CLS/Data",4912,blocksize=15e6,cal_order=1)
    data.Compute_Voltages()
    data.Compute_WL(Mass=56,ref=Frequency,harmonic=4)
    data.Compute_ToF(PMT_gate=[1,2,3,4])
    data.Compute_Raw_Bins(TOF_gate=[35,39.5],PMT_gate=[1,2,3,4])
    data.Compute_Bins(TOF_gate=[35,39.5],PMT_gate=[1,2,3,4])
    data.Info()
    
    fig_TOF, axs_TOF = plt.subplots(figsize=(16,9), dpi=100)
    
        
    T = np.array(data.ToF_binned.index.to_list())
    C = np.array(data.ToF_binned.values[:,0])
    bins = np.linspace(T.min(),T.max(),int((T.max()-T.min())/0.5))

    C_binned,T_binned, patch = axs_TOF.hist(T,bins=bins,  weights = C,histtype='step') 
    
    axs_TOF.set_ylabel("Counts")
    axs_TOF.set_xlabel("Time [$\mu$s]")
    axs_TOF.set_title('TOF')


    axs_TOF.axvline(35,linestyle='dashed',color='red',linewidth=2)
    axs_TOF.axvline(39.5,linestyle='dashed',color='red',linewidth=2)
    
    fig_RAW, axs_RAW = plt.subplots(figsize=(16,9), dpi=100)

    V = np.array(data.Raw_binned.index.to_list())
    C = np.array(data.Raw_binned.values[:,0])

    axs_RAW.plot(V,C,drawstyle='steps') 
    axs_RAW.set_ylabel("Counts [n.u.]")
    axs_RAW.set_xlabel('LCR Votlage [V]')
    axs_RAW.set_title('Voltage scan')

    fig, axs = plt.subplots(figsize=(16,9), dpi=100)
   
    F = np.array(data.Binned.index.to_list())
    C = np.array(data.Binned.values[:,0])
    frequency_binning=20 #MHz_
    bins = np.linspace(F.min(),F.max(),int((F.max()-F.min())/frequency_binning))

    C_binned,F_binned, patch = axs.hist(F,bins=bins, weights = C,histtype='step') 
    axs.set_ylabel("Counts [n.u.]")
    axs.set_xlabel("Frequency [MHz]")
    axs.set_title('Frequency scan')
    
    plt.show()

if __name__ == '__main__':
    main()
