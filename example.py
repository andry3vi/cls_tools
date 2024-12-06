import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
import asdf 

def main():

    # with asdf.open('/data/CLS/Data/2024/03/26/run_5149.asdf', copy_arrays=True) as af:
    #     af.info()
    c = 299792458 #m/s
    Elow=0
    Eup = 29552.05741
    WN_to_f = 1e2*c 
    Frequency = (Eup-Elow)*WN_to_f #Hz

    TOF_GATE = [54,63]
    PMT_GATE = [3,4]
    data = cls.CLSDataFrame()
    data.Load_Run("/data/CLS/Data/2024/11/05/run_5559.asdf")
    data.Compute_Voltages()
    data.Compute_WL(Mass=107,ref=Frequency,harmonic=2)
    
    data.Compute_ToF(PMT_gate=PMT_GATE )
    fig_TOF, axs_TOF = plt.subplots(figsize=(16,9), dpi=100)
    
    T = np.array(data.ToF_binned.index.to_list())
    C = np.array(data.ToF_binned.values[:,0])
    bins = np.linspace(T.min(),T.max(),int((T.max()-T.min())/0.5))
    C_binned,T_binned, patch = axs_TOF.hist(T,bins=bins,  weights = C,histtype='step') 
    
    axs_TOF.set_ylabel("Counts")
    axs_TOF.set_xlabel(r"Time [$\mu$s]")
    axs_TOF.set_title('TOF')
    
    data.Compute_Bins(TOF_gate=TOF_GATE)

    fig, axs = plt.subplots(figsize=(16,9), dpi=100)
   
    axs.plot(data.Binned['bins_center']/1e6,data.Binned['Fcount'],drawstyle='steps')
    
    axs.set_ylabel("Counts [n.u.]")
    axs.set_xlabel("Frequency [MHz]")
    plt.show()
    
   

if __name__ == '__main__':
    main()
