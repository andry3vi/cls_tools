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
    Eup = 42658.2444
    WN_to_f = 1e2*c 
    Frequency = (Eup-Elow)*WN_to_f*1e-6 #MHz

    TOF_GATE = [37.9,47.3]
    PMT_GATE = [3,4]
    data = cls.CLSDataFrame()
    data.Load_Run("/data/CLS/Data/2024/03/26/",5136,blocksize=15e6,cal_order=1)
    data.Compute_Voltages()
    data.Compute_WL(Mass=53,ref=Frequency,harmonic=4)
    data.Compute_ToF(PMT_gate=PMT_GATE )
    data.Compute_Raw_Bins(TOF_gate=TOF_GATE,PMT_gate=PMT_GATE )
    data.Compute_Bins(TOF_gate=TOF_GATE,PMT_gate=PMT_GATE)
    data.Info()
    
    fig_TOF, axs_TOF = plt.subplots(figsize=(16,9), dpi=100)
    
    T = np.array(data.ToF_binned.index.to_list())
    C = np.array(data.ToF_binned.values[:,0])
    bins = np.linspace(T.min(),T.max(),int((T.max()-T.min())/0.5))

    C_binned,T_binned, patch = axs_TOF.hist(T,bins=bins,  weights = C,histtype='step') 
    
    axs_TOF.set_ylabel("Counts")
    axs_TOF.set_xlabel(r"Time [$\mu$s]")
    axs_TOF.set_title('TOF')


    axs_TOF.axvline(TOF_GATE[0],linestyle='dashed',color='red',linewidth=2)
    axs_TOF.axvline(TOF_GATE[1],linestyle='dashed',color='red',linewidth=2)
    
    fig_RAW, axs_RAW = plt.subplots(figsize=(16,9), dpi=100)

    V = np.array(data.Raw_binned.index.to_list())
    C = np.array(data.Raw_binned.values[:,0])

    V,C = zip(*sorted(zip(V,C)))
    axs_RAW.plot(V,C,drawstyle='steps') 
    axs_RAW.set_ylabel("Counts [n.u.]")
    axs_RAW.set_xlabel('LCR Votlage [V]')
    axs_RAW.set_title('Voltage scan')

    fig, axs = plt.subplots(figsize=(16,9), dpi=100)
   
    F = np.array(data.Binned.index.to_list())
    C = np.array(data.Binned.values[:,0])
    
    frequency_binning=20 #MHz
    bins = np.linspace(F.min(),F.max(),int((F.max()-F.min())/frequency_binning))

    C_binned,F_binned, patch = axs.hist(F,bins=bins, weights = C,histtype='step') 
    
    data.apply_filter(filter_window=4)
    data.Compute_Bins(TOF_gate=TOF_GATE,PMT_gate=PMT_GATE )
    
    F = np.array(data.Binned.index.to_list())
    C = np.array(data.Binned.values[:,0])
    bins = np.linspace(F.min(),F.max(),int((F.max()-F.min())/frequency_binning))

    C_binned,F_binned, patch = axs.hist(F,bins=bins, weights = C,histtype='step') 
    axs.set_ylabel("Counts [n.u.]")
    axs.set_xlabel("Frequency [MHz]")
    axs.set_title('Frequency scan')


    fig, axs = plt.subplots(figsize=(16,9), dpi=100)
    
    
    tmp = data.Run.compute()
    axs.hist(tmp['TS'],bins=1000)

    
    # tmp['filter'] = True
    # tmp['filter'] = tmp.groupby("TS")['filter'].transform(lambda x: (False if x.size>2 else True))
    # print(tmp)
    # group_keys=True, as_index=False
    # axs.hist(tmp[tmp['filter']]['TS'],bins=1000)
    # tmp = tmp.groupby('TS').sum()
    # print(tmp[tmp['counts']>3])

    # tmp[["DV","counts"]].groupby('DV').sum()
    
    # tmp = tmp[tmp.TOF<max(TOF_GATE)]
    # tmp = tmp[tmp.TOF>min(TOF_GATE)]



    # PMTS = [1,2,3,4]
    # excluded = [i for i in PMTS if i not in PMT_GATE]
    # for pmt in excluded:
    #     tmp = tmp[tmp.TDC != pmt]
    
    #     # tmp = tmp[["DV","counts"]].groupby('DV').sum()
    #     # self.Raw_binned = tmp.compute()
    
    plt.show()

if __name__ == '__main__':
    main()
