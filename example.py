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
    Frequency = (Eup-Elow)*WN_to_f #MHz

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
    # data.Info()
    fig, axs = plt.subplots(figsize=(16,9), dpi=100)
    # F = np.array(data.Binned.index.to_list())
    # C = np.array(data.Binned.values[:,0])
    
    # frequency_binning=20 #MHzZ
    # bins = np.linspace(F.min(),F.max(),int((F.max()-F.min())/frequency_binning))

    # axs.plot(data.Binned['Fmean']/1e6,data.Binned['Fcount'])
    axs.plot(data.Binned['bins_center']/1e6,data.Binned['Fcount'],drawstyle='steps')
    # data.Compute_Bins(TOF_gate=TOF_GATE,bins=data.Binned data.Frequency_stepsize)

    # C_binned,F_binned, patch = axs.hist(F,bins=bins, weights = C,histtype='step') 
    # print(data.Binned)
    plt.show()
    # data.apply_filter(filter_window=4)
    # data.Compute_Bins(TOF_gate=TOF_GATE,PMT_gate=PMT_GATE )
    
    # F = np.array(data.Binned.index.to_list())
    # C = np.array(data.Binned.values[:,0])
    # bins = np.linspace(F.min(),F.max(),int((F.max()-F.min())/frequency_binning))

    # C_binned,F_binned, patch = axs.hist(F,bins=bins, weights = C,histtype='step') 
    # axs.set_ylabel("Counts [n.u.]")
    # axs.set_xlabel("Frequency [MHz]")
    # axs.set_title('Frequency scan')


    # fig, axs = plt.subplots(figsize=(16,9), dpi=100)
    
    
    # tmp = data.Run.compute()
    # axs.hist(tmp['TS'],bins=1000)

    
    # # tmp['filter'] = True
    # # tmp['filter'] = tmp.groupby("TS")['filter'].transform(lambda x: (False if x.size>2 else True))
    # # print(tmp)
    # # group_keys=True, as_index=False
    # # axs.hist(tmp[tmp['filter']]['TS'],bins=1000)
    # # tmp = tmp.groupby('TS').sum()
    # # print(tmp[tmp['counts']>3])

    # # tmp[["DV","counts"]].groupby('DV').sum()
    
    # # tmp = tmp[tmp.TOF<max(TOF_GATE)]
    # # tmp = tmp[tmp.TOF>min(TOF_GATE)]



    # # PMTS = [1,2,3,4]
    # # excluded = [i for i in PMTS if i not in PMT_GATE]
    # # for pmt in excluded:
    # #     tmp = tmp[tmp.TDC != pmt]
    
    # #     # tmp = tmp[["DV","counts"]].groupby('DV').sum()
    # #     # self.Raw_binned = tmp.compute()
    
    # plt.show()

if __name__ == '__main__':
    main()
