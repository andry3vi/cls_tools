import os

import matplotlib.pyplot as plt
import numpy as np

import clstools as cls


def main():

    # directories and run number
    data_directory = "../CLS_DATA/scans/2025/01/18"
    run_number = 6213
    # with asdf.open(
    #     os.path.join(data_directory, f"run_{run_number}.asdf"), copy_arrays=True
    # ) as af:
    #     af.info()

    # constants and parameters of the transition in use
    c = 299792458  # m/s
    Elow = 0  # cm-1
    Eup = 29104.71  # cm-1
    WN_to_f = 1e2 * c  # Hz/(cm-1)
    Frequency = (Eup - Elow) * WN_to_f  # Hz
    Rh103_mass = 102.949240  # amu

    TOF_GATE = [54, 64]
    # up to 4 PMTs on the line depending on the run; this run used only PMT 3 and 4
    PMT_GATE = [3, 4]
    data = cls.CLSDataFrame()  # initilize a cls dataframe class to sort the run
    data.Load_Run(os.path.join(data_directory, f"run_{run_number}.asdf"))  # load the run
    # compute absolute beam energies from the cooler voltage and scanning voltage using
    # the calibration of the voltage dividers
    data.Compute_Voltages()
    # compute the particle restframe Frequency from their energy and the laser setpoint;
    # the reference frequency is subtracted
    data.Compute_WL(Mass=Rh103_mass, ref=Frequency, harmonic=2)

    # computing and plotting the TOF spectrum to visualize the selected TOF gate
    data.Compute_ToF(PMT_gate=PMT_GATE)
    _fig_tof, axs_TOF = plt.subplots(figsize=(16, 9), dpi=100)
    T = np.array(data.ToF_binned.index.to_list())
    C = np.array(data.ToF_binned.values[:, 0])
    bins = np.linspace(T.min(), T.max(), int((T.max() - T.min()) / 0.5))
    _c_binned, _t_binned, _patch = axs_TOF.hist(T, bins=bins, weights=C, histtype="step")
    axs_TOF.axvspan(TOF_GATE[0], TOF_GATE[1], color="orange", alpha=0.5, label="TOF gate")
    axs_TOF.set_ylabel("Counts")
    axs_TOF.set_xlabel(r"Time [$\mu$s]")
    axs_TOF.set_title("TOF")

    # computing and plotting the binned frequency spectrum
    data.Compute_Bins(TOF_gate=TOF_GATE)
    _fig, axs = plt.subplots(figsize=(16, 9), dpi=100)
    axs.plot(data.Binned["Fmean"] / 1e6, data.Binned["Fcount"], drawstyle="steps")
    axs.set_ylabel("Counts [n.u.]")
    axs.set_xlabel("Frequency [MHz]")

    data.Info()
    plt.show()


if __name__ == "__main__":
    main()
