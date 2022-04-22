import clstools as cls
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def main():

    data = cls.CLSDataFrame()
    cls.Importer("/data/CLS/NatU",3919,data)
    data.computeWL(235,0) #mass[u],reference frequency[Hz]
    print(data.Sorted) # data.sorted host the calibrated data with frequency info
    data.info()

if __name__ == '__main__':
    main()
