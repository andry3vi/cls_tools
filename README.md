# CLS_tools

A python class to handle IGISOL collinear laser spectroscopy data files.
The class make use of [dask](https://docs.dask.org/) dataframes for the initial data sorting. This allows for a intrinsic paralelization of the task and a considerably faster processing time.