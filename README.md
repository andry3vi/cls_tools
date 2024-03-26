# CLS_tools

A python class to handle IGISOL collinear laser spectroscopy data files. 
The class make use of [dask](https://docs.dask.org/) dataframes for the initial data sorting. This allows for a intrinsic paralelization of the tasks and a considerably faster processing time.
New version 0.3 works with asdf file formats

## installation

Simply run in the main directory:

`pip install .`

Import in your code as

`import clstools as cls`

For a detailed example check [example.py](example.py).
