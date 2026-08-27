# CLS_tools

A lightweight utility package for loading and processing IGISOL collinear laser spectroscopy (CLS) run files.

This project provides a single main class, `CLSDataFrame`, which uses Dask DataFrames for initial sorting and
Pandas/Numpy for subsequent processing. The loader supports ASDF run files 

## Features

- Load ASDF run files and extract run metadata
- Compute calibrated voltages from divider calibrations
- Convert cooler/scan voltages into particle-frame frequencies using relativistic Doppler formulas
- Bin data in frequency or time-of-flight (ToF), with optional gating by PMT / voltage / ToF ranges

## Installation

This project uses a `pyproject.toml`-based build. From the project root directory run:

```powershell
pip install .
```
Or build a wheel and install it:

```powershell
# optional: create a wheel
python -m build
pip install dist\clstools-0.5.0-py3-none-any.whl
```
If you use `uv` you can run the example directly without creating a venv or installing:

```powershell
uv run python .\example.py
```

## Requirements

The core runtime dependencies (declared in `pyproject.toml`) are:

- pandas
- dask[complete]
- asdf
- numpy

## Quick usage

A runnable example is provided in `example.py` which demonstrates a typical analysis flow and plotting.

## License

GPL-3.0 — see `LICENSE`.


