#!/usr/bin/env python

import timeit
import warnings
from pathlib import Path
import xarray as xr

def writeline(fname: Path|str, stuff: list) -> None:
    linestring = ",".join([str(s) for s in stuff]) + "\n"
    with open(fname, "a") as of:
        of.write(linestring)

def do_open(fname):
    with xr.open_dataset(fname, engine="h5netcdf") as ds:
        pass

def do_ts(fname):
    with xr.open_dataset(fname, engine="h5netcdf") as ds:
        ds["ULML"].mean(("Ydim", "Xdim")).compute()

def do_spatial(fname):
    with xr.open_dataset(fname, engine="h5netcdf") as ds:
        ds["ULML"].mean("time").compute()

if __name__ == "__main__":
    # experiment_dir = Path("experiments/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10/")
    experiment_dir = Path("m21c_filespec/SAMPLE_DAY/")
    fnames = sorted(experiment_dir.glob("*.nc4"))
    resultfile = "benchmarks.csv"
    with open(resultfile, "w") as of:
        of.write("filename,open,timeseries,tspatial\n")
    for fname in fnames:
        # fname = fnames[0]
        print(fname)
        # Note: Ignore xarray duplicate dimension warnings...
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            topen = timeit.timeit(f'do_open("{fname}")', globals=globals(), number=20)
            ttimeseries = timeit.timeit(f'do_ts("{fname}")', globals=globals(), number=20)
            tspatial = timeit.timeit(f'do_spatial("{fname}")', globals=globals(), number=20)
        writeline(resultfile, [fname, topen, ttimeseries, tspatial])
