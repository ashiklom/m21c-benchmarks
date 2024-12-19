#!/usr/bin/env python

import timeit
import warnings
from pathlib import Path

def writeline(fname: Path|str, stuff: list) -> None:
    linestring = ",".join([str(s) for s in stuff]) + "\n"
    with open(fname, "a") as of:
        of.write(linestring)

if __name__ == "__main__":
    experiment_dir = Path("experiments/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10/")
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
            topen = timeit.timeit(f"xr.open_dataset('{fname}')", setup = "import xarray as xr", number=20)
            ttimeseries = timeit.timeit(f"xr.open_dataset('{fname}')['ULML'].mean(('Ydim', 'Xdim')).values", setup = "import xarray as xr", number=20)
            tspatial = timeit.timeit(f"xr.open_dataset('{fname}')['ULML'].mean(('Ydim', 'Xdim')).values", setup = "import xarray as xr", number=20)
        writeline(resultfile, [fname, topen, ttimeseries, tspatial])
