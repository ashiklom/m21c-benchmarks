#!/usr/bin/env python

import timeit
import warnings
from pathlib import Path
import xarray as xr
import h5py
import re

def writeline(fname: Path|str, stuff: list) -> None:
    linestring = ",".join([str(s) for s in stuff]) + "\n"
    with open(fname, "a") as of:
        of.write(linestring)

def parse_fname(fname):
    # fname = Path("experiments/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z/cNone_p1048576.nc4")
    stem = fname.stem
    m = re.match(r"c(.*?)_p([0-9]+)", stem)
    if not m:
        raise ValueError(f"Invalid file name: {fname}")
    chunking = m.group(1)
    if chunking == "None":
        chunking = None
    psize = m.group(2)
    if psize == "None":
        psize = None
    else:
        psize = int(psize)
    return {"chunking": chunking, "page_size": psize}

def open_with_props(fname):
    props = parse_fname(fname)
    return xr.open_dataset(h5py.File(
        str(fname),
        mode="r",
        page_buf_size = props["page_size"]
    ), engine="h5netcdf")

def do_open(fname):
    with open_with_props(Path(fname)) as _:
        pass

def do_ts(fname):
    with open_with_props(Path(fname)) as ds:
        ds["BRPHOBIC"].mean(("Ydim", "Xdim")).compute()

def do_spatial(fname):
    with open_with_props(Path(fname)) as ds:
        ds["BRPHOBIC"].mean("time").compute()

if __name__ == "__main__":
    # experiment_dir = Path("experiments/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10/")
    experiment_dir = Path("experiments/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z")
    fnames = sorted(experiment_dir.glob("*.nc4"))
    resultfile = Path("experiments") / "page-size.csv"
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
        writeline(resultfile, [fname.stem, topen, ttimeseries, tspatial])
