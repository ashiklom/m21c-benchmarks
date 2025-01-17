#!/usr/bin/env python

from typing import Iterable

from utils import ttime

import xarray as xr
import h5py
from pathlib import Path
import warnings

origdir = Path("m21c_all/SAMPLE_DAY")
ps8dir = Path("experiments/pagesize_8MB")

aer_inst_orig = sorted(origdir.glob("*aer_inst*v72*"))
aer_inst_p8 = sorted(ps8dir.glob("*aer_inst*v72*"))

# fname = aer_inst_orig[0]

warnings.simplefilter("ignore")

def open_mf_optim(flist: Iterable, **kwargs):
    hflist = (h5py.File(f, mode="r", **kwargs) for f in flist)
    return xr.open_mfdataset(hflist, engine="h5netcdf", drop_variables="anchor")

##################################################
# Opening a file

ds_orig = ttime(lambda: xr.open_mfdataset(aer_inst_orig, engine="h5netcdf", drop_variables="anchor"))
# Wall: 21.20, User: 12.70, System: 0.31
# Wall: 20.19, User: 12.70, System: 0.35

ds_p8 = ttime(lambda: xr.open_mfdataset(aer_inst_p8, engine="h5netcdf", drop_variables="anchor"))
# Wall: 16.73, User: 12.32, System: 0.07
# Wall: 17.65, User: 12.66, System: 0.12

ds_p8opt = ttime(lambda: open_mf_optim(aer_inst_p8, page_buf_size=8*1024*1024))
# Wall: 12.18, User: 12.05, System: 0.07
# Wall: 12.27, User: 12.14, System: 0.10

########################################
# Time-averaged vertical profile at a random location

_ = ttime(lambda: ds_orig["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute())
# Wall: 73.31, User: 26.75, System: 0.65

_ = ttime(lambda: ds_p8["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute())
# Wall: 60.55, User: 27.57, System: 0.59

_ = ttime(lambda: ds_p8opt["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute())
# Wall: 31.88, User: 27.58, System: 0.53

########################################
# Time-averaged map of the entire world at a particular vertical level?

_ = ttime(lambda: ds_orig["DU003"].isel(lev=7, nf=1).mean("time").compute())
# Wall: 1.42, User: 1.36, System: 0.05

_ = ttime(lambda: ds_p8["DU003"].isel(lev=7, nf=1).mean("time").compute())
# Wall: 1.29, User: 1.32, System: 0.05

_ = ttime(lambda: ds_p8opt["DU003"].isel(lev=7, nf=1).mean("time").compute())
# Wall: 1.44, User: 1.49, System: 0.04

########################################
# Global average of everything

_ = ttime(lambda: ds_orig["DU003"].mean().compute())
# Wall: 227.35, User: 160.92, System: 4.10

_ = ttime(lambda: ds_p8["DU003"].mean().compute())
# Wall: 246.04, User: 166.20, System: 3.98

_ = ttime(lambda: ds_p8opt["DU003"].mean().compute())
# Wall: 158.93, User: 165.10, System: 3.92
