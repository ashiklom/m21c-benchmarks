#!/usr/bin/env python

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

def open_mf_optim(flist, **kwargs):
    hflist = (h5py.File(f, mode="r", **kwargs) for f in flist)
    return xr.open_mfdataset(hflist, engine="h5netcdf", drop_variables="anchor")

##################################################
# Opening a file

%time ds_orig = xr.open_mfdataset(aer_inst_orig, engine="h5netcdf", drop_variables="anchor")  
# CPU times: user 12.6 s, sys: 287 ms, total: 12.9 s
# Wall time: 21.9 s

%time ds_p8 = xr.open_mfdataset(aer_inst_p8, engine="h5netcdf", drop_variables="anchor")  
# CPU times: user 12.2 s, sys: 107 ms, total: 12.3 s
# Wall time: 17 s

%time ds_p8opt = open_mf_optim(aer_inst_p8, page_buf_size=8*1024*1024)
# CPU times: user 12.6 s, sys: 147 ms, total: 12.8 s
# Wall time: 14.2 s

########################################
# Time-averaged vertical profile at a random location

%time ds_orig["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute()
# CPU times: user 27.2 s, sys: 745 ms, total: 27.9 s
# Wall time: 1min 13s

%time ds_p8["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute()
# CPU times: user 27.4 s, sys: 499 ms, total: 27.9 s
# Wall time: 1min 3s

%time ds_p8opt["DU003"].isel(Xdim=181, Ydim=181, nf=1).mean("time").compute()
# CPU times: user 27.8 s, sys: 517 ms, total: 28.3 s
# Wall time: 30.3 s

########################################
# Time-averaged map of the entire world at a particular vertical level?

%time ds_orig["DU003"].isel(lev=7, nf=1).mean("time").compute()
# CPU times: user 1.28 s, sys: 52.9 ms, total: 1.33 s
# Wall time: 1.34 s

%time ds_p8["DU003"].isel(lev=7, nf=1).mean("time").compute()
# CPU times: user 1.3 s, sys: 48.5 ms, total: 1.35 s
# Wall time: 1.26 s

%time ds_p8opt["DU003"].isel(lev=7, nf=1).mean("time").compute()
# CPU times: user 1.29 s, sys: 24.1 ms, total: 1.31 s
# Wall time: 1.25 s

########################################
# Global average of everything

%time ds_orig["DU003"].mean().compute()
# CPU times: user 2min 46s, sys: 4.08 s, total: 2min 50s
# Wall time: 3min 41s

%time ds_p8["DU003"].mean().compute()
# CPU times: user 2min 42s, sys: 4.4 s, total: 2min 47s
# Wall time: 4min 6s

%time ds_p8opt["DU003"].mean().compute()
# CPU times: user 2min 44s, sys: 3.9 s, total: 2min 47s
# Wall time: 2min 37s
