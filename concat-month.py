#!/usr/bin/env python

from pathlib import Path

import xarray as xr
from dask.distributed import Client
from dask.diagnostics import ProgressBar
from tempfile import mktemp

from rechunker import rechunk

import warnings
# warnings.resetwarnings()
warnings.filterwarnings("ignore", message="Duplicate dimension names present.*")

client = Client()

rootdir = Path("m21c_all/chem/Y2018/M01/")
aer_inst_files = sorted(rootdir.glob("*aer_inst*v72*"))

ds = xr.open_mfdataset(aer_inst_files[0:48], engine="h5netcdf", drop_variables="anchor")
ds_sub = ds[["DU003"]]

target_chunks = {"time": 24, "lev": 6, "nf": 1, "Ydim": 90, "Xdim": 90}
ds_sub_new = ds_sub.chunk(target_chunks)

target_store = Path("experiments/aggregates/Y2018-M01-aer_inst_v72-DU003.nc4")
target_store.parent.mkdir(exist_ok=True, parents=True)

hf = 
ds_sub_new.to_netcdf(target_store, format="NETCDF4", engine="h5netcdf")

temp_store = mktemp(".zarr")
ds_sub_plan = rechunk(
    ds_sub,
    target_chunks,
    "1.5 GB",
    target_store,
    temp_store=temp_store
)

with ProgressBar():
    ds_sub_plan.execute()
