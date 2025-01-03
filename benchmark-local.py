#!/usr/bin/env python

import xarray as xr
from pathlib import Path
import warnings

origdir = Path("m21c_all/SAMPLE_DAY")
ps8dir = Path("experiments/pagesize_8MB")

aer_inst_orig = sorted(origdir.glob("*aer_inst*v72*"))
aer_inst_p8 = sorted(ps8dir.glob("*aer_inst*v72*"))

# fname = aer_inst_orig[0]

warnings.simplefilter("ignore")
%time ds_orig = xr.open_mfdataset(aer_inst_orig, engine="h5netcdf", drop_variables="anchor")  
%time ds_p8 = xr.open_mfdataset(aer_inst_p8, engine="h5netcdf", drop_variables="anchor")  
