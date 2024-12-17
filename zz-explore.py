#!/usr/bin/env python

from pathlib import Path
import pprint
import subprocess

import h5py
from h5py import h5f, h5p

from utils import \
    file_size, \
    sizeof_fmt, \
    get_chunk_volume, \
    describe_compression

# Get compression on all datasets

# describe_compression("m21c_filespec/DAILY_FILES/diag/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10.nc4")
# describe_compression("m21c_filespec/DAILY_FILES/diag/m21c_filespec.asm_const_0hr_glo_C360x360x6_slv.1997-03-10.nc4")

f1 = Path("m21c_filespec/DAILY_FILES/diag/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10.nc4")
# infile = f1
# chunking = (6, 2, 180, 180)

out_root = Path("experiments")
outdir = out_root / f1.

out_root = Path("experiments")

outdir = out_root / "comp_09"
outdir.mkdir(exist_ok=True, parents=True)

outfile = outdir / f1.name

hf_sizes = {key: get_chunk_volume(hf, key) for key in hf.keys()}
pprint.pprint(hf_sizes)

hf = h5py.File(str(f1))
fcid = hf.id.get_create_plist()
strategy, persist, threshold = fcid.get_file_space_strategy()

hf.id
hfid = hf.id
h5f.get_filespace_strategy(hf)
indat = hf["TSTAR"]
sizeof_fmt(indat.id.get_storage_size() / indat.id.get_num_chunks())

with h5py.File(str(f1)) as in_hf, h5py.File(outfile, "w") as out_hf:
    for key in hf.keys():
        print(key)
        indat = hf[key]
        comp = None
        opts = None
        shuffle = False
        if indat.compression == "gzip":
            comp = "gzip"
            opts = 9
            shuffle = True
        new_dat = out_hf.create_dataset_like(
            key,
            indat,
            compression = comp,
            compression_opts = opts,
            shuffle = shuffle
        )
        new_dat[...] = indat[...]

# Compare input and output file sizes
Path(outfile).stat().st_size / f1.stat().st_size

# Now, try repacking the original dataset
subprocess.run(["h5repack"])

########################################
import xarray as xr
inhf
outhf = h5py.File(str(outfile))
outhf
infile
dtest = xr.open_dataset(outfile)

describe_h5file(infile)
describe_h5file(outfile)

################################################################################

# Old code

# Open file
# inhf = h5py.File(str(infile), mode='r')
# all_vars = get_chunks(inhf)
# chunked_vars_all = [key for key in all_vars.keys() if all_vars[key] is not None]
# chunked_vars = sorted(set(chunked_vars_all) - {"lons", "lats", "corner_lons", "corner_lats", "time"})
# Dimensions are time, vertical, lon, lat
# chunk_vols = {key: get_chunk_volume(inhf, key) for key in chunked_vars}

###

# outhf = h5py.File(
#     str(outfile),
#     mode='w',
#     fs_strategy="page",
#     fs_page_size=page_size
# )
# # Apply  global attributes
# for attr_name, attr_value in inhf.attrs.items():
#     outhf.attrs[attr_name] = attr_value
#
# def copy_and_rechunk(name, obj):
#     if isinstance(obj, h5py.Dataset):
#         print(f"Rechunking dataset: {name}")
#         din = inhf[name]
#         out_chunks = din.chunks
#         if out_chunks and (len(out_chunks) == len(chunking)):
#             out_chunks = chunking
#         dout = outhf.create_dataset_like(name, din, chunks=out_chunks)
#         dout[...] = din[...]
#         for attr_name, attr_value in obj.attrs.items():
#             dout.attrs[attr_name] = attr_value
#     elif isinstance(obj, h5py.Group):
#         print(f"Processing group: {name}")
#         outhf.create_group(name)
#         for attr_name, attr_value in obj.attrs.items():
#             outhf[name].attrs[attr_name] = attr_value
#
# inhf.visititems(copy_and_rechunk)
# outhf.close()
# return outfile

################################################################################
