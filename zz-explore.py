#!/usr/bin/env python

import s3fs
import h5py
import xarray as xr
import re
import humanize
import pprint
from collections import OrderedDict

import warnings
warnings.simplefilter("ignore")

s3 = s3fs.S3FileSystem(anon=False)
basedir = "gmao-m21c-test-usw2/SAMPLE_DAY"
fname = s3.glob(basedir + "/e5303_m21c_jan18.flx_tavg*")[0]

f = s3.open(fname, "rb")
hf = h5py.File(f, "r")

# Now, try with the optimized files

bucket = "gmao-m21c-test-usw2"
fname = s3.glob(f"{bucket}/experiments/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z/*")[0]

f = s3.open(fname, "rb")
f.close()
%time hf = h5py.File(f, "r", page_buf_size=1048576)
hf.close()

def parse_fname(fname):
    # fname = Path("experiments/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z/cNone_p1048576.nc4")
    stem = fname.split("/")[-1]
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
    s3 = s3fs.S3FileSystem(anon=False)
    return xr.open_dataset(h5py.File(
        s3.open(fname, "rb"),
        mode="r",
        page_buf_size = props["page_size"]
    ), engine="h5netcdf")

def open_plain(fname):
    try:
        _ = parse_fname(fname)
    except ValueError:
        pass
    s3 = s3fs.S3FileSystem(anon=False)
    return xr.open_dataset(h5py.File(
        s3.open(fname, "rb"),
        mode="r"
    ), engine="h5netcdf")

def do_ts(fname, openfunc):
    ds = openfunc(fname)
    return ds["T"].mean("time").compute()

def do_spatial(fname, openfunc):
    ds = openfunc(fname)
    return ds["T"].mean(("Xdim", "Ydim")).compute()

fnames = s3.glob(f"{bucket}/experiments/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z/*")
fname = fnames[2]

orig = f"{bucket}/SAMPLE_DAY/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z.nc4"
orig_size = s3.disk_usage(orig)

sizes = {key: s3.disk_usage(key)/orig_size for key in fnames}
sizes = OrderedDict({k: v for k, v in sorted(sizes.items(), key=lambda item: item[1])})
sizes

fname = list(sizes.keys())[0]
# fname = orig
print(fname)
%time _ = open_with_props(fname)
%time _ = open_plain(fname)
%time _ = do_ts(fname, open_plain)
%time _ = do_ts(fname, open_with_ptops)
%time _ = do_spatial(fname, open_plain)
%time _ = do_spatial(fname, open_with_props)

################################################################################

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
import xarray as xr
from pathlib import Path

fname = Path("m21c_filespec/SAMPLE_DAY/e5303_m21c_jan18.flx_tavg_1hr_glo_C360x360x6_slv.2018-01-31T0030Z.nc4")

ds = xr.open_dataset(fname)

ds["ULML"]
