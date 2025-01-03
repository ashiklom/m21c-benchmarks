#!/usr/bin/env python

from pathlib import Path
import subprocess

from humanize import naturalsize

import h5py

def file_size(fname: str|Path):
    p = Path(fname)
    return naturalsize(p.stat().st_size)

def get_chunk_volume(obj):
    if obj.chunks:
        size = obj.id.get_storage_size() / obj.id.get_num_chunks()
    else:
        size = obj.id.get_storage_size()
    return naturalsize(size)

def describe_variable(key, hf):
    dsub = hf[key]
    return {
        "compression": dsub.compression,
        "opts": dsub.compression_opts,
        "shuffle": dsub.shuffle,
        "shape": dsub.shape,
        "chunk_strategy": dsub.chunks,
        "chunk_storage": get_chunk_volume(dsub),
        "total_storage": naturalsize(dsub.id.get_storage_size())
    }

def describe_h5file(fname):
    hf = h5py.File(str(fname))
    compressions = {key: describe_variable(key, hf) for key in hf.keys()}
    return compressions

def get_chunks(hf):
    return {key: hf[key].chunks for key in hf.keys()}

def experiment_file(infile, chunking=None, page_size=None):
    out_root = Path("experiments")
    outdir = out_root / infile.stem
    outdir.mkdir(exist_ok=True, parents=True)

    if chunking:
        outfile = outdir / f"c{'x'.join(str(x) for x in chunking)}_p{page_size}.nc4"
    else:
        outfile = outdir / f"cNone_p{page_size}.nc4"
    return outfile

def cloud_optimize(
    infile: Path,
    chunking=None,
    page_size=None,
    overwrite=False,
    outfile=None
):

    if not outfile:
        outfile = experiment_file(infile, chunking, page_size)

    if outfile.exists() and not overwrite:
        print(f"Outfile exists: {outfile}.")
        return outfile

    h5r_command = ["h5repack"]
    if chunking:
        h5r_command += ["-l", f"CHUNK={'x'.join(str(x) for x in chunking)}"]
    if page_size:
        h5r_command += ["-G", str(page_size)]
    h5r_command += [
        "-S", "PAGE",
        str(infile),
        str(outfile)
    ]

    print("Running h5repack...")
    subprocess.run(h5r_command)
    print("Done!")

    return outfile
