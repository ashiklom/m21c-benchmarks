#!/usr/bin/env python

from utils import cloud_optimize
from pathlib import Path
from multiprocessing import Pool
import os

from tqdm import tqdm

if __name__ == "__main__":
    basedir = Path("m21c_all/SAMPLE_DAY/")
    flist = sorted(basedir.glob("*.nc4"))

    # outdir = Path("experiments/pagesize_8MB")
    # outdir.mkdir(exist_ok=True, parents=True)

    def do_page_size(fname):
        # fname = flist[0]
        page_size = 1024 * 1024 * 8  # 8 MB
        localdir = Path("experiments/pagesize_8MB/")
        localdir.mkdir(exist_ok=True, parents=True)
        outfile = localdir / fname.name
        _ = cloud_optimize(fname, page_size=page_size, outfile=outfile)
        return outfile

    # fsub = [f for f in flist if "aer_inst" in str(f) and "v72" in str(f)]
    # %time do_page_size(fsub[1])

    pool = Pool()
    for _ in tqdm(pool.imap_unordered(do_page_size, flist), total = len(flist)):
        pass
