#!/usr/bin/env python

from utils import cloud_optimize
from pathlib import Path
from multiprocessing import Pool
import os

import s3fs
from tqdm import tqdm

if __name__ == "__main__":
    basedir = Path("m21c_filespec/SAMPLE_DAY/")
    flist = sorted(basedir.glob("*.nc4"))

    outdir = "s3://gmao-m21c-test-usw2/experiments/pagesize_8MB"
    # outdir = Path("experiments/pagesize_8MB")
    # outdir.mkdir(exist_ok=True, parents=True)

    def do_page_size(fname):
        # fname = flist[0]
        outfile = f"{outdir}/{fname.name}"
        s3 = s3fs.S3FileSystem(anon=False)
        if s3.exists(outfile):
            print("Skipping existing file...")
            return outfile
        page_size = 1024 * 1024 * 8  # 8 MB
        localdir = Path("experiments/temporary/")
        localdir.mkdir(exist_ok=True, parents=True)
        localfile = localdir / fname.name
        _ = cloud_optimize(fname, page_size=page_size, outfile=localfile)
        # Push the result to S3
        s3.put(str(localfile), outfile)
        os.remove(localfile)
        return outfile

    pool = Pool(8)
    for _ in tqdm(pool.imap_unordered(do_page_size, flist), total = len(flist)):
        pass
