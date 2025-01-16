#!/usr/bin/env python

from utils import cloud_optimize
from pathlib import Path
from multiprocessing import Pool

from tqdm import tqdm

if __name__ == "__main__":
    basedir = Path("m21c_all/SAMPLE_DAY/")
    flist = sorted(basedir.glob("*.nc4"))

    def do_page_size(fname):
        # fname = fsub[0]
        page_size = 1024 * 1024 * 8  # 8 MB
        chunks = None
        if "_L1152x721_" in fname.name:
            space = (181, 288)
            if "_slv." in fname.name:
                chunks = (1,) + space
            elif "_p48" in fname.name:
                chunks = (1, 24) + space
        elif "_C360x360x6_" in fname.name:
            space = (90, 90)
            if "_slv." in fname.name:
                # Default chunking: (1, 1, 360, 360)
                chunks = (1, 1) + space
            elif "_v72." in fname.name:
                # Default chunking: (1, 1, 1, 360, 360)
                chunks = (1, 36, 1) + space
        localdir = Path("experiments/pagesize_8MB_chunk_c90/")
        localdir.mkdir(exist_ok=True, parents=True)
        outfile = localdir / fname.name
        _ = cloud_optimize(fname, page_size=page_size, chunking=chunks, outfile=outfile)
        return outfile

    # fsub = [f for f in flist if "aer_inst" in str(f) and "v72" in str(f)]
    # fsub = [f for f in flist if "qdt_tavg" in str(f) and "p48" in str(f)]
    # fsub = [f for f in flist if "flx_tavg" in str(f) and "slv" in str(f)]
    # fname = fsub[1]
    # %time result = do_page_size(fname)
    # from humanize import naturalsize
    # naturalsize(result.stat().st_size)
    # naturalsize(fname.stat().st_size)
    # result.stat().st_size / fname.stat().st_size

    pool = Pool()
    for _ in tqdm(pool.imap_unordered(do_page_size, flist), total = len(flist)):
        pass
