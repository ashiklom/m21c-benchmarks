#!/usr/bin/env python

from utils import cloud_optimize, describe_h5file

from pathlib import Path
from multiprocessing import Pool
from itertools import product

if __name__ == "__main__":
    infile = Path("m21c_filespec/DAILY_FILES/diag/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10.nc4")

    def my_co(pair):
        return cloud_optimize(infile, pair[0], pair[1])

    # describe_h5file(infile)

    chunk_schemes = [
        # Default: 1, 1, 360, 360
        (1, 1, 360, 360),
        (2, 1, 360, 360),
        (4, 1, 360, 360),
        (6, 1, 360, 360),
        (12, 1, 360, 360),
        ###
        (4, 1, 180, 180),
        (6, 1, 180, 180),
        (12, 1, 180, 180)
    ]

    # Default: 4 Kb (4096)
    page_sizes_kb = [4, 16, 256, 1024, 4096]
    page_sizes = [x*1024 for x in page_sizes_kb]

    my_iter = product(chunk_schemes, page_sizes)

    with Pool(24) as pool:
        pool.map(my_co, my_iter)
