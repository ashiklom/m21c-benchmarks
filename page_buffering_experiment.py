#!/usr/bin/env python

from utils import cloud_optimize, describe_h5file

from pathlib import Path
from multiprocessing import Pool

if __name__ == "__main__":
    infile = Path("m21c_filespec/SAMPLE_DAY/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z.nc4")

    def do_page_size(ps):
        return cloud_optimize(infile, page_size=ps)

    # describe_h5file(infile)

    # Default: 4 Kb (4096)
    page_sizes_kb = [4, 16, 256, 1024, 1024*4, 1024*8, 1024*16]
    page_sizes = [x*1024 for x in page_sizes_kb]

    with Pool(8) as pool:
        pool.map(do_page_size, page_sizes)
