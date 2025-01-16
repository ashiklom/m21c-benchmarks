#!/usr/bin/env python

from pathlib import Path
import h5py
from pprint import pprint
import natsort
from collections import OrderedDict

import utils

origdir = Path("m21c_all/SAMPLE_DAY")
aer_inst_orig = sorted(origdir.glob("*aer_inst*v72*"))

fname = aer_inst_orig[0]

hf = h5py.File(fname, "r")

desc = utils.describe_h5file(fname)
# Not quite...but gets you close to the answer
desc2 = OrderedDict(natsort.natsorted(desc.items(), key=lambda item: item[1]["total_storage"]))
pprint(desc2)

# DU003 is the biggest variable in aer_inst (~95 MB per file)
