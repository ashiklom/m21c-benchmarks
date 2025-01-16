#!/usr/bin/env bash

# What explains the differences between original and rechunked files?

h5dump -Hpd "/DU003" experiments/pagesize_8MB_chunk_c90/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0200Z.nc4
# DU003 compression ratio is ~2.364
h5dump -Hpd "/DU003" m21c_all/SAMPLE_DAY/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0200Z.nc4
# DU003 compression ratio is ~2.346

h5dump -Hpd "/VLML" experiments/pagesize_8MB_chunk_c90/e5303_m21c_jan18.flx_tavg_1hr_glo_C360x360x6_slv.2018-01-31T0130Z.nc4 | grep " SIZE"
h5dump -Hpd "/VLML" m21c_all/SAMPLE_DAY/e5303_m21c_jan18.flx_tavg_1hr_glo_C360x360x6_slv.2018-01-31T0130Z.nc4 | grep " SIZE"

h5dump -Hpd "/DQVDTDYN" experiments/pagesize_8MB_chunk_c90/e5303_m21c_jan18.qdt_tavg_1hr_glo_L1152x721_p48.2018-01-31T0130Z.nc4 | grep -E " SIZE"
h5dump -Hpd "/DQVDTDYN" m21c_all/SAMPLE_DAY/e5303_m21c_jan18.qdt_tavg_1hr_glo_L1152x721_p48.2018-01-31T0130Z.nc4 | grep " SIZE"

##################################################

# What if we compare original and page-buffered files?
compare_sizes() {
  SIZE1=$(du -s $1 | cut -f1)
  SIZE2=$(du -s $2 | cut -f1)
  echo "scale=5; $SIZE1/$SIZE2" | bc -l
}

# aer_inst --- basically the same
compare_sizes \
  experiments/pagesize_8MB/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z.nc4 \
  m21c_all/SAMPLE_DAY/e5303_m21c_jan18.aer_inst_1hr_glo_C360x360x6_v72.2018-01-31T0000Z.nc4

compare_sizes \
  experiments/pagesize_8MB/e5303_m21c_jan18.flx_tavg_1hr_glo_C360x360x6_slv.2018-01-31T0030Z.nc4 \
  m21c_all/SAMPLE_DAY/e5303_m21c_jan18.flx_tavg_1hr_glo_C360x360x6_slv.2018-01-31T0030Z.nc4

compare_sizes \
  experiments/pagesize_8MB/e5303_m21c_jan18.qdt_tavg_1hr_glo_L1152x721_p48.2018-01-31T0030Z.nc4 \
  m21c_all/SAMPLE_DAY/e5303_m21c_jan18.qdt_tavg_1hr_glo_L1152x721_p48.2018-01-31T0030Z.nc4
