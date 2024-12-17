#!/usr/bin/env bash

# h5dump -H -p m21c_filespec/DAILY_FILES/diag/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10.nc4 | grep "CHUNK"

INFILE=m21c_filespec/DAILY_FILES/diag/m21c_filespec.flx_tavg_1hr_glo_C360x360x6_slv.1997-03-10.nc4

OUTDIR="experiments/rechunked/12_2_180_180-page"
mkdir -p "$OUTDIR"
OUTFILE="$OUTDIR/$(basename $INFILE)"
echo "$INFILE --> $OUTFILE"

META_SIZE=$(h5stat $INFILE | grep "File metadata" | grep -o '[0-9]\+')
# echo "scale=0; 2^(1+l($META_SIZE)/l(2)/1)"
PAGE_SIZE=$((1024*1024))
echo $PAGE_SIZE

# Side: 24x6x360x360
# Default: 1x1x360x360
# 6x: 6x1x360x360
# 6x(2): 12x2x180x180
time h5repack -l "CHUNK=12x2x180x180" -S PAGE -G $PAGE_SIZE "$INFILE" "$OUTFILE"
time h5repack -l "CHUNK=12x2x180x180" -S PAGE "$INFILE" "$OUTFILE-2"

echo "$(du $INFILE)"
echo "$(du $OUTFILE-2)"

h5stat $OUTFILE
h5stat $INFILE
h5stat $OUTFILE-2

h5dump -H -p -d "/BSTAR" "$OUTFILE" | grep "COMPRESSION"
h5dump -H -p -d "/BSTAR" "$INFILE" | grep "COMPRESSION"
h5dump -H -p $OUTFILE | grep "CHUNKED"
