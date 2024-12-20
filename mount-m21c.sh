#!/usr/bin/env bash

MOUNTDIR="m21c_filespec"
mkdir -p $MOUNTDIR

if [[ ! -d "$MOUNTDIR/SAMPLE_DAY" ]]; then
  echo "Mounting to $MOUNTDIR"
  mount-s3 gmao-m21c-test-usw2 $PWD/$MOUNTDIR
else
  echo "Already mounted"
fi
