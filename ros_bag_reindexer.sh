#!/bin/bash
#
# Reindexes all bags files in a directory tree, renaming the newly
# reindexed files so they end in .bag rather than .bag.archive
#
# Usage:
#    ros_bag_reindexer.sh DIRECTORY
#
# Val Schmidt
# Center for Coastal and Ocean Mapping
# University of New Hampshire
# 2023

# Get a recursive list of all files in the directory tree that 
# need reindexing (e.g. ending in active)
FILES=`find "${1}" -type f | grep '.bag.active'`

# Loop through them, reindexing each, and 
# renaming them, removing the '.active' 
# Note, rosbag reindex will create a copy of the original
# in xxxxx.bag.orig.active. This is retained.

for f in $FILES; do
  rosbag reindex $f
  b=`basename $f`
  d=`dirname $f`
  outfile="$d/${b%%.*}.bag"
  mv $f ${outfile}
done;
