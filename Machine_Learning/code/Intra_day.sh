#!/bin/sh -f


#ulimit -s unlimited


PYTHON=/home/ubuntu/anaconda3/envs/td-wrf/bin/python
CODE=/home/ubuntu/WRF_CODEBASE/td-intra_day/code/Version_2

cd $CODE
echo "running intra day script"
$PYTHON Intra_day.py
 
