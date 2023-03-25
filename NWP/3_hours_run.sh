#!/bin/sh -f

PYTHON=/home/tensor/miniconda3/bin/python
NWP=/home/tensor/DST_HACKTHON/NWP


cd $NWP
$PYTHON nowcasting_run.py
