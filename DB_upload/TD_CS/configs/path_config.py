"""
relating paths repo for the source code
"""
import os
from funcs.helpers import parse_args_shell

# DEFAULT OPTIONS
data_path = "/home/tensor/DST_HACKTHON/OUTPUT/Week_ahead"
src_path = os.getcwd()
resources_path = os.path.join(src_path, 'resources')
test_samples = os.path.join(src_path, 'tests', 'samples')

# PARSE SHELL/CMD RUNTIME OPTIONS
config = parse_args_shell()
local_output = config.get('localoutput')
local_output_dir = config.get('localdir')
verbose = config.get('verbose')

if local_output:
    if not local_output_dir:
        local_output_dir = os.path.abspath(os.path.join(src_path, '..', 'WRF_VIEW'))
    os.makedirs(local_output_dir, exist_ok=True)
