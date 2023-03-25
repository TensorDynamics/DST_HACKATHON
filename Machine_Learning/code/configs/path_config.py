"""
relating paths repo for the source code
"""
import os 

#path config
home = '/home/tensor/DST_HACKTHON/Machine_Learning'
# home = 'D:\\Tensor Dynamics\\backup\\TD\\athena\\Solar_intra_day\\Solar_intra_day'
resource_path = os.path.join(home,'resources')
code_path = os.path.join(home,'code')
log_path = os.path.join(home,'logs')
os.makedirs(log_path, exist_ok = True)

sites = ['SPP1','SPP2','SPP3','SPP4','SPP5']
#for site in sites:
#    os.makedirs(os.path.join(home,'OUTPUT',site), exist_ok = True)
