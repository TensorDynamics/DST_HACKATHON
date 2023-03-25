"""
relating paths repo for the source code
"""
from os import getcwd, path, makedirs
# DEFAULT OPTIONS
#data_path = "/Users/vasu/Library/CloudStorage/OneDrive-DecisionTreeAnalyticsServices/TensorDynamics/Data/Satellite"
# exim_data_path = "/Users/vasu/OneDrive - DecisionTree Analytics Services/TensorDynamics/Data/Satellite/EXIM SAMPLE"
data_path = "/home/tensor/DST_HACKATHON/OUTPUT/Nowcast"
exim_data_path = "/home/tensor/DST_HACKATHON/OUTPUT/Nowcast/EXIM"
root_path = getcwd()
resources_path = path.join(root_path, 'resources')
makedirs(resources_path, exist_ok=True)
