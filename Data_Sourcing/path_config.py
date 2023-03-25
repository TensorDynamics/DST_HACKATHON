#PATHS

import os
import os.path

#Data_Path = '/home/tensor/DST_HACKTHON/Data_Sourcing/Input_Data/'
home = '/home/tensor/DST_HACKTHON/Data_Sourcing/'
gfs_path = os.path.join(home,'Input_Data/GFS_DATA/')
hrit_path =os.path.join(home,'Input_Data/SAT_DATA/')
external_folder_path = '/home/tensor/DST_HACKTHON/OUTPUT/Nowcast/'
#code_path = os.path.join(home,'code/')
code_path=home
os.makedirs(gfs_path,exist_ok=True)
os.makedirs(hrit_path,exist_ok=True)
os.makedirs(external_folder_path,exist_ok=True)

Root ='/home/tensor/Satellite_Nowcasting/build/'
Bin =os.path.join(Root,'bin/')
config = os.path.join(Root,'config/')
Log =os.path.join(Root,'logs/')
Export =os.path.join(Root,'export/')
CT =os.path.join(Root,'export/CT/')
NWP_data =os.path.join(Root,'import/NWP_data/')
Sat_data_archeive =os.path.join(Root,'import/Sat_data_archeive/')
Sat_data =os.path.join(Root,'import/Sat_data/')
