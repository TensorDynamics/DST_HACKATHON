#!/home/ubuntu/miniconda3/bin/python python

#
# Highlight this script by Select All, Copy and Paste it into a file;
# make the file executable and run it on command line.
#
# make sure to check python variable path under the variabel name 'python_dir'
#
# Contact support@tensordynamics.in (Faizan Khan) for further assistance.
#################################################################

import os
from datetime import datetime,timedelta
import shutil
import sys
#python variable path
python_dir = '/home/tensor/miniconda3/bin/python'


gfs_path = os.getcwd()
date,month,year = sys.argv[1], sys.argv[2],sys.argv[3]
stringdate = str(year)+str(month).zfill(2)+str(date).zfill(2)
latest_datetime = datetime.strptime(stringdate,"%Y%m%d")

iter = 0
flag = True
j=0
while (iter<10) &(flag==True):
    
    i=30+(24*j)
    next_latest_datetime = latest_datetime+timedelta(days=-j)
    gfs_datetime = next_latest_datetime+timedelta(days=-2)
    gfs_folder = (latest_datetime.date()).strftime("%d-%m-%Y")
    g_year = gfs_datetime.year
    g_month = gfs_datetime.month
    g_date = gfs_datetime.day
    g_hour = i
    
    gfs_folder_name = 'gfs_'+gfs_folder
    #os.makedirs(os.path.join(gfs_path,gfs_folder_name),exist_ok=True)
    #gfs_src_path = os.path.join(gfs_path,gfs_folder_name)
    gfs_inp_1 = python_dir+' '+'gfs_download.py'+' '+str(g_date).zfill(2) +' '+str(g_month).zfill(2)+' '+str(g_year)+' '+ str(g_hour)
    gfs_check_file1 = 'gfs.0p25.'+str(g_year)+str(g_month).zfill(2)+str(g_date).zfill(2)+str(12)+'.f'+str(g_hour).zfill(3)+'.grib2'
#     print(f"gfs checkfile is {gfs_check_file1}")
#     print(f"g_hour is {g_hour}")
#     print(f"gfs input is {gfs_inp_1}")
    if gfs_check_file1 in os.listdir(gfs_path):
        flag=False
    else:
        #os.chdir(gfs_path)
        os.system(gfs_inp_1)

    if gfs_check_file1 in os.listdir(gfs_path):
        flag=False
    j=j+1
    iter = iter+1

if (iter==10):
    print('maximum 10 itearation reachecd: GFS data not found')
elif(flag==False):
    print(f" {j} days previous GFS data found for date {str(next_latest_datetime.date())} ")
else:
    pass

x = i
for t in range(0,62):
#for t in range(0,35):
    x = x+3
    g_hour = x
    gfs_inp_1 = python_dir +' '+'gfs_download.py'+' '+str(g_date).zfill(2) +' '+str(g_month).zfill(2)+' '+str(g_year)+' '+ str(g_hour)
    gfs_check_file = 'gfs.0p25.'+str(g_year)+str(g_month).zfill(2)+str(g_date).zfill(2)+str(12)+'.f'+str(g_hour).zfill(3)+'.grib2'
    
    if (gfs_check_file in os.listdir(gfs_path)):
        pass
    else:
        #os.chdir(gfs_src_path)
        os.system(gfs_inp_1)
        
    if gfs_check_file in os.listdir(gfs_path):
        print(f"{gfs_check_file} downloaded")
        

last_check_file = 'gfs.0p25.'+str(g_year)+str(g_month).zfill(2)+str(g_date).zfill(2)+str(12)+'.f'+str(g_hour).zfill(3)+'.grib2'
if (last_check_file in os.listdir(gfs_path)):
    print('GFS data is downloaded for 7 days simulation')
else:
    print('last GFS file is not present')

