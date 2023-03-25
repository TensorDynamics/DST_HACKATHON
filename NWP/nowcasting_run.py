import eumdac 
import datetime
import shutil
import time
import fnmatch
import os
from datetime import datetime, timedelta
import zipfile
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
import glob


from Data_Sourcing.sat_data_download import *
from Data_Sourcing.path_config import *

latest,token = hrit_latest()
latest_datetime,l_day,l_month,l_year,l_hour,l_minute = hrit_latest_date(latest=latest)
export_date = str(l_year)+str(l_month)+str(l_day)
latest_time_step = latest_datetime.strftime("%Y%m%d%H%M")
if (CT_check(day = l_day,month=l_month,year=l_year,hour = l_hour,minute=l_minute,time_step = export_date)==False):
    print(f"CT for timestep {latest_time_step} not found in export folder")
    


    gfs_inp_1,gfs_inp_2,gfs_check_file1,gfs_check_file2 = gfs_input(latest_datetime=latest_datetime)
    gfs_datetime = latest_datetime+timedelta(hours = -48)
    gfs_folder = (latest_datetime.date()).strftime("%d-%m-%Y")
#     gfs_folder = (gfs_datetime.date()+timedelta(days = +2)).strftime("%d-%m-%Y")
    gfs_folder_name = 'gfs_'+gfs_folder
    print(gfs_path)
    os.makedirs(os.path.join(gfs_path,gfs_folder_name),exist_ok=True)

    shutil.copy(os.path.join(code_path,'gfs_download.py'),os.path.join(gfs_path,gfs_folder_name,'gfs_download.py'))
    gfs_src_path = os.path.join(gfs_path,gfs_folder_name)
    #print(gfs_src_path)
    gfs_dst_path = NWP_data
#-----------------------------------------------gfs for prev hour-----------------------------------------------------
    i=0
    flag=True
    while((flag==True) & (i<12)):
        next_latest_datetime = latest_datetime+timedelta(days=-i)
        gfs_inp_1,gfs_inp_2,gfs_check_file1,gfs_check_file2 = gfs_input(latest_datetime=next_latest_datetime)
        if (gfs_check_file1 in os.listdir(os.path.join(gfs_path,gfs_folder_name))):
            gfs_hour_rename1 = gfs_rename_copy(file=gfs_check_file1,
                                                year=int(l_year),month=int(l_month),
                                                day=int(l_day),src_dir=gfs_src_path,dst_dir=gfs_dst_path)
            flag=False
        else:
            print('GFS Data Does not Exist, Downloading GFS Data for prev hour')
            os.chdir(gfs_src_path)
            os.system(gfs_inp_1)
            if (gfs_check_file1 in os.listdir(os.path.join(gfs_path,gfs_folder_name))):
                gfs_hour_rename1 = gfs_rename_copy(file=gfs_check_file1,
                                                year=int(l_year),month=int(l_month),
                                                day=int(l_day),src_dir=gfs_src_path,dst_dir=gfs_dst_path)
                print('GFS Data for prev hour downloaded and renamed')
                flag=False
        i=i+1
#--------------------------------------------gfs for next hour----------------------------------------------------        
    i=0
    flag=True
    while((flag==True) & (i<12)):
        next_latest_datetime = latest_datetime+timedelta(days=-i)
        gfs_inp_1,gfs_inp_2,gfs_check_file1,gfs_check_file2 = gfs_input(latest_datetime=next_latest_datetime)
        if (gfs_check_file2 in os.listdir(os.path.join(gfs_path,gfs_folder_name))):
            gfs_hour_rename2 = gfs_rename_copy(file=gfs_check_file2,
                                                year=int(l_year),month=int(l_month),
                                                day=int(l_day),src_dir=gfs_src_path,dst_dir=gfs_dst_path)
            flag=False
        else:
            print('GFS Data Does not Exist, Downloading GFS Data for next hour')
            os.chdir(gfs_src_path)
            os.system(gfs_inp_2)
            if (gfs_check_file2 in os.listdir(os.path.join(gfs_path,gfs_folder_name))):
                gfs_hour_rename2 = gfs_rename_copy(file=gfs_check_file2,
                                                year=int(l_year),month=int(l_month),
                                                day=int(l_day),src_dir=gfs_src_path,dst_dir=gfs_dst_path)
                print('GFS Data for next hour downloaded and renamed')
                flag=False
        i=i+1
#-----------------------------------------------------------------------------------------------------------------------       
            
    print('gfs data is prepared for further analysis')


    os.makedirs(os.path.join(hrit_path,latest_time_step), exist_ok = True)
    if (len(os.listdir(os.path.join(hrit_path,latest_time_step)))!=90):
        # latest,product,token = hrit_manual(Y=Y,m=m,d=d, H=H,M=M)
        # latest_datetime,l_day,l_month,l_year,l_hour,l_minute = hrit_latest_date(latest = product)
        # latest_time_step = product.sensing_start
        # latest_time_step = latest_time_step.strftime('%Y%m%d%H%M')
        jobID= hrit_data(latest=latest,token=token)
        hrit_unzip(path = hrit_path,ID = jobID)
        hrit_rename_copy(path=hrit_path)
        print('hrit data renamed and copied')
        time.sleep(2)
        #MSG = latest.split('-')[0]
        #print(f'file type is {MSG}')
        hrit_zip_del(folder = latest_time_step,ID = jobID)
    else:
        print('hrit data already renamed and copied')


    hrit_file = os.listdir(os.path.join(hrit_path,latest_time_step))[1]
    if (hrit_file.find('MSG1')!=-1):
        MSG = 'MSG1'
    elif (hrit_file.find('MSG2')!=-1):
        MSG = 'MSG2'
    # MSG = hrit_file.split('-')[2]
    print(f'file type is {MSG} ')
    if (MSG=='MSG1'):
        os.chdir(config)
        config_prompt = 'cp sat_conf_file.MSG1 sat_conf_file'
        os.system(config_prompt)
    elif (MSG =='MSG2'):
        os.chdir(config)
        config_prompt = 'cp sat_conf_file.MSG2 sat_conf_file'
        os.system(config_prompt)
    #else:
#     print('latest file in different format: other than MSG1 or MSG2')
#         break
    os.chdir(Bin)
    link = f'ln {hrit_path}{latest_time_step}/*{latest_time_step}* {Sat_data}'
    print(link)
    os.system(link)
    log = 'tm '+'log'
    os.system(log)
    print("fetching latest log file")
    list_of_files = glob.glob(Log+'*.log')
    latest_log_file = max(list_of_files,key=os.path.getctime)
    latest_log_filename = os.path.basename(latest_log_file)
    print(f"latest log file is {latest_log_filename}")
    check_slot = str(l_year)+str(l_month)+str(l_day)+'T'+str(l_hour)+str(l_minute)+'00Z'
    export_folders = ['CMA','CT','CMIC','CRR','CRRPh','CTTH']
    curr_datetime = datetime.now()
    curr_time = curr_datetime.time()
    max_time = (curr_datetime+timedelta(minutes=4)).time()
    for exp in export_folders:
        export_file_check = 'S_NWC_'+exp+'_'+MSG+'_IODC-VISIR_'+check_slot+'.nc'
        flag = True
        while flag==True:
            # print(f"export file check is {export_file_check}")
            print("RUNNING")
            print(f"current time is {curr_time}")
            if (export_file_check in os.listdir(os.path.join(Export,exp))) or (curr_time>=max_time):
                # print(f"export files generated in {exp} folder")
                flag = False
                print(f"SUCCESS {export_file_check}")
                pass
            else:
                time.sleep(3)
                curr_time =(datetime.now()+timedelta(seconds=3)).time()

    time.sleep(5)
    check_string = 'All PGEs for Slot {check_slot} have concluded'
    # print("files are generated in export folders")
    # print('program succesfully executed')
    with open(os.path.join(Log,latest_log_filename)) as file:
        if check_string in file.read():
            print('program succesfully executed')
        else:
            print('program not executed')
    export_date = str(l_year)+str(l_month)+str(l_day)
    #prev_export_date = 
    os.makedirs(os.path.join(external_folder_path,export_date),exist_ok = True)
    print(external_folder_path)
    # time.sleep(10)
    # print('wait for 10 seconds till export files are getting ready')

    # export_folders = ['CI','CMA','CMIC','CRR','CRRPh','CT','CTTH','HRW','PC']
    # check_slot1 = str(l_year)+str(l_month)+str(l_day)+'T'+str(l_hour)+str(l_minute)+'00Z'
    #print(check_slot1)
    print('waiting for 1 min so that program can be executed')
    time.sleep(60)
    print('file copying to export folder')
    export_folders1 = ['CMA','CT','CMIC','CRR','CRRPh','CTTH','PC']
    for i in export_folders1:
        new_path = os.path.join(Export,i)
        items = os.listdir(new_path)
        #items = [item for item in items if '.nc' or '.bufr' in item ]
        items = [item for item in items if '.nc' in item]
        items = [item for item in items if '.txt' not in item]
        items = [item for item in items if '.nc.stat' not in item]

        # items = [item for item in items if 'TRAG' not in item]
        for item in items :
            if (item.find(check_slot) != -1):
                try:
                    shutil.copy(os.path.join(new_path,item),os.path.join(external_folder_path,export_date))
                    print(f'{i} file copied to external folder')
                except:
                    pass
            else:
                pass
    #time.sleep(5)
    print('Moving HRW files to local export folder')
    items = os.listdir(os.path.join(Export,'HRW'))
    items = [item for item in items if '.txt' not in item]
    items = [item for item in items if '.nc.stat' not in item]
    for item in items:
        try:
            shutil.move(os.path.join(Export,'HRW',item),os.path.join(external_folder_path,export_date))
            #print('HRW file moved')
        except:
            print('HRW files not created')
            pass
    print('Moving EXIM')
    items = os.listdir(os.path.join(Export,'EXIM'))
    items = [item for item in items if '.nc' in item]
    external_exim_path = os.path.join(external_folder_path,'EXIM',export_date)
    os.makedirs(external_exim_path, exist_ok= True)
    # os.makedirs(os.path.join())
    for item in items:
        try:
            shutil.move(os.path.join(Export,'EXIM',item),external_exim_path)
            #print('EXIM files moved')
        except:
            print('EXIM is not created')
            pass
    # for exp in export_folders1:
    # 	export_file_check = 'S_NWC_'+exp+'_'+MSG+'_IODC-VISIR_'+check_slot+'.nc'
    # 	if (export_file_check in os.listdir(os.path.join(external_folder_path,export_date))):
    # 		print(f"{exp} file present in local export folder")
    # 	#	pass
    # 	else:
    # 		print(f"{exp} file not present in local export folder")
    #deleting NWP files
    nwp_files = glob.glob(NWP_data+'*.grib')
    # nwp_check0 = 'T00:00:00Z_0'+gfs_hour_rename0
    nwp_check1 = 'T00:00:00Z_0'+gfs_hour_rename1
    nwp_check2 = 'T00:00:00Z_0'+gfs_hour_rename2
    for f in nwp_files:
        if (f.find(nwp_check1)!=-1) or (f.find(nwp_check2)!=-1):
            print('No extra file found in SAFNWC NWP folder')
            pass
        else:
            print(f)
            os.remove(f)
            print(f'SAFNWC NWP import file {f} deleted')
    #print(f)
        # os.remove(f)

    pre_datetime = latest_datetime + timedelta(minutes = -60)
    pre_time_step = pre_datetime.strftime('%Y%m%d%H%M')
    p_year = pre_datetime.year
    p_month = str(pre_datetime.month).zfill(2)
    p_day = str(pre_datetime.day).zfill(2)
    p_hour = str(pre_datetime.hour).zfill(2)
    p_minute = str(pre_datetime.minute).zfill(2)
    pre_export_date = str(p_year)+str(p_month)+str(p_day)


    # pre_CT_check = 'S_NWC_CT_MSG2_IODC-VISIR_'+str(p_year)+str(p_month)+str(p_day)+'T'+str(p_hour)+str(p_minute)+'00Z.nc'
    # print('CT prev 2 timestep file: ',pre_CT_check)
    print(f'deleting raw data from SAFNWC import for {pre_time_step} : 4 time step previous to latest ')
    if (CT_check(day = p_day,month=p_month,year=p_year,hour = p_hour,minute=p_minute,time_step = pre_export_date)):
        check_slot1 = str(p_year)+str(p_month)+str(p_day)+'T'+str(p_hour)+str(p_minute)+'00Z'
        for exp in export_folders1:
            in_its = os.listdir(os.path.join(Export,exp))
            for i in in_its:
                if (i.find(check_slot1)!=-1):
                    os.remove(os.path.join(Export,exp,i))

        print('SAFNWC export files removed')

        sat_files = glob.glob(Sat_data+'*IODC*')
        for f in sat_files:
            if (f.find(pre_time_step)!=-1):
                #print(f)
                os.remove(f)
        print('sat file removed')
        #try:
        #	shutil.rmtree(os.path.join(Sat_data_archeive,pre_time_step))
        #	print('sat archeive folder deleted')
        #except:
        #	pass
        try:
            shutil.rmtree(os.path.join(hrit_path,pre_time_step))
            print('hrit previous data folder deleted')
        except:
            pass

    else:
        print(f'CT of previous time step {pre_time_step} not found')

    # print('wating for 60 sec so that program can be completed')
    # time.sleep(60)

else:
    print(f'time step {latest_time_step} CT file already present')

#run_time = datetime.now().time()

    try:
        shutil.rmtree(os.path.join(hrit_path,latest_time_step))
        print('hrit latest data folder deleted')
    except:
        pass

try:
    customisation_hrit_del(token=token)
except:
    pass
