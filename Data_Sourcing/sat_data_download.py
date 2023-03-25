from Data_Sourcing.path_config import *
from datetime import datetime,timedelta
import eumdac
#import datetime
import shutil
import time
import fnmatch
import os
import zipfile
import sys
# from inputimeout import inputimeout,TimeoutOccurred
import glob


#EUMATSAT ID-------------skycaster------------------------------------------

def hrit_latest():
    consumer_key = ''
    consumer_secret = ''
    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)
    datastore = eumdac.DataStore(token)
    datastore.collections
    selected_collection = datastore.get_collection('EO:EUM:DAT:MSG:HRSEVIRI-IODC')
    latest = selected_collection.search().first()
    print(latest)
    return latest,token

def hrit_latest_date(latest):
    latest_timestep = latest.sensing_start
    latest_datetime = datetime.strptime(latest_timestep.strftime("%Y%m%d%H%M"),"%Y%m%d%H%M")
    l_day = str(latest_datetime.day).zfill(2)
    l_month = str(latest_datetime.month).zfill(2)
    l_year = latest_datetime.year
    l_hour = str(latest_datetime.hour).zfill(2)
    l_minute = str(latest_datetime.minute).zfill(2)
    return latest_datetime,l_day,l_month,l_year,l_hour,l_minute

def CT_check(day,month,year,hour,minute,time_step):
    CT_check_file1 = 'S_NWC_CT_MSG1_IODC-VISIR_'+str(year)+str(month)+str(day)+'T'+str(hour)+str(minute)+'00Z.nc'
    CT_check_file2 = 'S_NWC_CT_MSG2_IODC-VISIR_'+str(year)+str(month)+str(day)+'T'+str(hour)+str(minute)+'00Z.nc'
    os.makedirs(os.path.join(external_folder_path,time_step), exist_ok = True)
    return ((CT_check_file1 in os.listdir(os.path.join(external_folder_path,time_step))) or (CT_check_file2 in os.listdir(os.path.join(external_folder_path,time_step))))

def gfs_input(latest_datetime):
    gfs_datetime = latest_datetime+timedelta(hours = -48)
    gfs_date = gfs_datetime.date()
    hour = gfs_datetime.hour
    if 0<=hour<3:
        gfs_hour0 = 33
        gfs_hour1 = 36
        gfs_hour2 = 39
    elif 3<=hour<6:
        gfs_hour0 = 36
        gfs_hour1 = 39
        gfs_hour2 = 42
    elif 6<=hour<9:
        gfs_hour0 = 39
        gfs_hour1 = 42
        gfs_hour2 = 45
    elif 9<=hour<12:
        gfs_hour0 = 42
        gfs_hour1 = 45
        gfs_hour2 = 48
    elif 12<=hour<15:
        gfs_hour0 = 45
        gfs_hour1 = 48
        gfs_hour2 = 51
    elif 15<=hour<18:
        gfs_hour0 = 48
        gfs_hour1 = 51
        gfs_hour2 = 54
    elif 18<=hour<21:
        gfs_hour0 = 51
        gfs_hour1 = 54
        gfs_hour2 = 57
    else:
        gfs_hour0 = 54
        gfs_hour1 = 57
        gfs_hour2 = 60

    g_year = str(gfs_datetime.year)
    g_month = str(gfs_datetime.month).zfill(2)
    g_date = str(gfs_datetime.day).zfill(2)
    # gfs_inp_0 = 'python'+' '+'gfs_data.py'+' '+str(g_date +' '+g_month+' '+g_year+' '+ str(gfs_hour0))
    gfs_inp_1 = '/home/tensor/miniconda3/bin/python'+' '+'gfs_download.py'+' '+str(g_date +' '+g_month+' '+g_year+' '+ str(gfs_hour1))
    gfs_inp_2 = '/home/tensor/miniconda3/bin/python'+' '+'gfs_download.py'+' '+str(g_date +' '+g_month+' '+g_year+' '+ str(gfs_hour2))


    # gfs_check_file0 = 'gfs.0p25.'+str(g_year)+str(g_month)+str(g_date)+str(12)+'.f0'+str(gfs_hour0)+'.grib2'
    gfs_check_file1 = 'gfs.0p25.'+str(g_year)+str(g_month)+str(g_date)+str(12)+'.f0'+str(gfs_hour1)+'.grib2'
    gfs_check_file2 = 'gfs.0p25.'+str(g_year)+str(g_month)+str(g_date)+str(12)+'.f0'+str(gfs_hour2)+'.grib2'
    return gfs_inp_1,gfs_inp_2,gfs_check_file1,gfs_check_file2


def gfs_input_backup(latest_datetime):
    gfs_datetime = latest_datetime+timedelta(hours = -72)
    gfs_date = gfs_datetime.date()
    hour = gfs_datetime.hour
    if 0<=hour<3:
        gfs_hour1 = 36+24
        gfs_hour2 = 39+24
    elif 3<=hour<6:
        gfs_hour1 = 39+24
        gfs_hour2 = 42+24
    elif 6<=hour<9:
        gfs_hour1 = 42+24
        gfs_hour2 = 45+24
    elif 9<=hour<12:
        gfs_hour1 = 45+24
        gfs_hour2 = 48+24
    elif 12<=hour<15:
        gfs_hour1 = 48+24
        gfs_hour2 = 51+24
    elif 15<=hour<18:
        gfs_hour1 = 51+24
        gfs_hour2 = 54+24
    elif 18<=hour<21:
        gfs_hour1 = 54+24
        gfs_hour2 = 57+24
    else:
        gfs_hour1 = 57+24
        gfs_hour2 = 60+24

    g_year = str(gfs_datetime.year)
    g_month = str(gfs_datetime.month).zfill(2)
    g_date = str(gfs_datetime.day).zfill(2)
    gfs_inp_1 = '/home/ubuntu/anaconda3/bin/python'+' '+'gfs_download.py'+' '+str(g_date +' '+g_month+' '+g_year+' '+ str(gfs_hour1))
    gfs_inp_2 = '/home/ubuntu/anaconda3/bin/python'+' '+'gfs_download.py'+' '+str(g_date +' '+g_month+' '+g_year+' '+ str(gfs_hour2))

    gfs_check_file1 = 'gfs.0p25.'+str(g_year)+str(g_month)+str(g_date)+str(12)+'.f0'+str(gfs_hour1)+'.grib2'
    gfs_check_file2 = 'gfs.0p25.'+str(g_year)+str(g_month)+str(g_date)+str(12)+'.f0'+str(gfs_hour2)+'.grib2'
    return gfs_inp_1,gfs_inp_2,gfs_check_file1,gfs_check_file2

def gfs_rename_copy(file,year,month,day,src_dir,dst_dir):
    
    gfs_hour_curr = int(file.split('.')[-2][-2:])
    gfs_hour_rename = str(gfs_hour_curr+12-48).zfill(2)
    rename_string = 'S_NWC_NWP_'+str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)+'T00:00:00Z_0'+gfs_hour_rename+'.grib'
    if (rename_string in os.listdir(NWP_data)):
        # print(f'gfs file {gfs_check_file1} exist')
        pass
    else:
        print('GFS files are not present in SAFNWS NWP import')
        shutil.copy(os.path.join(src_dir,file),os.path.join(dst_dir,rename_string))
        print("GFS file renamed and copied")
        #print('GFS files not present in local GFS folder')

    return gfs_hour_rename    

def gfs_rename_copy_backup(file,year,month,day,src_dir,dst_dir):
    
    gfs_hour_curr = int(file.split('.')[-2][-2:])
    gfs_hour_rename = str(gfs_hour_curr+12-72).zfill(2)
    rename_string = 'S_NWC_NWP_'+str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)+'T00:00:00Z_0'+gfs_hour_rename+'.grib'
    if (rename_string in os.listdir(NWP_data)):
        # print(f'gfs file {gfs_check_file1} exist')
        pass
    else:
        print('GFS files are not present in SAFNWS NWP import')
        shutil.copy(os.path.join(src_dir,file),os.path.join(dst_dir,rename_string))
        print("GFS file renamed and copied")

    return gfs_hour_rename



def hrit_data(latest,token):
    datatailor = eumdac.DataTailor(token)

    seviri_hrit = datatailor.chains.read('seviri_hrit')

    customisation = datatailor.new_customisation(latest, chain=seviri_hrit)
    status = "QUEUED"
    sleep_time = 10 # seconds

    # Customisation Loop
    while status == "QUEUED" or status == "RUNNING":
        # Get the status of the ongoing customisation
        status = customisation.status

        if "DONE" in status:
            print(f"SUCCESS")
            break
        elif "ERROR" in status or 'KILLED' in status:
            print(f"UNSUCCESS, exiting")
            break
        elif "QUEUED" in status:
            print(f"QUEUED")
        elif "RUNNING" in status:
            print(f"RUNNING")
        elif "INACTIVE" in status:
            sleep_time = max(60*10, sleep_time*2)
            print(f"INACTIVE, doubling status polling time to {sleep_time} (max 10 mins)")
        time.sleep(sleep_time)

    png = fnmatch.filter(customisation.outputs,'*')
    jobID= customisation._id

    print(f"Dowloading the NETCDF4 output of the customisation {jobID}")
    os.chdir(hrit_path)
    for i in png:
        with customisation.stream_output(i) as stream, \
            open(stream.name, mode='wb') as fdst:
            shutil.copyfileobj(stream, fdst)
    print(f"Dowloaded the HRIT output of the customisation {jobID}")
    return jobID

def hrit_data_manual(latest,token):
    datastore = eumdac.DataStore(token)
    selected_product = datastore.get_product(product_id = latest,collection_id = "EO:EUM:DAT:MSG:HRSEVIRI-IODC")
    datatailor = eumdac.DataTailor(token)
    seviri_hrit = datatailor.chains.read('seviri_hrit')
    customisation = datatailor.new_customisation(selected_product, chain=seviri_hrit)
    status = "QUEUED"
    sleep_time = 10 # seconds

    # Customisation Loop
    while status == "QUEUED" or status == "RUNNING":
        # Get the status of the ongoing customisation
        status = customisation.status

        if "DONE" in status:
            print(f"SUCCESS")
            break
        elif "ERROR" in status or 'KILLED' in status:
            print(f"UNSUCCESS, exiting")
            break
        elif "QUEUED" in status:
            print(f"QUEUED")
        elif "RUNNING" in status:
            print(f"RUNNING")
        elif "INACTIVE" in status:
            sleep_time = max(60*10, sleep_time*2)
            print(f"INACTIVE, doubling status polling time to {sleep_time} (max 10 mins)")
        time.sleep(sleep_time)

    png = fnmatch.filter(customisation.outputs,'*')
    jobID= customisation._id

    print(f"Dowloading the HRIT output of the customisation {jobID}")
    os.chdir(hrit_path)
    for i in png:
        with customisation.stream_output(i) as stream, \
            open(stream.name, mode='wb') as fdst:
            shutil.copyfileobj(stream, fdst)
    print(f"Dowloaded the HRIT output of the customisation {jobID}")

    return jobID


def hrit_unzip(path,ID):
    items= os.listdir(path)
    item = [i for i in items if 'EPCT_HRSEVIRI_'+ID in i]

    with zipfile.ZipFile(os.path.join(path,item[0]), 'r') as zip_ref:
        zip_ref.extractall(path)

def hrit_rename_copy(path):
    files = os.listdir(path)
    files = [file for file in files if 'MSG' in file]
    # print(files)
    x = files[0][-15:-3]
    folder_name = x[:8]+x[8:]
    os.makedirs(os.path.join(path,folder_name),exist_ok=True)
    folder_path = os.path.join(path,folder_name)

    for f in files:
        string = f[:18]+'IODC_'+f[23:]
        os.rename(os.path.join(path,f),os.path.join(folder_path,string))

def customisation_hrit_del(token):
    datatailor = eumdac.DataTailor(token)
    for customisation in datatailor.customisations:
        if customisation.status == 'DONE' or 'FAILED':
            print(f'Delete completed customisation {customisation} from {customisation.creation_time}.')
            customisation.delete()

def hrit_zip_del(folder,ID):
    # check_file = 'H-000-'+MSG+'__-'+MSG+'_IODC___-WV_073___-000008___-'+folder+'-__'
    # check_file = 'H-000-MSG1__-MSG1_IODC___-WV_073___-000008___-202206010030-__'
    if (len(os.listdir(os.path.join(hrit_path,folder)))==90):
        try:
            # print(f"check_file for zip delete is {check_file}")
            os.remove(os.path.join(hrit_path,'EPCT_HRSEVIRI_'+str(ID)))
            print('hrit files unzipped and zip file deleted')
        except:
            pass
    else:
        # print(f"check_file for zip delete is {check_file}")
        print("Check : files did not unzipped")
