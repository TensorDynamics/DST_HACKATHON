import pandas as pd
import numpy as np
import sys
import glob
from datetime import datetime,timedelta,time
import os
from tqdm import tqdm
import sys
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from configs import db_config
from configs.path_config import *
from sqlalchemy import create_engine
from pandas import read_sql_query
from d6tstack.utils import pd_to_psql
import warnings
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


def create_db_connection(dbname,
                         host,
                         port,
                         user,
                         password):
    """
    Establishes connection to database
    :param dbname: database name
    :param host: endpoint
    :param port: port
    :param user: username
    :param password: password
    """
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(connection_string)

db_connection = None

def get_sql_connection():
    print("Opening DB connection")
    global db_connection

    if db_connection is None:
        db_connection = create_db_connection(dbname=db_config.dbname,
                                             host=db_config.host,
                                             port = db_config.port,
                                             user = db_config.user,
                                             password=db_config.password)

    return db_connection

class SiteDataExtractor:

    def __init__(self, 
                 db_connection, 
                 table_name,
                 schema_name,
                 site_name,
                 today_date,
                 site_date_label='timestamp',
                 site_column_label='site_name',
                 eng='pandas'):
        """
        General purpose class to extract data for a specific site from the schema.table address of the db connection
        :param db_connection: sql alchemy db connection
        :param table_name: str, table name in the db to extract from
        :param schema_name: str, schema name in the db to extract from
        :param site_column_label: str, site identifier column label i.e. name of column containing the site names
        :param eng: str, ['connectorx', 'pandas']. Defaults to pandas. Can show time improvements in production.
        """
        self.db_connection = db_connection
        self.table_name = table_name
        self.schema_name = schema_name
        self.site_name = site_name
        self.today_date = today_date
        self.site_column_label = site_column_label
        self.site_date_label = site_date_label
        self.eng = eng
        self.db_str = 'postgresql://admin123:tensor123@tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com:5432/postgres'

    def parse_query(self, query):
        """ parses sql query via the desired engine """
        if self.eng.lower() == 'pandas':
            return pd.read_sql_query(sql=query, con=self.db_connection)
        elif self.eng.lower() == 'connectorx':
            return cx.read_sql(conn=self.db_str, query=query)
        else:
            raise NotImplementedError(f"Only 'pandas' and 'connectorx' are valid choices for eng in __init__ call. {self.eng} was provided.")

    def read_data(self):
        """ reads data for a specific site from database """
        query = f"select * from {self.schema_name}.{self.table_name}"                 f" where {self.site_column_label} = '{self.site_name}' and {self.site_date_label}::date = '{self.today_date}'"
        return self.parse_query(query=query)

def utc_to_ist(data_frame, time_col='timestamp'):
    temp_data = data_frame.copy()
    temp_data[time_col] = pd.to_datetime(temp_data[time_col], utc=True)
    return temp_data.set_index(time_col).tz_convert('Asia/Kolkata').reset_index()

def remove_timezone(data_frame, time_col='timestamp'):
    temp_data = data_frame.copy()
    temp_data[time_col] = temp_data[time_col].dt.tz_localize(None)
    return temp_data

def extract_avail_timeseries_for_variable(data_frame, time_col, variable='ct'):
    return data_frame[[time_col, variable]].set_index(time_col)
def pre_process_satellite_data(data_frame, time_col='timestamp', variable='ct'):
    output = (data_frame
              .copy()
              .pipe(utc_to_ist, time_col)
              .pipe(remove_timezone, time_col)
              .pipe(extract_avail_timeseries_for_variable, time_col, variable)
              .sort_index())
    return output

def roundTime(dt, roundTo=15*60):
   """
   Round a datetime object to any time lapse in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   """
   if dt != None :
        dt = datetime.now()
        seconds = (dt.replace(tzinfo=None) - dt.min).seconds
        rounding = (seconds+roundTo/2) // roundTo * roundTo
        return dt + timedelta(0,rounding-seconds,-dt.microsecond)

def day_ahead(db_connection,
              table,
              schema,
              date,
              site):
    
    day_ahead_data = SiteDataExtractor(db_connection=db_connection, 
                                       table_name=table,
                                       schema_name=schema, 
                                       today_date = str(date),
                                       site_name=site).read_data() 
    if day_ahead_data.shape[0]>0:
        
        day_ahead_data['timestamp'] = day_ahead_data['timestamp'].dt.round('15min')
        day_ahead_data = day_ahead_data.drop_duplicates(subset=['timestamp','site_name'], keep='last').set_index('timestamp')
        day_ahead_data = day_ahead_data[['swdown_wpm2','site_name']]
        day_ahead_data = day_ahead_data.rename(columns={'swdown_wpm2':'ghi_predicted(w/m2)'})
        day_ahead_data['forecast_method'] = len(day_ahead_data.index)*['day_ahead']
        day_ahead_data['time'] = day_ahead_data.index.time
        print(f"fetched day_ahead for {site} with {day_ahead_data.shape[0]} rows")
        return day_ahead_data.sort_index()
    else:
        return pd.DataFrame()
    
    
def clearsky(db_connection,
             table,
             schema,
             date,
             site,
             date_label='times'):
    
    clearsky_data = SiteDataExtractor(db_connection=db_connection, 
                                       table_name= table,
                                       schema_name=schema,
                                       site_date_label=date_label,
                                       today_date = str(date),
                                       site_name=site).read_data() 
    if clearsky_data.shape[0]>0:
        clearsky_data['times'] = clearsky_data[date_label].dt.round('15min')
        clearsky_data = clearsky_data.drop_duplicates(subset=[date_label,'site_name'], keep='last').set_index('times')
        clearsky_data = clearsky_data[['swdnbc','site_name']]
        clearsky_data = clearsky_data.reset_index().rename(columns={'swdnbc':'cs',
                                                                    'times':'timestamp'}).set_index('timestamp')
        print(f"fetched clearsky for {site} with {clearsky_data.shape[0]} rows")
        return clearsky_data.sort_index()
    else:
        return pd.DataFrame()
    
def satellite_ct(db_connection,
                 table,
                 schema,
                 timestamp,
                 date,
                 site,
                 satellite_time_col = 'timestamp',
                 satellite_ct_col = 'ct'):
    
    date_utc = (timestamp-timedelta(hours=5.50)).date()
    
    satellite_data = SiteDataExtractor(db_connection=db_connection, 
                                       table_name=table,
                                       schema_name=schema, 
                                       today_date = str(date_utc),
                                       site_name=site).read_data()
    if (satellite_data.shape[0]>0):
        satellite_ct_series = pre_process_satellite_data(data_frame=satellite_data, 
                                                         time_col= satellite_time_col,
                                                         variable=satellite_ct_col)
        satellite_ct_series = satellite_ct_series.reset_index().drop_duplicates(subset=satellite_time_col, keep='last').set_index(satellite_time_col)
        satellite_ct_series = satellite_ct_series.dropna(subset=satellite_ct_col)
        print(f"Fetched satellite data for {site} with {satellite_ct_series.shape[0]} rows")
        
        return satellite_ct_series.shift(-1).sort_index()
    else:
        return pd.DataFrame()
    


def slice_forecast(df,start,end):
    temp_df = df.copy()
#     temp['time'] = temp.index.time
    temp_df = temp_df[(temp_df['time']>=start) &
                        (temp_df['time']<=end)]
    return temp_df

def real_data(db_connection,
              table,
              schema,
              date,
              site,
              time_col = 'timestamp',
              rad_col = 'ghi(w/m2)',
              power_col = 'power(kw)'):
    
    real_data = SiteDataExtractor(db_connection=db_connection, 
                                       table_name= table,
                                       schema_name= schema, 
                                       today_date = str(date),
                                       site_name=site).read_data()
    if (real_data.shape[0]>0):
        real_data = real_data[[time_col,rad_col]].set_index(time_col)
        real_data.index = pd.to_datetime(real_data.index)
        real_data['time'] = real_data.index.time
        print(f"real data is fetched for {site} with {real_data.shape[0]} rows")
        return real_data
    else:
        return pd.DataFrame()

def read_sample_table_from_DB(schema,table,con):
    query = f"select * from {schema}.{table} limit 1 "
    df = pd.read_sql_query(sql=query,con=con, index_col='timestamp')
    return df.columns

def fill_values_to_zero_in_night(df,
                                 sunrise = time(5,30),
                                 sunset = time(17,30),
                                 target_col= 'ghi(w/m2)'):
    
    """
    time_series index is datetime index
    """
    df[target_col] = df[target_col].apply(lambda x: 0 if x < 0 else x)
    for i in df.index:
        if ((sunset < (i.time())) | ((i.time()) < sunrise)):
            #         print(i)
            df.at[i, target_col] = 0
    return df

def clearsky_roll_and_shift(clearsky_df,
                           window=3,
                           target_col = 'cs',
                           target_col_new_name = 'cs_rolled'):
    """
    clearsky_df index is datetime index.
    window is number of timeblock for which dataframe have to roll
    """
    temp_df = clearsky_df.copy()
    for ts in tqdm(temp_df.index):
        ts_date = ts.date()
        ts_time = ts.time()
        last_n_block_ago_timestamp = ts - pd.Timedelta(minutes=window*15)
        req_cs_series = temp_df.loc[(temp_df.index >= last_n_block_ago_timestamp) & 
                                     (temp_df.index <= ts)][target_col]
        req_cs = req_cs_series.mean()
        temp_df.loc[ts,target_col_new_name] = req_cs
    
    temp_df[target_col_new_name] = temp_df[target_col_new_name].shift(-2)
    temp_df = fill_values_to_zero_in_night(df=temp_df,
                                           sunrise=time(5,0),
                                           sunset=time(18,0),
                                           target_col=target_col_new_name)
    return temp_df.drop(columns=target_col).sort_index()

def ct_postprocessing(satellite_df,
                     window=3,
                     target_col = 'ci'):
    """
    satellite_df index is datetime index.
    cloud index would be rolled for previous 3 timeblocks
    window is number of timeblock for which cloud index have to roll
    """
    temp_df = satellite_df.copy()
    for ts in tqdm(temp_df.index):
        ts_date = ts.date()
        ts_time = ts.time()
        last_n_block_ago_timestamp = ts - pd.Timedelta(minutes=window*15)
        req_ci_series = temp_df.loc[(temp_df.index >= last_n_block_ago_timestamp) & 
                                     (temp_df.index <= ts)][target_col]
        ci_value = req_ci_series.mean()
        temp_df.loc[ts,'ci_past_mean'] = ci_value
    return temp_df.drop(columns='ci').sort_index()
        
def nowcasting(satellite_df,
               clearsky_df,
               site,
               date,
               clearsky_col = 'cs_rolled',
               cloud_index_column = 'ci_past_mean',
               ct_column = 'ct'):
    satellite_df = satellite_df.dropna(subset=ct_column)
    nowcasting_df = pd.DataFrame()
    nowcasting_df.index = pd.date_range(start=date,periods=96,freq='15min' )
    ct_missing = {}
    ct_missing_timestamp = []
    for ts in tqdm(nowcasting_df.index):
        try:
            req_cs = clearsky_df.loc[ts,clearsky_col]
            req_ci = satellite_df.loc[ts,cloud_index_column]
            nowcasting_df.loc[ts,'ghi_predicted(w/m2)'] = (1-req_ci)*req_cs
            nowcasting_df.loc[ts,'site_name'] = site
            nowcasting_df.loc[ts,'forecast_method'] = 'sat_forecast'
            nowcasting_df.loc[ts,'time'] = ts.time()
        except:
            ct_missing_timestamp.append(ts)
    ct_missing[site] = ct_missing_timestamp
#             print(f"clearsky value or ci value is missing for timestamp {ts}")
    return nowcasting_df.sort_index()
    
def fill_missing_with_next_and_prev_mean(df,
                                         target_col = 'ghi_predicted(w/m2)'):
    """
    input dataframe index is datetime index
    """
    temp_df = df.copy()
    for i in tqdm(temp_df.index):
        req_present_value = temp_df.loc[i,target_col]
        if pd.isnull(req_present_value):
            try:
                req_prev_value = temp_df.loc[i+timedelta(minutes=-15),target_col]
                req_next_value = temp_df.loc[i+timedelta(minutes=15),target_col]
                mean_value = np.mean([req_prev_value,req_next_value])
                temp_df.loc[i,target_col] = mean_value
            except:
                pass
    return temp_df.sort_index()

def week_ahead_df(con,
                  site,
                  date,
                 schema=db_config.wrf_schema,
                 table=db_config.week_ahead_view,
                 ghi_column = 'swdown',
                 site_label_column = 'site_name',
                 temp_column = 'temp'):
    query = f"select * from {schema}.{table}" \
            f" where site_name = '{site}'"
    week_ahead_df = pd.read_sql_query(sql=query,con=db_connection,index_col='timestamp')
    week_ahead_df.index = week_ahead_df.index.round('15min')
    return week_ahead_df[[ghi_column,temp_column,site_label_column]].loc[str(date):]
