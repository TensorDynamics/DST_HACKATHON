#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
from datetime import datetime,timedelta,time
import os
from tqdm import tqdm
# sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from configs import db_config
from configs.path_config import *
from sqlalchemy import create_engine
from pandas import read_sql_query
from d6tstack.utils import pd_to_psql


# In[ ]:


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


class SiteDataExtractor:

    def __init__(self, 
                 db_connection, 
                 table_name,
                 schema_name,
                 site_name,
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
#         self.today_date = today_date
        self.site_column_label = site_column_label
#         self.site_date_label = site_date_label
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
        query = f"select * from {self.schema_name}.{self.table_name} where {self.site_column_label} = '{self.site_name}'"
        return self.parse_query(query=query)
    
def utc_to_ist(df,time_col='timestamp'):
    temp_df = df.copy()
    temp_df[time_col] = pd.to_datetime(temp_df[time_col],utc=True)
    return temp_df.set_index(time_col).tz_convert('Asia/Kolkata').reset_index()

def remove_timezone(df,time_col='timestamp'):
    temp_df = df.copy()
    return temp_df.set_index(time_col).tz_localize(None).reset_index()

def extract_avail_timeseries(df,time_col='timestamp', variable='ct'):
    temp_df = df[[time_col,variable]]
    return temp_df

def satellite_preprocessing(df,time_col='timestamp', variable='ct'):
    temp_df = (df
               .copy()
               .pipe(utc_to_ist,time_col)
               .pipe(remove_timezone,time_col)
               .pipe(extract_avail_timeseries,time_col,variable))
    return temp_df
    

def fill_missing_with_next_and_prev_mean(time_series,target_col = 'ghi(w/m2)'):
    temp_df = time_series.copy()
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
    return temp_df

def fill_values_to_zero_in_night(df,
                                 sunrise = time(5,30),
                                 sunset = time(17,30),
                                 target_col= 'ghi(w/m2)'):
    df[target_col] = df[target_col].apply(lambda x: 0 if x < 0 else x)
    for i in df.index:
        if ((sunset < (i.time())) | ((i.time()) < sunrise)):
            #         print(i)
            df.at[i, target_col] = 0
    return df


# In[1]:


total_sites = ['Hisar','Rewa','Ichhawar','Siddipet','Barod','Aurad','SPP1','SPP2','SPP3','SPP5']


# In[ ]:


final_df = pd.DataFrame()
cs_wrf = {}
ct = {}
for site in tqdm(total_sites):
    print(f"simulation site is {site}")
    db_connection = create_db_connection(host=db_config.host,
                                     dbname=db_config.dbname,
                                     port=db_config.port,
                                     user=db_config.user,
                                     password=db_config.password)

    real_ = SiteDataExtractor(db_connection=db_connection,
                               table_name=db_config.site_actual_table,
                               schema_name=db_config.site_actual_schema,
                               site_name = site).read_data()
    real_df = real_.drop_duplicates(subset='timestamp',keep='last')
    real_df = real_df.set_index('timestamp').sort_index().resample('15min',closed='right',label='right').mean()
    real_df1 = fill_values_to_zero_in_night(real_df)
    real_df2 = fill_missing_with_next_and_prev_mean(real_df1)
    real_df3 = real_df2[['ghi(w/m2)']]
    real_df3['site_name'] = len(real_df3.index)*[site]
    
    real_df4 = real_df3.copy()
    
    print("-----------------fetching clearksy from wrf------------")
    
    wrf_query = f"select * from {db_config.site_actual_schema}.{db_config.wrf_cs_table} "                f" where site_name = '{site}'"
    site_cs_wrf = pd.read_sql_query(wrf_query,con=db_connection)
    site_cs_wrf1 = site_cs_wrf.drop_duplicates(subset='times',keep='last')
    site_cs_wrf1['times'] = site_cs_wrf1['times'].dt.round('15min')
    site_cs_wrf1 = site_cs_wrf1.set_index('times')
    cs_wrf[site] = site_cs_wrf1
    print('----------------fetching satellite data')
    db_connection = create_db_connection(dbname='postgres',
                                     host='tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com',
                                     port=5432,
                                     user='admin123',
                                     password='tensor123')
    site_ct_ = SiteDataExtractor(db_connection=db_connection,
                               table_name=db_config.satellite_ip_view,
                               schema_name=db_config.satellite_schema,
                               site_name = site).read_data()
    site_ct = satellite_preprocessing(df=site_ct_,
                                    time_col='timestamp',
                                    variable='ct')
    print(f"satellite data fetched for {site} with {site_ct.shape[0]} rows")
    ct[site] = site_ct
    site_ct = site_ct.set_index('timestamp')
    for ts in tqdm(real_df4.index):
        try: 
            real_df4.loc[ts,'ct'] = site_ct.loc[ts,'ct']
        except:
            pass
    
#     real_df4['ct_flag'] = real_df4['ct'].map(ct_flag_dict)
    for ts in tqdm(real_df4.index):
        try:
            real_df4.loc[ts,'cs_from_wrf'] = site_cs_wrf1.loc[ts,'swdnbc']
        except:
            pass
        
        try:
            real_df4.loc[ts,'ci_from_wrf'] = 1-(real_df4.loc[ts,'ghi(w/m2)']/real_df4.loc[ts,'cs_from_wrf'])
        except:
            pass
    final_df = pd.concat([final_df,real_df4],axis=0)


# In[ ]:


final_df1 = final_df.copy()
final_df2 = final_df1[~pd.isnull(final_df1['cs_wrf'])]
final_df3 = fill_values_to_zero_in_night(df=final_df2,target_col='cs_wrf')
dist_analysis_df = final_df7.reset_index().copy()
dist_analysis_df1 = dist_analysis_df[dist_analysis_df['ci_wrf'].between(0.1,1, inclusive=True)]
ct_ci_df = dist_analysis_df1.groupby('ct', as_index=False).agg(min_ci=('ci_wrf', 'min'),
                                                              max_ci=('ci_wrf', 'max'),
                                                              avg_ci=('ci_wrf', 'mean'),
                                                              median_ci=('ci_wrf', 'median'))
ct_ci_df.to_csv('ct_ci.csv',index=False)

