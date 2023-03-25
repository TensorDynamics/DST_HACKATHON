"""
database config
"""
dbname = 'postgres'
host = 'tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com'
port = 5432
user = 'admin123'
password = 'tensor123'
#wrf_schema = 'td_wrf'
wrf_schema = 'site_actual'
#wrf_stg_table = 'td_wrf_stg'
wrf_stg_table = 'clearsky_from_wrf'
wrf_view = 'v_wrf_data'
testing_schema = 'samples'
testing_table = 'test_table'
testing_view = 'v_sample'
configs_schema = 'configs'
site_table = 'site_config'
ip_actual_site_table = 'site_actual'
ip_actual_data_schema = 'site_actual'
op_day_head_wind_forecast_table = 'wrf_wind_forecast_da'
