from configs import db_config

temp_gran = 'D'

db_map = {
           'actual': {'table': db_config.ip_actual_site_table, 'schema': db_config.ip_actual_data_schema},
           'wrf': {'table': db_config.wrf_view, 'schema': db_config.wrf_schema},
         }

col_agg_map = {
                'actual': {'power(kw)': 'sum', 'ghi(w/m2)': 'sum', 'temp(c)': 'mean', 'ws': 'mean'},
                'wrf': {'temp_c': 'mean', 'wind_speed_10m_mps': 'mean', 'wind_direction_in_deg': 'mean', 'swdown_wpm2': 'sum'},
                }


time_col_map = {'actual': 'timestamp', 'wrf': 'timestamp'}

model_feat_config_temp = {
                           'target':   {'actual': 'temp(c)'},
                           'features': {'wrf': 'temp_c'}
                          }


model_feat_config_wind = {
                           'target':   {'actual': 'ws'},
                           'features': {'wrf': ['wind_speed_10m_mps', 'temp_c', 'wind_direction_in_deg']}
                          }

model_feat_config_wind_power = {
                               'target':   {'actual': 'power(kw)'},
                               'features': {'wrf': ['wind_speed_10m_mps', 'temp_c', 'wind_direction_in_deg'],
                                            'actual': 'ws'}
                              }
