from tqdm import tqdm
import pandas as pd

from src.funcs.site_data_io import SiteDataExtractor
from src.funcs.preprocess import (pre_process_actual_site_data,
                                  pre_process_satellite_data)


def calculate_clear_sky_n_cloud_index(time_series, window=10, radiation_col='ghi(w/m2)'):
    temp_data = time_series.copy()
    temp_data = temp_data[temp_data[radiation_col] > 0]
    if temp_data.shape[1] > 1:
        raise ValueError("Only 1 dim time series is supported while calculatin Clear Sky values.")
    temp_data['time'] = temp_data.index.time
    for ts in tqdm(temp_data.index):
        ts_date = ts.date()
        ts_time = ts.time()
        ts_rad_value = temp_data.loc[ts, radiation_col]
        # FILTER FOR LAST N DAYS FOR CS VALUE
        last_n_days_ago_date = ts_date - pd.Timedelta(days=window)
        req_data = temp_data.loc[(temp_data.index.date >= last_n_days_ago_date) &
                                 (temp_data.index.date <= ts_date)]
        req_data = req_data[req_data['time'] == ts_time]
        cs_value = req_data[radiation_col].max()
        ci_value = 1 - (ts_rad_value / cs_value)

        temp_data.loc[ts, 'cs'] = cs_value
        temp_data.loc[ts, 'ci'] = ci_value
    return temp_data.drop(['time', radiation_col], axis=1)


def get_site_ct_vs_ci_data(site_name,
                           db_connection,
                           satellite_table_name,
                           satellite_schema,
                           site_actual_table,
                           site_actual_schema,
                           clear_sky_window_len_days=10,
                           satellite_ct_col='ct',
                           satellite_time_col='timestamp',
                           site_actual_radiation_col='ghi(w/m2)',
                           site_actual_time_col='timestamp'):
    satellite_data = SiteDataExtractor(db_connection=db_connection,
                                       table_name=satellite_table_name,
                                       schema_name=satellite_schema,
                                       site_name=site_name).read_data()
    print(f"Fetched satellite data for {site_name} with {satellite_data.shape[0]} rows")

    site_actual_data = SiteDataExtractor(db_connection=db_connection,
                                         table_name=site_actual_table,
                                         schema_name=site_actual_schema,
                                         site_name=site_name).read_data()
    print(f"Fetched actual site data for {site_name} with {site_actual_data.shape[0]} rows")

    satellite_ct_series = pre_process_satellite_data(data_frame=satellite_data,
                                                     time_col=satellite_time_col,
                                                     variable=satellite_ct_col)

    site_ghi_series = pre_process_actual_site_data(data_frame=site_actual_data,
                                                   time_col=site_actual_time_col,
                                                   variable=site_actual_radiation_col)

    if pd.merge(left=satellite_ct_series,
                right=site_ghi_series,
                left_index=True,
                right_index=True).shape[0] > 0:

        cs_ci_series = calculate_clear_sky_n_cloud_index(time_series=site_ghi_series,
                                                         window=clear_sky_window_len_days,
                                                         radiation_col=site_actual_radiation_col)
        print(f"Caluclated CI for {site_name}")
        site_final_data = pd.merge(left=satellite_ct_series,
                                   right=cs_ci_series,
                                   left_index=True,
                                   right_index=True)
        site_final_data['site_name'] = site_name
        return site_final_data, cs_ci_series
    else:
        return pd.DataFrame(), pd.DataFrame()


def get_ct_ci_map(ct_ci_data):
    global_ct_maps = ct_ci_data.groupby('ct', as_index=False).agg(min_ci=('ci', 'min'),
                                                                  max_ci=('ci', 'max'),
                                                                  avg_ci=('ci', 'mean'),
                                                                  median_ci=('ci', 'median'))
    global_ct_maps['site_name'] = 'Global'

    site_ct_maps = ct_ci_data.groupby(['ct', 'site_name'], as_index=False).agg(min_ci=('ci', 'min'),
                                                                               max_ci=('ci', 'max'),
                                                                               avg_ci=('ci', 'mean'),
                                                                               median_ci=('ci', 'median'))
    output = pd.concat([global_ct_maps, site_ct_maps], axis=0)
    output['snapshot_date'] = pd.to_datetime('today').normalize()
    return output.sort_values(by=['site_name', 'ct'])
