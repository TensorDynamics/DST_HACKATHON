"""
Preprocess functions for actual site data and satellite data
"""
import pandas as pd


def utc_to_ist(data_frame, time_col='timestamp'):
    temp_data = data_frame.copy()
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


def make_timelike_col(data_frame, time_col):
    temp_data = data_frame.copy()
    temp_data[time_col] = pd.to_datetime(temp_data[time_col])
    return temp_data


def pre_process_actual_site_data(data_frame, time_col='timestamp', variable='ghi(w/m2)'):
    output = (data_frame
              .copy()
              .pipe(make_timelike_col, time_col)
              .pipe(remove_timezone, time_col)
              .pipe(extract_avail_timeseries_for_variable, time_col, variable)
              .sort_index())
    return output
