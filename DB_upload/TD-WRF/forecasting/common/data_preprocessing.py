import pandas as pd
import numpy as np


class TSPreProcessor:

    def __init__(self,
                 data_frame,
                 time_col,
                 temporal_gran='D'):
        self.data = data_frame
        self.time_col = time_col
        self.temp_gran = temporal_gran

    @staticmethod
    def clean_series(data_frame, col_name):
        series = data_frame[col_name].copy()
        return series.clip(lower=series.quantile(0.01), upper=series.quantile(0.99))

    def make_ts(self, target_col, agg):
        df = self.data.set_index(self.time_col)
        df.index = pd.DatetimeIndex(df.index)
        resampled_df = df.resample(self.temp_gran).agg({target_col: agg})
        resampled_df[target_col] = self.clean_series(data_frame=resampled_df, col_name=target_col)
        return resampled_df


def preprocess_dataframe(data_frame, time_col, col_aggs):
    source_signals = pd.DataFrame()
    source_df_init = TSPreProcessor(data_frame=data_frame, time_col=time_col, temporal_gran='15T')
    for col in col_aggs:
        try:
            variable_data = source_df_init.make_ts(target_col=col, agg=col_aggs.get(col))
            source_signals = pd.concat([source_signals, variable_data], axis=1)
        except KeyError:
            continue
    return source_signals.replace(0, np.nan)


def pre_process_site_data(raw_site_data, db_map, time_col_map, col_agg_map):
    ts_site_data = {}
    for source in db_map:
        raw_source_data_frame = raw_site_data.get(source).copy()
        source_time_col = time_col_map.get(source)
        source_cols_map = col_agg_map.get(source)
        source_signals = preprocess_dataframe(data_frame=raw_source_data_frame,
                                              time_col=source_time_col,
                                              col_aggs=source_cols_map)

        ts_site_data[source] = source_signals
    return ts_site_data
