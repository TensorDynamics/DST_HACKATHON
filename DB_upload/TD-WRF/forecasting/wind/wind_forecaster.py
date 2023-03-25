"""
Converts WRF 10 M wind to 85 M wind, Draws power curve for site and thus power from WRF wind.
Also supports scaling of WRF temperature to Site Temperature
"""
import os

from scipy.optimize import curve_fit
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearnex import patch_sklearn
patch_sklearn()
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error


class WRFWindMLConversion:

    def __init__(self,
                 ts_site_data,
                 model_feat_config,
                 site_name,
                 manual_break_date=None):
        """
        Converts WRF WIND TO POWER AND WIND SPEED
        """
        self.site_name = site_name
        self.source_time_series_data = ts_site_data
        self.model_feat_config = model_feat_config
        self.break_date = manual_break_date

    @staticmethod
    def clean_training_data(training_data, clean_outliers=True, target_name='target'):
        temp_df = training_data.copy()
        temp_df = temp_df.dropna(subset=[target_name])
        temp_df = temp_df.fillna(method='ffill')
        temp_df = temp_df[temp_df[target_name] > 0]
        if clean_outliers:
            lower, upper = temp_df[target_name].quantile(0.02), temp_df[target_name].quantile(0.98)
            temp_df = temp_df.query(f'{target_name} > @lower and {target_name} < @upper')
        return temp_df

    @staticmethod
    def scale_wrf_wind_speed(series):
        return series * (8.5) ** (1 / 7)

    def get_target_var_name(self):
        return self.model_feat_config.get('target').get('actual')

    def get_target_vector(self):
        target_col = self.get_target_var_name()
        actual_data = self.source_time_series_data['actual']
        target_series = actual_data[target_col]
        target_series.name = 'target'
        if self.break_date:
            target_series = target_series.loc[target_series.index < self.break_date]
        return target_series

    def get_feature_vectors(self):
        feature_names = self.model_feat_config.get('features')
        data_columns = feature_names.get('wrf')
        return self.source_time_series_data['wrf'][data_columns].copy()

    def get_training_data(self):
        target_vector = self.get_target_vector()
        feature_space = self.get_feature_vectors()
        training_data = pd.merge(left=target_vector,
                                 right=feature_space,
                                 left_index=True,
                                 right_index=True,
                                 how='inner')
        if 'wind_speed_10m_mps' in training_data.columns:
            training_data['wind_speed_10m_mps'] = self.scale_wrf_wind_speed(series=training_data['wind_speed_10m_mps'])
        return training_data

    @staticmethod
    def get_forecast_starting_date():
        return pd.Timestamp(pd.Timestamp('today').date())

    def get_horizon_index(self):
        feature_vector = self.get_feature_vectors()
        return feature_vector.loc[
            feature_vector.index >= self.get_forecast_starting_date()].index  # ALL AVAIL WRF FROM TODAY

    def get_forecast_exogs(self):
        feature_vector = self.get_feature_vectors().fillna(method='ffill')
        horizon_index = self.get_horizon_index()
        return feature_vector.loc[[i for i in horizon_index if i in feature_vector.index]]

    @staticmethod
    def power_curve_objective(x, l, k, a):
        return l / (1 + np.exp(-k * (x - a)))

    def get_power_curve_data(self):
        x_vector_name = self.model_feat_config.get('features').get('actual')
        y_vector_name = self.model_feat_config.get('target').get('actual')
        power_df = self.source_time_series_data.get('actual')[[x_vector_name, y_vector_name]]
        power_df = power_df.rename(columns={y_vector_name: 'y',
                                            x_vector_name: 'x'})
        power_df = self.clean_training_data(training_data=power_df,
                                            target_name='y')
        return power_df

    def get_power_curve_params(self):
        power_curve_data = self.get_power_curve_data()
        popt, _ = curve_fit(f=self.power_curve_objective,
                            xdata=power_curve_data['x'],
                            ydata=power_curve_data['y'],
                            bounds=((0, 0, 0),   # (lower L, lower k, lower a)
                                    (np.inf, 1, np.inf))  # (upper L, upper k, upper a)
                            )
        return popt

    def plot_power_curve(self, save=False, savedir=None):
        power_curve_data = self.get_power_curve_data()
        params = self.get_power_curve_params()
        max_x = power_curve_data['x'].max()
        sim_range = np.arange(0.1, int(max_x * 1.5), 0.1)
        sim_df = pd.DataFrame()
        for i in sim_range:
            simulated_power_at_i = self.power_curve_objective(x=i,
                                                              l=params[0],
                                                              k=params[1],
                                                              a=params[2])

            sim_df.loc[i, 'simulated_power'] = simulated_power_at_i
        sim_df.index.name = 'x'
        fig, ax = plt.subplots(figsize=(14, 4), ncols=2)
        sim_df.plot(ax=ax[1])
        power_curve_data.plot.scatter(y='y', x='x', ax=ax[0])
        for axis in ax:
            axis.grid(linestyle=':')
            axis.set_ylabel('wind power (kw)')
            axis.set_xlabel('wind speed (m/s)')
        plt.suptitle(f'Inferred Power Curve for {self.site_name}')
        plt.tight_layout()
        if save:
            path = savedir or os.getcwd()
            plt.savefig(os.path.join(path, f'PowerCurve_{self.site_name}.jpg'))

    def scale_temp(self):
        tr_data = self.get_training_data().copy().dropna()
        fcst_exogs = self.get_forecast_exogs()
        if tr_data.shape[0] == 0:
            if 'temp_c' in fcst_exogs.columns:
                return fcst_exogs['temp_c']
            else:
                return pd.Series(np.repeat(a=np.nan, repeats=len(fcst_exogs)), index=fcst_exogs.index)
        org_mape = mean_absolute_percentage_error(y_true=tr_data['target'], y_pred=tr_data['temp_c'])
        endog = tr_data['target']
        exog = tr_data['temp_c']
        mod = RandomForestRegressor().fit(X=exog.values.reshape(-1, 1), y=endog)
        in_sample_preds = mod.predict(exog.values.reshape(-1, 1))
        rev_mape = mean_absolute_percentage_error(y_true=tr_data['target'], y_pred=in_sample_preds)
        out_sample_preds = mod.predict(fcst_exogs.values.reshape(-1, 1))
        out_sample_preds = pd.Series(out_sample_preds, index=fcst_exogs.index)
        return out_sample_preds if rev_mape < org_mape else fcst_exogs['temp_c']

    def get_forecast(self):
        target_var = self.get_target_var_name()
        target_vector = self.get_target_vector()
        feature_vector = self.get_feature_vectors()
        horizon_index = self.get_horizon_index()
        if 'ws' in target_var or 'power' in target_var:
            tx_wrf_wind_speed = feature_vector['wind_speed_10m_mps'].copy()
            tx_wrf_wind_speed = self.scale_wrf_wind_speed(series=tx_wrf_wind_speed)
            tx_wrf_wind_speed = tx_wrf_wind_speed.loc[horizon_index]
        if 'ws' in target_var:
            output_series = tx_wrf_wind_speed.copy()
            output_series.name = target_var
            return output_series
        if 'power' in target_var:
            power_curve_params = self.get_power_curve_params()
            output_series = pd.Series(dtype=float, index=tx_wrf_wind_speed.index)
            for i in tx_wrf_wind_speed.index:
                wrf_tx_ws_at_i = tx_wrf_wind_speed.loc[i]
                output_series.loc[i] = self.power_curve_objective(x=wrf_tx_ws_at_i,
                                                                  l=power_curve_params[0],
                                                                  k=power_curve_params[1],
                                                                  a=power_curve_params[2])
            output_series.name = target_var
            return output_series
        if 'temp' in target_var:
            output_series = self.scale_temp()
            output_series.name = target_var
            return output_series
