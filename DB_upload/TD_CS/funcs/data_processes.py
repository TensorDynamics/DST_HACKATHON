"""
Different functions for supporting data processes
"""
from statistics import NormalDist


def calculate_confidence_intervals(series, target_col, conf_level=0.95):
    """
    Calculates confidence interval with y_fcst +- z(val) * std method
    
    :param series: series/dataframe containing the forecasts
    :type series: pd.DataFrame/pd.Series

    :param target_col: target column name from series containing the forecasts
    :type target_col: str

    :param conf_level: Confidence level intervals. Optional, defaults to 95%
    :type conf_level: float or list of floats

    :return: forecasts and confidence intervals
    :rtype: pd.DataFrame
    """
    
    ts_series = series.copy()
    result = pd.DataFrame()

    if not isinstance(conf_level, list):
        conf_level = [conf_level]

    for level in  conf_level:

        lower_res = []
        upper_res = []

        for i,v in enumerate(ts_series.index):
            fcst_at_i = ts_series.loc[v, target_col]
            std_till_i = ts_series.loc[ts_series.index <= v, target_col].std()
            std_till_i = 0 if i == 0 else std_till_i
            z_val = NormalDist().inv_cdf((1 + level) / 2.)
            scale_value = z_val * std_till_i

            lower, upper = fcst_at_i - scale_value, fcst_at_i + scale_value
            lower_res.append(lower)
            upper_res.append(upper)

        level_result = pd.DataFrame({f'lower_ci_{int(level*100)}%': lower_res,
                                     f'upper_ci_{int(level*100)}%': upper_res}, index=ts_series.index)

        result = pd.concat([result, level_result], axis=1)

    # ADD ORG SERIES
    mean_forecast = ts_series[[target_col]].copy()
    result = pd.merge(left=mean_forecast,
                      right=result, 
                      left_index=True,
                      right_index=True,
                      how='left').clip(lower=0)
 
    return result