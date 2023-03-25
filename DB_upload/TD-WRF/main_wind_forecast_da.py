"""
MAIN PROCESS to forecast wind speed, wind power and temp
"""
import os
import datetime
from uuid import uuid4

import pandas as pd
from loguru import logger
from tqdm import tqdm
from d6tstack.utils import pd_to_psql

from funcs import db_io
from forecasting.common.data_extractors import (gather_site_data_from_db,
                                                SiteConfigExtractor)
from forecasting.common.data_preprocessing import pre_process_site_data
from forecasting.wind.wind_forecaster import WRFWindMLConversion
from configs import (path_config,
                     db_config,
                     wind_model_configs)


def main():
    """
    main run for the process of reading NC files and uploading them to DB
    """
    run_time = datetime.datetime.now()
    run_time_str = run_time.strftime("%Y-%m-%d %H_%M_%S")
    run_id = uuid4()

    # LOGGER
    sink_file = os.path.join('LOGS', 'wind_forecast', f'{run_time_str}.log')
    logger.add(sink=sink_file,
               level='DEBUG',
               backtrace=True,
               diagnose=True)
    logger.info(f"Process started at : {run_time} | run_id: {run_id}")
    logger.info('-------------------------------------------------------')

    # 1. DB CONNECTION
    logger.info("============== SECTION - 1 : DB CONNECTION ==============")
    db_connection = db_io.create_db_connection(dbname=db_config.dbname,
                                               host=db_config.host,
                                               port=db_config.port,
                                               user=db_config.user,
                                               password=db_config.password)
    logger.info("> DB connection successful.")

    # 2. GATHER SITE INFO
    logger.info("============== SECTION - 2 : GETTING SITE INFO ==============")
    site_config = SiteConfigExtractor(db_connection=db_connection,
                                      table_name=db_config.site_table,
                                      schema_name=db_config.configs_schema,
                                      site_type=None)
    all_sites = list(site_config.extract_locations().keys())
    site_capacities = site_config.extract_site_capacities()
    site_types = site_config.extract_site_types()
    active_sites = site_config.extract_active_sites()
    active_wind_sites = [site for site in active_sites if site_types.get(site) == 'Wind']
    logger.info("----------------------------------------")
    logger.info(f"Active Wind Sites {len(active_wind_sites)}: {active_wind_sites}")
    logger.info("----------------------------------------")

    # GET FORECAST TABLE STATUS
    table_exists = db_io.table_exists(db_con=db_connection,
                                      table_str=db_config.op_day_head_wind_forecast_table)
    if table_exists:
        existing_table_cols = db_io.get_table_col_names(con=db_connection,
                                                        table_str=db_config.op_day_head_wind_forecast_table,
                                                        schema=db_config.wrf_schema)
        site_forecast_status_map = db_io.get_site_forecast_status(db_connection=db_connection,
                                                                  table=db_config.op_day_head_wind_forecast_table,
                                                                  schema=db_config.wrf_schema,
                                                                  site_col='site_name',
                                                                  date_col='snapshot_date')

    # 3 Forecast and Append to Table
    logger.info("============== SECTION - 3 : CALCULATE FORECAST AND APPEND ==============")
    for sample_site in tqdm(active_wind_sites):
        default_model_configs = [wind_model_configs.model_feat_config_wind,
                                 wind_model_configs.model_feat_config_wind_power,
                                 wind_model_configs.model_feat_config_temp]
        raw_site_data = gather_site_data_from_db(db_connection=db_connection,
                                                 db_map=wind_model_configs.db_map,
                                                 site_name=sample_site,
                                                 eng='pandas',
                                                 multithread=True)

        ts_site_data = pre_process_site_data(raw_site_data=raw_site_data,
                                             db_map=wind_model_configs.db_map,
                                             time_col_map=wind_model_configs.time_col_map,
                                             col_agg_map=wind_model_configs.col_agg_map)

        site_forecasts = pd.DataFrame()
        for config in default_model_configs:
            var_method_init = WRFWindMLConversion(ts_site_data=ts_site_data,
                                                  site_name=sample_site,
                                                  model_feat_config=config)
            target_name = var_method_init.get_target_var_name()
            if 'power' in target_name:
                var_method_init.plot_power_curve(save=True, savedir=path_config.resources_path)
            config_forecast = var_method_init.get_forecast()
            site_forecasts = pd.concat([site_forecasts, config_forecast], axis=1)
        snapshot_date = site_forecasts.index.min()
        site_forecasts['snapshot_date'] = snapshot_date
        site_forecasts['site_name'] = sample_site
        site_forecasts['run_id'] = run_id
        site_forecasts.index.name = 'timestamp'
        site_forecasts = site_forecasts.reset_index()
        site_forecasts = db_io.add_log_info_to_data(site_forecasts)
        if table_exists:
            site_forecasts = site_forecasts[existing_table_cols]
            snapshot_exists = snapshot_date in site_forecast_status_map.get(sample_site, [])
        else:
            snapshot_exists = False
        if snapshot_exists:
            logger.info(f"Snapshot for {sample_site} already exists in table.")
        else:
            status = pd_to_psql(df=site_forecasts,
                                uri=db_io.tensor_aws_db1_url(),
                                table_name=db_config.op_day_head_wind_forecast_table,
                                schema_name=db_config.wrf_schema,
                                if_exists='append')
            if status:
                success_msg = f"Successfully appended data ({site_forecasts.shape[0]} rows)  for {sample_site}" \
                              f"->  DB table -> " \
                              f"{db_config.wrf_schema}.{db_config.op_day_head_wind_forecast_table}"
                logger.info(success_msg)


if __name__ == '__main__':
    main()
