"""
Main CT-CI training point
"""
import os
import datetime

from loguru import logger
from tqdm import tqdm
from pandas import concat, DataFrame

from configs import db_config
from src.funcs import db_io
from src.funcs.site_data_io import SiteConfigExtractor
from src.funcs.ct_ci_train import get_site_ct_vs_ci_data, get_ct_ci_map
from src.funcs.decorators import process_timer


@process_timer('ct_ci_train')
@logger.catch
def main():
    # INIT INFO
    run_time = datetime.datetime.now()
    run_time_str = run_time.strftime("%Y-%m-%d %H_%M_%S")

    # LOGGER
    sink_file = os.path.join('logs/ct_ci_train', f'{run_time_str}.log')
    logger.add(sink=sink_file,
               level='DEBUG',
               backtrace=True,
               diagnose=True)
    logger.info(f"Process started at : {run_time}")
    logger.info('-------------------------------------------------------')
    db_connection = db_io.create_db_connection(dbname=db_config.dbname,
                                               host=db_config.host,
                                               port=db_config.port,
                                               user=db_config.user,
                                               password=db_config.password)

    site_config = SiteConfigExtractor(db_connection=db_connection,
                                      table_name=db_config.site_table,
                                      schema_name=db_config.configs_schema,
                                      site_type=None)
    site_names = site_config.site_names()

    ct_ci_data = DataFrame()
    site_ci_cs_data = {}
    for sample_site in tqdm(site_names):
        print("---------------------------------------------------------")
        site_data, site_ci_cs = get_site_ct_vs_ci_data(site_name=sample_site,
                                                       db_connection=db_connection,
                                                       satellite_table_name=db_config.satellite_ip_view,
                                                       satellite_schema=db_config.satellite_schema,
                                                       site_actual_table=db_config.site_actual_table,
                                                       site_actual_schema=db_config.site_actual_schema,
                                                       clear_sky_window_len_days=10,
                                                       satellite_ct_col='ct',
                                                       satellite_time_col='timestamp',
                                                       site_actual_radiation_col='ghi(w/m2)',
                                                       site_actual_time_col='timestamp')
        if site_data is not None:
            print(f"{sample_site} rows = {site_data.shape[0]}")
        ct_ci_data = concat([ct_ci_data, site_data], axis=0)
        site_ci_cs_data[sample_site] = site_ci_cs

    final_data_to_db = get_ct_ci_map(ct_ci_data=ct_ci_data)

    db_io.append_data_to_table(data=final_data_to_db,
                               db_url=db_io.tensor_aws_db1_url(),
                               table_name=db_config.ct_ci_map_table,
                               schema=db_config.satellite_schema,
                               logger_obj=logger)


if __name__ == '__main__':
    main()