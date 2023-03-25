"""
Main process run
"""
import os
import datetime
import ntpath

from loguru import logger

from configs import (path_config, db_config, run_config)
from src.funcs.db_io import (create_db_connection, read_table_to_df, get_uq_vals_from_table)
from src.funcs.decorators import process_timer
from src.funcs.data_io import (get_all_file_paths_in_dir, process_site_table)
from src.process_wraps.read_exim_files import read_files_and_upsert_to_db_exim


@process_timer("main_data_read_exim")
@logger.catch
def main():
    # INIT INFO
    run_time = datetime.datetime.now()
    run_time_str = run_time.strftime("%Y-%m-%d %H_%M_%S")

    # LOGGER
    sink_file = os.path.join('logs/data_read_exim', f'{run_time_str}.log')
    logger.add(sink=sink_file,
               level='DEBUG',
               backtrace=True,
               diagnose=True)
    logger.info(f"Process started at : {run_time}")
    logger.info('-------------------------------------------------------')
    # 1. DB CONNECTION
    logger.info("============== SECTION - 1 : DB CONNECTION ==============")
    db_connection = create_db_connection(dbname=db_config.dbname,
                                         host=db_config.host,
                                         port=db_config.port,
                                         user=db_config.user,
                                         password=db_config.password)
    logger.info(" > DB Connection successful.")

    # 2. GATHER SITE INFO
    logger.info("============== SECTION - 2 : GETTING SITE INFO ==============")
    locations_df = read_table_to_df(con=db_connection,
                                    table_str=db_config.site_table,
                                    schema=db_config.configs_schema)
    locations_dict = process_site_table(site_df=locations_df, site_type=None)
    logger.info(f"> Found {len(locations_dict)} sites from database table {db_config.configs_schema}.")
    logger.info("> Successfully extracted site info from database.")

    # 3.A GATHER FILES TO READ
    logger.info("============== SECTION - 3 : LOCAL FILES IO ==============")
    local_nc_file_paths = get_all_file_paths_in_dir(folder_path=path_config.exim_data_path,
                                                    find_match='EXIM-CT',
                                                    ignore_matches=['stat'])
    logger.info(f"> Found {len(local_nc_file_paths)} EXIM NETCDF files in the local directory {path_config.data_path}.")

    existing_files_in_db = get_uq_vals_from_table(sql_engine=db_connection,
                                                  schema=db_config.satellite_schema,
                                                  table_name=db_config.satellite_ct_exim_table,
                                                  col_name='file_name')
    logger.info(f"> Found {len(existing_files_in_db)} EXIM NETCDF files "
                f"in the DB Table {db_config.satellite_schema}.{db_config.satellite_ct_exim_table}.")
    read_file_paths = [p for p in local_nc_file_paths if ntpath.basename(p) not in existing_files_in_db]
    logger.info(f"> Found {len(read_file_paths)} new EXIM NETCDF files to read and append")

    if read_file_paths:
        # 3.B READ FILES
        read_files_and_upsert_to_db_exim(file_paths=read_file_paths,
                                         locations_dict=locations_dict,
                                         lat_col='lat',
                                         lon_col='lon',
                                         exclude_cols_str_matching=['nx', 'ny'],
                                         db_connection=db_connection,
                                         sql_schema=db_config.satellite_schema,
                                         sql_table_name=db_config.satellite_ct_exim_table,
                                         send_to_db=run_config.SEND_DATA_TO_DB)


if __name__ == '__main__':
    main()
