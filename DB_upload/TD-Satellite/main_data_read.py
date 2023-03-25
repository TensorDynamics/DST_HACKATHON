"""
Main process run
"""
import os
import datetime
import ntpath

from loguru import logger

from configs import (path_config, db_config, run_config)
from src.funcs.db_io import (create_db_connection, read_table_to_df, wrapper_get_files_to_read)
from src.funcs.decorators import process_timer
from src.funcs.data_io import (get_all_file_paths_in_dir, process_site_table)
from src.process_wraps.read_nc_files import read_files_and_upsert_to_db


@process_timer("main_data_read")
@logger.catch
def main():
    # INIT INFO
    run_time = datetime.datetime.now()
    run_time_str = run_time.strftime("%Y-%m-%d %H_%M_%S")

    # LOGGER
    sink_file = os.path.join('logs/data_read', f'{run_time_str}.log')
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
    logger.info("============== SECTION - 2 : GETTING SITE CONFIG INFO ==============")
    locations_df = read_table_to_df(con=db_connection,
                                    table_str=db_config.site_table,
                                    schema=db_config.configs_schema)
    locations_dict = process_site_table(site_df=locations_df, site_type=None, status=None)
    logger.info(f"> Found {len(locations_dict)} sites from database table "
                f"{db_config.configs_schema}.{db_config.site_table}")

    # 3.A GATHER FILES TO READ
    logger.info("============== SECTION - 3 : FILES IO ==============")
    local_nc_file_paths = get_all_file_paths_in_dir(folder_path=path_config.data_path,
                                                    find_match='S_NWC_CT_',
                                                    ignore_matches=['EXIM', 'stat'])
    local_nc_file_map = {ntpath.basename(filepath): filepath for filepath in local_nc_file_paths}
    local_nc_file_names = list(local_nc_file_map.keys())
    logger.info(f"> Found {len(local_nc_file_paths)} NETCDF files in the local directory {path_config.data_path}.")

    # 3.B GATHER DB STATUS OF FILES SITES AND FILE-SITES
    read_file_names = wrapper_get_files_to_read(locations=locations_dict,
                                                local_file_names=local_nc_file_names,
                                                db_connection=db_connection,
                                                schema=db_config.satellite_schema,
                                                table_name=db_config.satellite_stg_table,
                                                file_col='id',
                                                site_col='site_name',
                                                logger_obj=logger)

    read_file_paths = [local_nc_file_map.get(file) for file in read_file_names]
    logger.info(f"> Inferred {len(read_file_paths)} / {len(local_nc_file_paths)} NETCDF files to read and upsert.")

    # 4. READ FILES AND UPSERT
    if read_file_paths:
        read_files_and_upsert_to_db(file_paths=read_file_paths,
                                    locations_dict=locations_dict,
                                    lat_col='lat',
                                    lon_col='lon',
                                    exclude_cols_str_matching=['nx', 'ny'],
                                    db_connection=db_connection,
                                    sql_schema=db_config.satellite_schema,
                                    sql_table_name=db_config.satellite_stg_table,
                                    send_to_db=run_config.SEND_DATA_TO_DB)


if __name__ == '__main__':
    main()
