"""
MAIN PROCESS to read and push WRF data to DB
"""
import os
import datetime

from loguru import logger
from tqdm import tqdm

from configs import path_config, db_config, run_config
from funcs.db_io import (create_db_connection,
                         get_site_init_dates_map,
                         get_table_col_names,
                         read_db_data_to_df, infer_files_to_read)
from funcs.data_io import get_folders_at_path, process_site_table
from funcs.wrappers import read_ncfolder_and_push_to_db, read_n_push_parallel


def main():
    """
    main run for the process of reading NC files and uploading them to DB
    """
    # INIT INFO
    run_time = datetime.datetime.now()
    run_time_str = run_time.strftime("%Y-%m-%d %H_%M_%S")

    # LOGGER
    sink_file = os.path.join('LOGS', 'data_read', f'{run_time_str}.log')
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
    logger.info("> DB connection successful.")

    # 2. GATHER SITE INFO
    logger.info("============== SECTION - 2 : GETTING SITE INFO ==============")
    locations_df = read_db_data_to_df(con=db_connection,
                                      table_str=db_config.site_table,
                                      schema=db_config.configs_schema)

    requested_locs = process_site_table(site_df=locations_df, site_type=None)

    logger.info(f"> Found {len(requested_locs)} sites from database "
                f"table {db_config.site_table}.{db_config.configs_schema}.")
    logger.info("> Successfully extracted site info from database.")

    existing_site_maps = get_site_init_dates_map(con=db_connection,
                                                 table_name=db_config.wrf_stg_table,
                                                 schema=db_config.wrf_schema)

    report = existing_site_maps.groupby('site_name').agg(UniqueFiles=('init_date',
                                                                      'nunique')).sort_index().to_dict()['UniqueFiles']
    locations_df['Number of WRF Initialisations'] = locations_df['site_name'].apply(lambda x: report.get(x))

    logger.info("----------------------------------------")
    logger.info("Existing number of files per site in DB:")
    logger.info(locations_df.to_markdown())
    logger.info("----------------------------------------")

    db_site_file_map = {site: existing_site_maps[existing_site_maps['site_name'] == site]['init_date'].to_list()
                        for site in existing_site_maps['site_name'].unique() if site in requested_locs}

    wrf_date_folders = get_folders_at_path(path=path_config.data_path, include_empty=False)

    read_folders = infer_files_to_read(input_locations=requested_locs,
                                       local_files=wrf_date_folders,
                                       db_site_file_map=db_site_file_map)
    if read_folders:
        existing_schema = get_table_col_names(con=db_connection,
                                              table_str=db_config.wrf_stg_table,
                                              schema=db_config.wrf_schema)

        logger.info("============== SECTION - 3 : READ AND UPDATE DB TABLE ==============")
        if run_config.DATA_READ_PARALLEL:
            read_n_push_parallel(folders=read_folders,
                                 data_path=path_config.data_path,
                                 requested_locs=requested_locs,
                                 table_col_names=existing_schema,
                                 db_site_file_map=db_site_file_map,
                                 table_name=db_config.wrf_stg_table,
                                 table_schema=db_config.wrf_schema,
                                 logger=logger)
        else:
            for folder in tqdm(read_folders):
                read_ncfolder_and_push_to_db(data_path=path_config.data_path,
                                             requested_locs=requested_locs,
                                             table_col_names=existing_schema,
                                             db_site_file_map=db_site_file_map,
                                             table_name=db_config.wrf_stg_table,
                                             table_schema=db_config.wrf_schema,
                                             logger=logger,
                                             folder=folder)
    else:
        logger.info("No initialisations to read, data up to date in DB for all sites.")

    end = datetime.datetime.now()
    logger.info(f"Main script run ended at : {end} | Runtime: {end-run_time}")


if __name__ == '__main__':
    main()
