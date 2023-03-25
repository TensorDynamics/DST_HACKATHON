"""
processes i.e. wrappers from different funcs
"""
import os
from functools import partial
import numpy as np
from p_tqdm import p_map
from pathos.pools import ThreadPool
from d6tstack.utils import pd_to_psql

from funcs.nc_ops import read_nc_from_folder
from funcs.db_io import (add_log_info_to_data,
                         remove_special_chars_from_df_names,
                         tensor_aws_db1_url)


def read_ncfolder_and_push_to_db(data_path,
                                 requested_locs,
                                 table_col_names,
                                 db_site_file_map,
                                 table_schema,
                                 table_name,
                                 logger,
                                 folder):
    path = os.path.join(data_path, folder)
    file_data = read_nc_from_folder(folder_path=path, location_dict=requested_locs)
    file_data = add_log_info_to_data(data_frame=file_data)
    file_data.columns = remove_special_chars_from_df_names(data_frame=file_data)
    extra_cols = [col for col in table_col_names if col not in file_data.columns]
    for col in extra_cols:
        file_data[col]=len(file_data.index)*[np.nan] 
    file_data = file_data[table_col_names]
    print(file_data)
    append_for_site = [site
                       for site in file_data['site_name'].unique()
                       if site not in db_site_file_map.keys()]
    for site, dates in db_site_file_map.items():
        if folder not in dates:
            append_for_site.append(site)
    file_data = file_data[file_data['site_name'].isin(append_for_site)].reset_index(drop=True)
    if file_data.shape[0] > 0:
        append = pd_to_psql(df=file_data,
                            uri=tensor_aws_db1_url(),
                            table_name=table_name,
                            schema_name=table_schema,
                            if_exists='append')
        if append:
            logger.info(f"Appended data for {folder} for sites: {append_for_site} "
                        f"to DB Table {table_schema}.{table_name}")


def read_n_push_parallel(folders,
                         data_path,
                         requested_locs,
                         table_col_names,
                         db_site_file_map,
                         table_schema,
                         table_name,
                         logger):
    partial_func = partial(read_ncfolder_and_push_to_db,
                           data_path,
                           requested_locs,
                           table_col_names,
                           db_site_file_map,
                           table_schema,
                           table_name,
                           logger)
    p_map(partial_func, folders)   # NEW IMPLEMENTATION WITH PROGRESS BAR (Defaults to Process Pool)
    # pool = ThreadPool()
    # pool.map(partial_func, folders)
