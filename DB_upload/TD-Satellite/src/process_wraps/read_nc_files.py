"""
Read and send to DB wrapper
"""
from tqdm import tqdm
from src.funcs.data_db_pusher import upsert_data
from src.funcs.satellite_nc_loader import SatelliteNCExtractor


def read_files_and_upsert_to_db(file_paths,
                                lat_col,
                                lon_col,
                                exclude_cols_str_matching,
                                locations_dict,
                                db_connection,
                                sql_schema,
                                sql_table_name,
                                send_to_db=True):
    """
    Main wrapper to read nc satellite file and push to DB
    """
    for file in tqdm(file_paths):
        try:
            file_data = SatelliteNCExtractor(nc_file_path=file,
                                             lat_col=lat_col,
                                             lon_col=lon_col,
                                             exclude_cols_str_matching=exclude_cols_str_matching,
                                             locations_dict=locations_dict).get_data()
            if send_to_db:
                upsert_data(nc_files_data=file_data,
                            db_connection=db_connection,
                            sql_schema=sql_schema,
                            sql_table_name=sql_table_name,
                            file_name_col='id')
            print("Processed", file)
        except Exception as e:
            print(file, e)
