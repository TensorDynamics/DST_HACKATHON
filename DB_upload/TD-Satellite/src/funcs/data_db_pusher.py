from tqdm import tqdm

from src.funcs.db_io import add_log_info_to_data, table_exists


def append_data_to_table(db_connection, data, schema_name, table_name, chunker_col=None):
    """
    Appends data to sql table
    :param db_connection: sql alchemy engine
    :param data: dataframe to append
    :param schema_name: schema name
    :param table_name: table name
    :param chunker_col: To create chunks while uploading, Optional, defaults pushes complete data frame at once.
        Helpful when dataframe is large
    """
    temp_data = data.copy()
    if 'tz' in temp_data.columns:
        temp_data['tz'] = temp_data['tz'].apply(str)
    temp_data = add_log_info_to_data(data_frame=temp_data)
    if chunker_col:
        for value in tqdm(temp_data[chunker_col].unique()):
            append_this = temp_data[temp_data[chunker_col] == value].copy()
            append_this.to_sql(name=table_name,
                               con=db_connection,
                               schema=schema_name,
                               if_exists='append',
                               index=False)
    else:
        temp_data.to_sql(name=table_name,
                         con=db_connection,
                         schema=schema_name,
                         if_exists='append',
                         index=False)


def upsert_data(nc_files_data, db_connection, sql_schema, sql_table_name, file_name_col='id'):
    """
    Deletes existing data and appends at [experiment-site-variable-timestamp] level
    :param nc_files_data: dataframe to push to db
    :param db_connection: database sql alchemy connection
    :param sql_schema: schema name
    :param sql_table_name: table name
    """
    for file_name in nc_files_data[file_name_col].unique():
        file_data = nc_files_data[nc_files_data[file_name_col] == file_name].copy()
        delete_query = f" delete from {sql_schema}.{sql_table_name}" \
                       f" where {file_name_col} = '{file_name}' "
        db_connection.execute(delete_query)
        append_data_to_table(db_connection=db_connection,
                             data=file_data,
                             schema_name=sql_schema,
                             table_name=sql_table_name)
