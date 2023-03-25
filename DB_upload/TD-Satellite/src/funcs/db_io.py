"""
Funcs to support database io
"""
import datetime
import re
from tqdm import tqdm
from sqlalchemy import create_engine
from pandas import read_sql_query
from d6tstack.utils import pd_to_psql


def create_db_connection(dbname,
                         host,
                         port,
                         user,
                         password):
    """
    Establishes connection to database
    :param dbname: database name
    :param host: endpoint
    :param port: port
    :param user: username
    :param password: password
    """
    connection_string = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    return create_engine(connection_string)


def tensor_aws_db1_url():
    """
    Used for writing df to psql via d6tstack utils
    :return: str, url
    """
    return 'postgresql+psycopg2://admin123:tensor123@tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com:5432/postgres'


def table_exists(db_con, table_str):
    """
    Checks whether a table exists
    :param db_con: DB Connection
    :param table_str: table name to check
    :return: bool
    """
    try:
        cur = db_con.cursor()
    except AttributeError:
        cur = db_con.raw_connection().cursor()
    cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def get_table_col_names(con, table_str, schema):
    """
    Return table column names from db table
    :param con: DB connection
    :param table_str: table name
    :param schema: schema in db
    :return: list, col names
    """
    try:
        cur = con.cursor()
    except AttributeError:
        cur = con.raw_connection().cursor()
    cur.execute(f"select * from  {schema}.{table_str} LIMIT 0")
    col_names = [desc[0] for desc in cur.description]
    cur.close()
    return col_names


def read_table_to_df(con, table_str, schema, **kwargs):
    """
    Reads a table from the Database to a dataframe

    :param con: THe connection object, as returned from create_connection
    :type con: connection obj

    :param table_str: The table name
    :type table_str: str

    :param schema: schema name
    :type schema: str

    :return: Data from the table if exists
    :rtype: DataFrame
    """

    if not table_exists(con, table_str):
        raise RuntimeError(f"Unable to find {schema}.{table_str} in database")
    query_string = f'select * from {schema}.{table_str}'
    return read_sql_query(query_string, con=con, **kwargs)


def add_log_info_to_data(data_frame):
    """
    ADDS ROW ID and CURRENT TS to dataframe
    :param data_frame:  data frame
    :return: data frame with added 'log_row_id' and 'log_ts'
    """
    temp_data = data_frame.copy()
    temp_data['row_id'] = range(1, data_frame.shape[0] + 1)
    temp_data['log_ts'] = datetime.datetime.now()
    return temp_data


def append_data_to_table(data, db_url, table_name, schema, verbose=True, logger_obj=None, **kwargs):
    """
    Append data to table - d6tstack.utils method
    :param data: Data to append
    :param db_url: Database connection url
    :param table_name: table name to append data to
    :param schema: schema of database where table is located
    :param verbose: whether to print the process verbose
    :param logger_obj: logger object to gather verbose of process.
                       Optional, defaults to no logging.
    :type logger_obj: logging.logger class
    """
    init_msg = f"Appending data ({data.shape[0]} rows) -> DB table -> {schema}.{table_name}."

    if verbose:
        print(init_msg)

    data_frame = data.copy()
    data_frame = add_log_info_to_data(data_frame=data_frame)
    append = pd_to_psql(df=data_frame,
                        uri=db_url,
                        table_name=table_name,
                        schema_name=schema,
                        if_exists='append',
                        **kwargs)

    success_msg = f"Successfully appended data ({data.shape[0]} rows)  ->  DB table -> {schema}.{table_name}"

    if append and verbose:
        print(success_msg)

    if append and logger_obj:
        logger_obj.info(success_msg)


def get_uq_vals_from_table(sql_engine,
                           schema,
                           table_name,
                           col_name,
                           where_statement_dict=None,
                           logger_obj=None,
                           **kwargs):
    """
    list of unique values from a column in a database table
    :param sql_engine: SQL connection object
    :param schema: schema name
    :param table_name:  table name
    :param col_name: column name to get unique of
    :param where_statement_dict: dict. {col: value, col2: value2}. This is passed to db as where col=val and col2=val2
    :return: unique values of the column from table
    :rtype: list
    """
    if not table_exists(db_con=sql_engine, table_str=table_name):
        raise RuntimeError(f"Table - {table_name} does not exist in schema {schema}")

    if where_statement_dict:
        where_statement = build_where_statement_from_dict(where_statement_dict=where_statement_dict,
                                                          db_con=sql_engine,
                                                          schema=schema,
                                                          table_str=table_name)
        query = f"select distinct({col_name}) from {schema}.{table_name} WHERE {where_statement}"
    else:
        query = f"select distinct({col_name}) from {schema}.{table_name}"

    output = read_sql_query(sql=query, con=sql_engine, **kwargs).values.reshape(-1, )
    if logger_obj:
        logger_obj.info(f"Read {len(output)} unique values for column - {col_name} from {schema}.{table_name} ")
    return list(output)


def build_where_statement_from_dict(where_statement_dict, db_con, table_str, schema):
    """
    builds sql style where statement from a python dict
    :param where_statement_dict: dict. {col: value, col2: value2}. This is passed to db as where col=val and col2=val2
    :return: str, where clauses for a sql query
    """
    avail_col_names = get_table_col_names(con=db_con, table_str=table_str, schema=schema)
    where_statement = ""
    for key in where_statement_dict:
        if key.split('::')[0] not in avail_col_names:
            raise KeyError(f"Could not find {key} in {schema}.{table_str}")

        if not where_statement:
            where_statement += f"{key}='{where_statement_dict[key]}'"
        else:
            where_statement = where_statement + " and " + f"{key}='{where_statement_dict[key]}'"
    return where_statement


def delete_records_from_table(con, where_statement_dict, schema, table_str):
    """
    Deletes records from an existing table
    :param con: db connection
    :param schema: schema name
    :param table_str: table name
    :param where_statement_dict: dict. {col: value, col2: value2}. This is passed to db as where col=val and col2=val2
    """
    where_statement = build_where_statement_from_dict(where_statement_dict=where_statement_dict,
                                                      db_con=con,
                                                      table_str=table_str,
                                                      schema=schema)
    query = f"DELETE FROM {schema}.{table_str}  WHERE {where_statement}"
    con.execute(query)


def remove_special_chars_from_df_names(data_frame):
    """
    General clean column names of a data frame
    """
    df_names = list(data_frame.columns)
    revised_df_names = []
    for name in df_names:
        new_name = re.sub(r"[^A-Za-z0-9]+", ' ', name).lower().replace(' ', '_')
        revised_df_names.append(new_name)
    return revised_df_names


def read_db_data_to_df(con, table_str, schema, where_dict=None, **kwargs):
    """
    Reads a table from the Database to a dataframe

    :param con: THe connection object, as returned from create_connection
    :type con: connection obj

    :param table_str: The table name
    :type table_str: str

    :param schema: schema name
    :type schema: str

    :param where_dict: where additional filters for the sql query.
        Optional, defaults to None. i.e. will return all data in a db table. Caution data maybe large
    :type where_dict: dict

    :return: Data from the table if exists
    :rtype: DataFrame
    """

    if not table_exists(con, table_str):
        raise RuntimeError(f"Unable to find {schema}.{table_str} in database")
    if where_dict:
        where_statement = build_where_statement_from_dict(where_statement_dict=where_dict,
                                                          db_con=con,
                                                          table_str=table_str,
                                                          schema=schema)
        query_string = f"SELECT * FROM {schema}.{table_str} WHERE {where_statement}"
    else:
        query_string = f"SELECT * FROM {schema}.{table_str}"
    return read_sql_query(query_string, con=con, **kwargs)


def get_avail_site_dates(con, table_str, schema, **kwargs):
    """
    avail site and corresponding dates from a table.
    searches for date col with keywords 'time' and 'date'
    searches for site col with keywords 'site'
    """
    avail_cols = get_table_col_names(con=con, table_str=table_str, schema=schema)

    try:
        date_col = [col for col in avail_cols if 'time' in col.lower() or 'date' in col.lower()][0]
    except IndexError as ie:
        raise IndexError(f"Unable to find date/time column in {schema}.{table_str}. {ie}")

    try:
        site_col = [col for col in avail_cols if 'site' in col.lower()][0]
    except IndexError as ie:
        raise IndexError(f"Unable to find date/time column in {schema}.{table_str}. {ie}")

    query_string = f"SELECT {date_col}::date, {site_col} from {schema}.{table_str} " \
                   f"GROUP BY 1, 2"

    data = read_sql_query(query_string, con=con, **kwargs)
    return {site: list(data[data[site_col] == site][date_col].unique()) for site in data[site_col].unique()}


def get_site_files_map(con, table_name, schema, file_col='id', site_col='site_name', logger_obj=None):
    """
    Returns a map of existing files for each site in db table.
    :param con: DB connection obj
    :param table_name: table name to fecth map from
    :param schema:  schema name for table
    :return: dict, {site : [file1, file2 ...],
                    site2: [file1, file2, ...], ...}
    """
    # CHECK IF TABLE EXISTS
    if not table_exists(db_con=con, table_str=table_name):
        raise RuntimeError(f"Table {table_name} in schema {schema} does not exist")

    # CHECK IF COLUMN NAMES CONTAIN REQ COLS
    col_names_of_table = get_table_col_names(con=con, table_str=table_name, schema=schema)
    for name in [site_col, file_col]:
        if name not in col_names_of_table:
            raise KeyError(f"Could not find {name} in {schema}.{table_name} ")

    # READ DATA AND RETURN DICT
    db_site_file_map = read_sql_query(sql=f"select {site_col}, {file_col} "
                                          f"from {schema}.{table_name} "
                                          f"group by {site_col}, {file_col}",
                                      con=con)
    db_site_file_map = {site: db_site_file_map[db_site_file_map[site_col] == site][file_col].to_list()
                        for site in db_site_file_map[site_col].unique()}
    if logger_obj:
        logger_obj.info(f"Extracted site-file map from {schema}.{table_name} from columns {site_col} & {file_col}")
    return db_site_file_map


def infer_files_to_read(input_locations, locations_in_db, local_files, files_in_db, db_site_file_map) -> list:
    """ Logic to identify new files and new locations and which files to read"""
    new_locations = [loc for loc in input_locations if loc not in locations_in_db]
    new_files = [file for file in local_files if file not in files_in_db]
    if new_locations:
        return local_files
    files_missing_for_any_site = []
    for site_name, site_files in db_site_file_map.items():
        if site_name in input_locations:
            for file in tqdm(local_files):
                if file in files_in_db and file not in site_files:
                    files_missing_for_any_site.append(file)
    files_missing_for_any_site = list(set(files_missing_for_any_site))
    return list(set(files_missing_for_any_site + new_files))


def wrapper_get_files_to_read(locations,
                              local_file_names,
                              db_connection,
                              schema,
                              table_name,
                              file_col='id',
                              site_col='site_name',
                              logger_obj=None):
    if logger_obj:
        logger_obj.info("Inferring files to read per site - based on local system files and db files. "
                        "This may take a while...")

    existing_files_in_db = get_uq_vals_from_table(sql_engine=db_connection,
                                                  schema=schema,
                                                  table_name=table_name,
                                                  col_name=file_col,
                                                  logger_obj=logger_obj)

    locations_in_db = get_uq_vals_from_table(sql_engine=db_connection,
                                             schema=schema,
                                             table_name=table_name,
                                             col_name=site_col,
                                             logger_obj=logger_obj)

    site_file_map = get_site_files_map(con=db_connection,
                                       table_name=table_name,
                                       schema=schema,
                                       file_col=file_col,
                                       site_col=site_col,
                                       logger_obj=logger_obj)

    if logger_obj:
        logger_obj.info(f"> Found {len(existing_files_in_db)} NETCDF files "
                        f"in the DB Table {schema}.{table_name}.")

        for site, files in site_file_map.items():
            logger_obj.info(f"> Site - {site} total files in db table - {len(files)}")

    return infer_files_to_read(input_locations=locations,
                               locations_in_db=locations_in_db,
                               local_files=local_file_names,
                               files_in_db=existing_files_in_db,
                               db_site_file_map=site_file_map)
