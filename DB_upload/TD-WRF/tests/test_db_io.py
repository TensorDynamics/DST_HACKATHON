"""
DB OIO TESTS
"""
import datetime
import unittest

import pytest
import pandas as pd

from funcs import db_io
from configs import db_config


def test_create_db_connection():
    """ TEST funcs.db_io.create_db_connection """
    eng = db_io.create_db_connection(dbname=db_config.dbname,
                                     host=db_config.host,
                                     port=db_config.port,
                                     user=db_config.user,
                                     password=db_config.password)
    connection = eng.connect()
    sample_query = "SELECT table_name FROM information_schema.tables " \
                   "WHERE table_schema='td_long_term' "
    results = connection.execute(sample_query).fetchall()
    assert isinstance(results, list)
    assert len(results) > 0


@pytest.fixture
def open_db_connection():
    """ FIXTURE TO BE USED FOR DB CONNECTION IN TESTS"""
    return db_io.create_db_connection(dbname=db_config.dbname,
                                      host=db_config.host,
                                      port=db_config.port,
                                      user=db_config.user,
                                      password=db_config.password)


def test_table_exists_yes(open_db_connection):
    result = db_io.table_exists(db_con=open_db_connection,
                                table_str=db_config.testing_table)
    assert result


def test_table_exists_no(open_db_connection):
    result = db_io.table_exists(db_con=open_db_connection,
                                table_str="Thistabledoesntexist")
    assert not result


def test_get_table_col_names(open_db_connection):
    result = db_io.get_table_col_names(con=open_db_connection,
                                       schema=db_config.testing_schema,
                                       table_str=db_config.testing_table)
    names = ['time',
             'punjab',
             'haryana',
             'rajasthan',
             'delhi',
             'up',
             'uttarakhand',
             'hp',
             'jk',
             'chd',
             'total_nr',
             'row_id',
             'log_ts',
             'site']

    assert result == names


def test_add_log_info_to_data(open_db_connection):
    data_frame = pd.DataFrame({'abc': [1, 7],
                               'def': [3, 9],
                               'ghi': [4, 0]}, index=[0, 1])
    result = db_io.add_log_info_to_data(data_frame=data_frame)

    assert 'row_id' in result.columns
    assert 'log_ts' in result.columns
    assert list(result['row_id'].values) == [1, 2]
    assert pd.api.types.is_datetime64_any_dtype(result['log_ts'])


def test_read_table_to_df_case1(open_db_connection):
    """ TEST FOR funcs.db_io.read_table_to_df"""
    result = db_io.read_table_to_df(con=open_db_connection,
                                    table_str=db_config.testing_table,
                                    schema=db_config.testing_schema)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] > 0


def test_read_table_to_df_raiseruntime(open_db_connection):
    """ TEST FOR funcs.db_io.read_table_to_df"""
    with pytest.raises(RuntimeError):
        db_io.read_table_to_df(con=open_db_connection,
                               table_str="Thistabledoesntexist",
                               schema=db_config.testing_schema)


def test_append_data_to_table(open_db_connection):
    """ TEST FOR funcs.db_io.append_data_to_table"""
    test_row = pd.DataFrame({'time': "99:99:99",
                             'punjab': 100,
                             'haryana': 200,
                             'rajasthan': 300,
                             'delhi': 400,
                             'up': 500,
                             'uttarakhand': 600,
                             'hp': 700,
                             'jk': 800,
                             'chd': 900,
                             'total_nr': 1000,
                             'row_id': 9999,
                             'log_ts': datetime.datetime.now(),
                             'site': 'north'},
                            index=[0])
    initial_table = db_io.read_table_to_df(con=open_db_connection,
                                           table_str=db_config.testing_table,
                                           schema=db_config.testing_schema)

    db_io.append_data_to_table(data=test_row,
                               db_url=db_io.tensor_aws_db1_url(),
                               table_name=db_config.testing_table,
                               schema=db_config.testing_schema)

    result_append_table = db_io.read_table_to_df(con=open_db_connection,
                                                  table_str=db_config.testing_table,
                                                  schema=db_config.testing_schema)

    assert (result_append_table.shape[0] - initial_table.shape[0]) == 1


def test_build_where_statement_from_dict(open_db_connection):
    """ TEST FOR funcs.db_io.build_where_statement_from_dict"""
    where_dict = {'time': '23:30:00',
                  'site': 'igi'}
    result = db_io.build_where_statement_from_dict(where_statement_dict=where_dict,
                                                   db_con=open_db_connection,
                                                   table_str=db_config.testing_table,
                                                   schema=db_config.testing_schema)
    expected_query = "time='23:30:00' and site='igi'"
    assert result == expected_query


def test_build_where_statement_from_dict_specify_dtype(open_db_connection):
    """ TEST FOR funcs.db_io.build_where_statement_from_dict"""
    where_dict = {'time': '23:30:00',
                  'site::text': 'igi'}
    result = db_io.build_where_statement_from_dict(where_statement_dict=where_dict,
                                                   db_con=open_db_connection,
                                                   table_str=db_config.testing_table,
                                                   schema=db_config.testing_schema)
    expected_query = "time='23:30:00' and site::text='igi'"
    assert result == expected_query


def test_build_where_statement_from_dict_keyerror(open_db_connection):
    """ TEST FOR funcs.db_io.build_where_statement_from_dict keyerror"""
    where_dict = {'time': '23:30:00',
                  'this_col_doesnt_exist': 'qwerty'}
    with pytest.raises(KeyError):
        db_io.build_where_statement_from_dict(where_statement_dict=where_dict,
                                              db_con=open_db_connection,
                                              table_str=db_config.testing_table,
                                              schema=db_config.testing_schema)


def test_delete_records_from_table(open_db_connection):
    """ TEST FOR funcs.db_io.delete_records_from_table"""
    initial_table = db_io.read_table_to_df(con=open_db_connection,
                                           table_str=db_config.testing_table,
                                           schema=db_config.testing_schema)

    db_io.delete_records_from_table(con=open_db_connection,
                                    schema=db_config.testing_schema,
                                    table_str=db_config.testing_table,
                                    where_statement_dict={'time': '99:99:99',
                                                          'site': 'north'})

    result_table = db_io.read_table_to_df(con=open_db_connection,
                                          table_str=db_config.testing_table,
                                          schema=db_config.testing_schema)

    assert initial_table.shape[1] == result_table.shape[1]
    assert initial_table.shape[0] >= result_table.shape[0]
    assert result_table[(result_table['time'] == '99:99:99') &
                        (result_table['site'] == 'north')].shape[0] == 0


def test_remove_special_chars_from_df_names():
    """ TEST FOR funcs.db_io.remove_special_chars_from_df_names"""
    data_frame = pd.DataFrame({'a': [1, 2],
                               'H&M': ['x', 'y'],
                               'Two Three': [3, 4]})
    result = db_io.remove_special_chars_from_df_names(data_frame=data_frame)
    assert result == ['a', 'h_m', 'two_three']


def test_get_uq_vals_from_table_where(open_db_connection):
    """ TEST FOR funcs.db_io.get_uq_vals_from_table"""
    where_dict = {'site': 'south'}
    result = db_io.get_uq_vals_from_table(sql_engine=open_db_connection,
                                          schema=db_config.testing_schema,
                                          table_name=db_config.testing_table,
                                          col_name='time',
                                          where_statement_dict=where_dict)
    assert isinstance(result, list)
    assert len(result) == 0


def test_get_uq_vals_from_table(open_db_connection):
    """ TEST FOR funcs.db_io.get_uq_vals_from_table"""

    result = db_io.get_uq_vals_from_table(sql_engine=open_db_connection,
                                          schema=db_config.testing_schema,
                                          table_name=db_config.testing_table,
                                          col_name='time')
    assert isinstance(result, list)
    assert len(result) > 0


def test_read_db_data_to_df(open_db_connection):
    """ TEST for funcs.db_io.read_db_data_to_df """
    result = db_io.read_db_data_to_df(con=open_db_connection,
                                       table_str=db_config.testing_view,
                                       schema=db_config.testing_schema,
                                       where_dict={'site': 'north'})
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] > 0
