"""
tests for ..funcs/data_io
"""
import os
import unittest

import pandas as pd
import pytest
from funcs import data_io
from configs import path_config


def test_search_col_in_df():
    """ TEST FOR funcs.data_io.search_col_in_df"""
    test_frame = pd.DataFrame({'abc': [1, 2],
                               'def': [3, 4]}, index=[0, 1])
    result = data_io.search_col_in_df(data_frame=test_frame,
                                      search_str='AB')
    assert result == 'abc'


def test_search_col_in_df_indexerror():
    """ TEST FOR funcs.data_io.search_col_in_df INDEX ERROR"""
    test_frame = pd.DataFrame({'abc': [1, 2],
                               'def': [3, 4]}, index=[0, 1])
    with pytest.raises(IndexError):
        data_io.search_col_in_df(data_frame=test_frame,
                                 search_str='XYZ')


class TestGetLocationsByType(unittest.TestCase):
    """ TEST FOR funcs.data.io.get_locations by type """

    def __init__(self, *args, **kwargs):
        super(TestGetLocationsByType, self).__init__(*args, **kwargs)
        self.file_dir = path_config.resources_path
        self.file_name = 'site_locations.csv'

    def test_check_file_exist(self):
        assert os.path.isfile(os.path.join(self.file_dir, self.file_name))

    def test_file_type_csv(self):
        assert self.file_name.lower().endswith('csv')

    def test_return_asserts(self):
        raw_csv_read = pd.read_csv(os.path.join(self.file_dir, self.file_name))
        result = data_io.get_locations_by_type(filedir=self.file_dir,
                                               filename=self.file_name)

        assert isinstance(result, dict)
        assert len(result) == raw_csv_read.shape[0]
        for key in result:
            assert isinstance(key, str)
            value = result[key]
            assert isinstance(value, tuple)
            for val in value:
                assert isinstance(val, float)


def test_get_folders_at_path():
    """ TEST FOR funcs.data.io.get_folders_at_path """
    result = data_io.get_folders_at_path(path=os.getcwd())
    assert isinstance(result, list)
    assert len(result) > 0


class TestGetUnmatchingRows(unittest.TestCase):
    """ TEST FOR funcs.data.io.get_unmatching_rows """

    def __init__(self, *args, **kwargs):
        super(TestGetUnmatchingRows, self).__init__(*args, **kwargs)

        self.df1 = pd.DataFrame({'abc': [1, 2],
                                 'def': [3, 4],
                                 'ghi': [4, 5]}, index=[0, 1])

        self.df2 = pd.DataFrame({'abc': [6, 7],
                                 'def': [8, 9],
                                 'ghi': [11, 0]}, index=[0, 1])

        self.df3 = pd.DataFrame({'abc': [1, 7],
                                 'def': [3, 9],
                                 'ghi': [4, 0]}, index=[0, 1])

    def test_get_unmatching_rows_case1(self):
        result = data_io.get_unmatching_rows(data_frame1=self.df1,
                                             data_frame2=self.df2,
                                             return_from='all')
        op = pd.DataFrame({'abc': [1, 2, 6, 7],
                           'def': [3, 4, 8, 9],
                           'ghi': [4, 5, 11, 0]})

        pd.testing.assert_frame_equal(left=result, right=op)

    def test_get_unmatching_rows_case2(self):
        result = data_io.get_unmatching_rows(data_frame1=self.df1,
                                             data_frame2=self.df2,
                                             return_from='outer_df')

        pd.testing.assert_frame_equal(left=result, right=self.df1)

    def test_get_unmatching_rows_case3(self):
        result = data_io.get_unmatching_rows(data_frame1=self.df1,
                                             data_frame2=self.df2,
                                             return_from='inner_df')

        pd.testing.assert_frame_equal(left=result, right=self.df2)

    def test_get_unmatching_rows_case4(self):
        result = data_io.get_unmatching_rows(data_frame1=self.df1,
                                             data_frame2=self.df3,
                                             return_from='all')
        op = pd.DataFrame({'abc': [2, 7],
                           'def': [4, 9],
                           'ghi': [5, 0]})
        pd.testing.assert_frame_equal(left=result, right=op)


def test_get_folders_at_path_with_empty():
    test_dir = os.path.join(path_config.test_samples, 'test_dir')
    result = data_io.get_folders_at_path(path=test_dir, include_empty=True)
    assert result == ['a', 'b']


def test_get_folders_at_path_without_empty():
    test_dir = os.path.join(path_config.test_samples, 'test_dir')
    result = data_io.get_folders_at_path(path=test_dir, include_empty=False)
    assert result == ['a']

