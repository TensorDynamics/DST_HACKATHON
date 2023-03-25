"""
tests for ..funcs/nc_ops module
"""
import os
import unittest
import datetime
import numbers

import pytest
import pandas as pd

from configs import path_config
from funcs import nc_ops


class TestCalcHaversineDist(unittest.TestCase):
    """ TEST funcs.calc_haversine_dist function """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sample_loc1 = (28.7041, 77.1025)
        self.sample_loc2 = (26.9124, 75.7873)
        self.sample_loc_incorrect = {28.7041, 77.1025}
        self.sample_loc_incorrect2 = ('a', 75.7873)
        self.correct_unit1 = 'km'
        self.correct_unit2 = 'mi'
        self.incorrect_unit = 'yards'

    def test_location_type(self):
        """ TEST LOCATION TYPE INPUT SHOULD BE TUPLE OF NUMBERS : CHECK CORRECT RAISES"""
        # 1. Location 2 is not a tuple
        with pytest.raises(TypeError, match=r'Cannot calculate dist'):
            nc_ops.calc_haversine_dist(loc1=self.sample_loc1,
                                       loc2=self.sample_loc_incorrect,
                                       unit=self.correct_unit1)

        # 2. Location 1 is not a tuple and unit is correct to second
        with pytest.raises(TypeError, match=r'Cannot calculate dist'):
            nc_ops.calc_haversine_dist(loc1=self.sample_loc_incorrect,
                                       loc2=self.sample_loc2,
                                       unit=self.correct_unit2)

        # 3. Location contains a string
        with pytest.raises(TypeError, match=r'must be real number'):
            nc_ops.calc_haversine_dist(loc1=self.sample_loc1,
                                       loc2=self.sample_loc_incorrect2,
                                       unit=self.correct_unit2)

    def test_distance_units(self):
        """ TEST DISTANCE CALCULATION WITH NOT IMPLEMENTED UNITS FOR INIT SAMPLE LOCS"""
        with pytest.raises(NotImplementedError, match=r"Only 'km' or 'mi' are implemented"):
            nc_ops.calc_haversine_dist(loc1=self.sample_loc1,
                                       loc2=self.sample_loc2,
                                       unit=self.incorrect_unit)

    def test_dist_calculation_km(self):
        """ TEST DISTANCE CALCULATION IN KM"""
        result = nc_ops.calc_haversine_dist(loc1=self.sample_loc1,
                                            loc2=self.sample_loc2,
                                            unit=self.correct_unit1)
        assert round(result, 1) == 237.5

    def test_dist_calculation_miles(self):
        """ TEST DISTANCE CALCULATION IN MILES"""
        result = nc_ops.calc_haversine_dist(loc1=self.sample_loc1,
                                            loc2=self.sample_loc2,
                                            unit=self.correct_unit2)
        assert round(result, 1) == 147.6


def test_make_timelike_from_float():
    """ TEST funcs.nc_ops.make_timelike_from_float"""
    sample_floats = [20220115.84375, 20220115.833333332, 20220115.822916668]
    expected_times = ['2022-01-16 01:45:00',
                      '2022-01-16 01:30:00',
                      '2022-01-16 01:15:00']
    expected_times = [datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S') for d in expected_times]

    result = nc_ops.make_timelike_from_float(timefloats=sample_floats)
    assert result == expected_times


def test_open_xarray_df_from_nc():
    """ TEST funcs.nc_ops.open_xarray_df_from_nc"""
    file_path = os.path.join(path_config.test_samples, 't2.nc')
    result = nc_ops.open_xarray_df_from_nc(nc_file_path=file_path)
    assert isinstance(result, pd.DataFrame)
    assert result.shape[0] > 0


@pytest.fixture()
def sample_xarray_df():
    """ Fixture from fuyncs.nc_ops.open_xarray_df_from_nc -> file input from samples t2.nc"""
    file_path = os.path.join(path_config.test_samples, 't2.nc')
    return nc_ops.open_xarray_df_from_nc(nc_file_path=file_path)


def test_closest_nc_distances(sample_xarray_df):
    """ TEST funcs.nc_ops.closest_nc_distances"""
    sample_loc_dict = {'Lahori': (23.37, 76.25), 'Sadla': (22.7308, 71.3685)}
    lat_col = 'XLAT'
    lon_col = 'XLONG'
    result = nc_ops.get_closest_nc_distances(locations_dict=sample_loc_dict,
                                             xarray_df=sample_xarray_df,
                                             lat_col=lat_col,
                                             lon_col=lon_col)

    assert isinstance(result, dict)
    assert len(result) == len(sample_loc_dict)
    for key, value in sample_loc_dict.items():
        assert key in result.keys()
        assert list(result[key].keys()) == ['NC', 'SITE']
        assert result[key]['SITE'] == value
        nc_lat = result[key]['NC'][0]  # NC LAT VALUE
        nc_lon = result[key]['NC'][1]  # NC LONG VALUE
        assert isinstance(nc_lat, numbers.Number)
        assert isinstance(nc_lon, numbers.Number)
        assert nc_lat in sample_xarray_df[lat_col].values
        assert nc_lon in sample_xarray_df[lon_col].values


def test_filter_xarray_for_locs(sample_xarray_df):
    """ TEST funcs.nc_ops.filter_xarray_for_locs"""
    sample_loc_dict = {'Lahori': (23.37, 76.25), 'Sadla': (22.7308, 71.3685)}
    result, locs_meta = nc_ops.filter_xarray_df_for_locs(xarray_df=sample_xarray_df, location_dict=sample_loc_dict)
    added_cols = ['site_name', 'site_lat', 'site_lon', 'tz', 'distance_site_grid_point_km']

    assert isinstance(result, pd.DataFrame)
    assert isinstance(locs_meta, dict)
    assert all(x in result.columns for x in added_cols)
    assert all(x in result.columns for x in sample_xarray_df)
    assert all(x in result['site_name'].unique() for x in sample_loc_dict)
    assert pd.api.types.is_datetime64_any_dtype(result['Times'])


def test_read_data_from_nc_with_locs():
    """ TEST funcs.nc_ops.read_data_from_nc_with_locs"""
    sample_loc_dict = {'Lahori': (23.37, 76.25), 'Sadla': (22.7308, 71.3685)}
    file_path = os.path.join(path_config.test_samples, 't2.nc')
    result, locs_meta = nc_ops.read_data_from_nc(nc_file_path=file_path, location_dict=sample_loc_dict)

    assert isinstance(result, pd.DataFrame)
    assert isinstance(locs_meta, dict)
    assert all(i not in result.columns for i in ['x', 'y'])
    assert pd.api.types.is_datetime64_any_dtype(result['Times'])


def test_read_data_from_nc_without_locs():
    """ TEST funcs.nc_ops.read_data_from_nc_without_locs"""
    file_path = os.path.join(path_config.test_samples, 't2.nc')
    result = nc_ops.read_data_from_nc(nc_file_path=file_path, location_dict=None)

    assert isinstance(result, pd.DataFrame)
    assert all(i not in result.columns for i in ['x', 'y'])
    assert pd.api.types.is_datetime64_any_dtype(result['Times'])
