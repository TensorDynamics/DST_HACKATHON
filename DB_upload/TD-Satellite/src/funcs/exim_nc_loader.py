"""
Class to extract data from a NETCDF file
"""
import re
from heapq import nsmallest
from ntpath import basename

import numpy as np
import xarray as xr
import pandas as pd
import haversine


class EXIMNCExtractor:

    def __init__(self,
                 nc_file_path,
                 locations_dict=None,
                 lat_col=None,
                 lon_col=None,
                 exclude_cols_str_matching=None,
                 extract_attrs=None):
        """
        Extracts dataframe from NETCDF File with given params

        :param nc_file_path: File path to read
        :type nc_file_path: str, path like

        :param locations_dict: locations dictionary to filter netcdf file variables. Optional, default to None.
            Will return all lat lon info if None.
        :type locations_dict: dict

        :param lat_col: Latitude index label in netcdf file. Optional, defaults to None - will try to infer
        :type lat_col: str

        :param lon_col: Longitude index label in netcdf file. Optional, defaults to None - will try to infer
        :type lon_col: str

        :param exclude_cols_str_matching: List of string matches to avoid in output. Can be part of the column names.
            Optional, defaults to ['bnds']. i.e. all column labels with 'bnds' will be omitted.
        :type exclude_cols_str_matching: [str], list of str

        :param extract_attrs: List of attribute names of the netcdf file to extract.
            Defaults to ['title', 'experiment', 'creation_date']
        :type lat_col: [str], list of str
        """
        self.file_path = nc_file_path
        self.locs_dict = locations_dict
        self.lat_col = lat_col
        self.lon_col = lon_col
        self.exclude_cols_str_matching = exclude_cols_str_matching or ['bnds']
        self.global_attrs = extract_attrs or ['keywords',
                                              'date_created',
                                              'time_coverage_start',
                                              'time_coverage_end',
                                              'product_name',
                                              'nominal_product_time',
                                              'satellite_identifier',
                                              'id',
                                              'summary',
                                              'title']

    @staticmethod
    def calc_haversine_dist(loc1, loc2, unit='KM') -> float:
        """
        Calculates haversine (great-circle) distance between two co-ordinates in km
        :param loc1: location 1 (lat1, lon1)
        :param loc2: location 2 (lat2, lon2)
        :param unit: UNITS of distance, optional, defaults to KM. Options = [KM, MI] i.e. [kilometers, miles]
        :return: distance in unit
        :rtype: float
        """
        if not isinstance(loc1, tuple) or not isinstance(loc2, tuple):
            raise TypeError("Cannot calculate distance. Please provide location as (lat, lon) ")

        if 'km' in unit.lower():
            unit = haversine.Unit.KILOMETERS
        elif 'mi' in unit.lower():
            unit = haversine.Unit.MILES
        else:
            raise NotImplementedError("Only 'km' or 'mi' are implemented units while calculating haversine distance.")
        return haversine.haversine(point1=loc1, point2=loc2, unit=unit)

    @staticmethod
    def haversine_np(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)

        All args must be of equal length.

        """
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

        c = 2 * np.arcsin(np.sqrt(a))
        return 6367 * c

    @staticmethod
    def drop_columns_matching(data_frame, col_matches) -> pd.DataFrame:
        """
        Drops columns from the dataframe that matches any given str in the iter of col_matches
        """
        temp_df = data_frame.copy()
        drop_cols = [col for col in data_frame.columns if any(x.lower() in col.lower() for x in col_matches)]
        return temp_df.drop(drop_cols, axis=1)

    @staticmethod
    def get_attrs_from_xarray(xarray_obj, attr_names) -> dict:
        """Extract attributes from NC file with the given attr_names"""
        return {name: xarray_obj.attrs.get(name) for name in attr_names}

    @staticmethod
    def add_dict_items_to_df(data_frame, attr_dict, prefix=None):
        temp_df = data_frame.copy()
        for counter, (attr, value) in enumerate(attr_dict.items()):
            name = f"{prefix}_{attr}" if prefix else attr
            temp_df.insert(loc=counter, column=name, value=value)
        return temp_df

    @staticmethod
    def get_timezone(xarray_obj):
        return str(pd.to_datetime(xarray_obj.attrs.get('date_created', None)).tz)

    @staticmethod
    def replace_special_chars(my_string, replace_with='_') -> str:
        """
        Replaces special characs from a string with regex.
        """
        return re.sub('[^A-Za-z0-9]+', replace_with, my_string).lower()

    @staticmethod
    def extract_all_vars(xr_obj):
        output_df = pd.DataFrame()
        var_names = list(xr_obj.variables.keys())
        for var in var_names:
            var_obj = xr_obj.variables.get(var)
            if var_obj is not None:
                melted_var = pd.DataFrame({var: var_obj.values.ravel()})
                output_df = pd.concat([output_df, melted_var], axis=1)
        return output_df

    @staticmethod
    def extract_cords(xr_obj, lat_col, lon_col):
        melted_lats = pd.DataFrame({lat_col: xr_obj.coords[lat_col].values.ravel()})
        melted_lons = pd.DataFrame({lon_col: xr_obj.coords[lon_col].values.ravel()})
        return pd.concat([melted_lats, melted_lons], axis=1)

    @staticmethod
    def find_k_closest_number(find_number, search_array, k):
        return nsmallest(k, search_array, key=lambda x: abs(x - find_number))

    @staticmethod
    def add_minutes_to_timestamp(ts, mins):
        return ts + pd.Timedelta(minutes=mins)

    def add_calculated_time_index(self, data_frame, time_col):
        temp_df = data_frame.copy()
        temp_df['time_split'] = temp_df[time_col].apply(lambda x: x.split('Z'))

        # EXTRACT HORIZON TIME
        try:
            temp_df['horizon_time'] = temp_df['time_split'].apply(lambda x: self.replace_special_chars(my_string=x[1],
                                                                                                         replace_with=''))
        except IndexError:
            temp_df['horizon_time'] = '0'

        try:
            temp_df['horizon_time'] = temp_df['horizon_time'].apply(lambda x: int(x))
        except ValueError:
            temp_df['horizon_time'] = 0

        # EXTRACT BASETIME
        temp_df['base_time'] = temp_df['time_split'].apply(lambda x: x[0])
        temp_df['base_time'] = pd.to_datetime(temp_df['base_time']).dt.round('15min')
        temp_df[f"calc_{time_col}"] = temp_df.apply(lambda row: self.add_minutes_to_timestamp(ts=row['base_time'],
                                                                                          mins=row['horizon_time']),
                                                axis=1)

        temp_df.drop(['horizon_time', 'base_time', 'time_split'], axis=1, inplace=True)
        return temp_df

    def open_xarray_obj(self):
        """Opens xarray dataframe from netcdf file"""
        return xr.open_dataset(self.file_path)

    def add_timezone(self, data_frame, xarray_obj):
        temp_df = data_frame.copy()
        temp_df['tz'] = self.get_timezone(xarray_obj=xarray_obj)
        return temp_df

    def clean_col_names(self, data_frame) -> pd.DataFrame:
        """Clean column names from special characters"""
        temp_df = data_frame.copy()
        temp_df.columns = [self.replace_special_chars(col) for col in temp_df.columns]
        return temp_df

    def find_closest_distance_haversine_np(self, locations_dict, lat_lon_df, lat_col='lat', lon_col='lon'):
        output_dict = {}
        for location in locations_dict:
            site_actual_location = locations_dict.get(location)
            site_lat = site_actual_location[0]
            site_lon = site_actual_location[1]

            temp_df = lat_lon_df.copy()
            temp_df['site_lat'] = site_lat
            temp_df['site_lon'] = site_lon
            temp_df['distance'] = self.haversine_np(lon1=temp_df[lon_col],
                                                    lat1=temp_df[lat_col],
                                                    lon2=temp_df['site_lon'],
                                                    lat2=temp_df['site_lat'])
            required_row = temp_df.loc[temp_df['distance'].idxmin()]
            required_lat = required_row.loc[lat_col]
            required_lon = required_row.loc[lon_col]
            output_row = {location: {'NC': (required_lat, required_lon), 'SITE': site_actual_location}}
            output_dict.update(output_row)
        return output_dict

    def get_closest_nc_distances(self, locations_dict, xarray_df, lat_col, lon_col) -> dict:
        """
        Prepares a dictionary of actual site lat lons and the closes lat lons in a dataframe
        :param locations_dict: Locations dict in format {site : (lat, lon), site2: (lat2, lon2). .. .}
        :param xarray_df: dataframe containing lat lon cols to search from
        :param lat_col: column label containing lats
        :param lon_col: column label containing lons
        :return: dictionary of actual site lat lons and the closes lat lons found
            {'site: {'SITE': (lat, lon), 'NC':(lat`, lon`)}, 'site2':{'SITE': (lat, lon), 'NC':(lat`, lon`)} ... }
        :rtype: dict
        """
        lat_lon_df = xarray_df[[lat_col, lon_col]].drop_duplicates().reset_index(drop=True).copy()
        return self.find_closest_distance_haversine_np(locations_dict=locations_dict,
                                                       lat_lon_df=lat_lon_df,
                                                       lat_col=lat_col,
                                                       lon_col=lon_col)

    def filter_xarray_df_for_locs(self, xarray_df, location_dict, lat_col, lon_col) -> pd.DataFrame:
        """
        Filter xarray dataframe for incoming locations
        :param xarray_df: xarray df
        :param location_dict: Locations dict in format {site : (lat, lon), site2: (lat2, lon2). .. .}
        :param lat_col: col label containing latitude
        :param lon_col: col label containing longitude
        :return: netcdf dataframe filtered for locations
        """
        # GET AVAIL LOCATIONS
        locations_dict_nc = self.get_closest_nc_distances(locations_dict=location_dict,
                                                          xarray_df=xarray_df,
                                                          lat_col=lat_col,
                                                          lon_col=lon_col)
        # ITER OVER SITES
        file_df = pd.DataFrame()
        for location_name, location_meta in locations_dict_nc.items():
            actual_site_loc = location_meta['SITE']
            nc_site_loc = location_meta['NC']

            loc_df = xarray_df[(xarray_df[lat_col] == nc_site_loc[0]) &
                               (xarray_df[lon_col] == nc_site_loc[1])].copy().reset_index(drop=True)
            loc_df['site_name'] = location_name
            loc_df['site_lat'] = actual_site_loc[0]
            loc_df['site_lon'] = actual_site_loc[0]
            loc_df['distance_site_grid_point_km'] = self.calc_haversine_dist(loc1=actual_site_loc,
                                                                             loc2=nc_site_loc,
                                                                             unit='KM')
            file_df = pd.concat([file_df, loc_df], axis=0)
        return file_df.reset_index(drop=True)

    def get_data(self) -> pd.DataFrame:
        """
        wrapper for:
        1. Conversion process from NETCDF TO XARRAY
        2. Extracting attributes & dataframe
        3. Filtering for locations
        4. Cleaning of column names and dropping extra columns
        """
        xarray_obj = self.open_xarray_obj()  # OPEN X ARRAY OBJECT
        xarray_attrs = self.get_attrs_from_xarray(xarray_obj=xarray_obj,
                                                  attr_names=self.global_attrs)  # EXTRACT ATTRIBUTES

        xarray_df = self.extract_all_vars(xr_obj=xarray_obj)
        if self.locs_dict:
            xarray_df = self.filter_xarray_df_for_locs(xarray_df=xarray_df,
                                                       location_dict=self.locs_dict,
                                                       lat_col=self.lat_col,
                                                       lon_col=self.lon_col)

        xarray_df = (xarray_df
                     .pipe(self.drop_columns_matching, self.exclude_cols_str_matching)  # EXCLUDE COLS (Optional)
                     .pipe(self.add_timezone, xarray_obj)  # ADD TIME ZONE IF PRESENT  (FROM 'date_created' attr)
                     .pipe(self.add_dict_items_to_df, xarray_attrs)  # ADD ATTRIBUTES FROM NETCDF ATTRIBUTES
                     .pipe(self.add_calculated_time_index, 'nominal_product_time')  # CONVERT COL TO DATETIME LIKE
                     .pipe(self.add_calculated_time_index, 'date_created')  # CONVERT COL TO DATETIME LIKE
                     .pipe(self.clean_col_names)  # REMOVE SPECIAL CHARS FROM COL NAMES
                     )
        xarray_df['file_name'] = basename(self.file_path)
        return xarray_df
