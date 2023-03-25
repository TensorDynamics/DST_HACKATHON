"""
MODULE TO SUPPORT NC IO OPERATIONS
"""
import os
import datetime

import haversine
import pandas as pd
import numpy as np
import xarray as xr


def calc_haversine_dist(loc1, loc2, unit='KM'):
    """
    Calculates haversine (great-circle) distance between two co-ordinates in km
    :param loc1: location 1 (lat1, lon1)
    :param loc2: location 2 (lat2, lon2)
    :param unit: UNITS of distance, optional, defaults to KM. Options = [KM, MI] i.e. [kilometers, miles]
    :return: distance in km
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


def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    All args must be of equal length.
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    return 6367 * c


def make_timelike_from_float(timefloats):
    """
    Converts float like time from wrf to datetime in IST
    :param timefloats: time values to convert
    :return: time like values from floats
    :rtype: list
    """
    timepoints = []
    for timeval in timefloats:
        year, timeval = int(timeval // 10000), timeval % 10000
        month, timeval = int(timeval // 100), timeval % 100
        day, timeval = int(timeval // 1), timeval % 1
        timeval = round(timeval * 3600 * 24)
        hour, timeval = int(timeval // 3600), timeval % 3600
        minute, timeval = int(timeval // 60), timeval % 60
        second = int(timeval)
        timepoints.append("{:02d}-{:02d}-{:04d} {:02d}:{:02d}:{:02d}".format(day, month, year, hour, minute, second))
    timepoints = [datetime.datetime.strptime(t, '%d-%m-%Y %H:%M:%S') for t in timepoints]
    return [t + datetime.timedelta(hours=5, minutes=30) for t in timepoints]


def open_xarray_df_from_nc(nc_file_path):
    """
    Opens xarray dataframe from netcdf file
    :param nc_file_path: path to netcdf file
    :return: dataframe, all xarray of nc
    """
    return xr.open_dataset(nc_file_path).to_dataframe().reset_index(drop=False)


def get_closest_nc_distances(locations_dict, xarray_df, lat_col='XLAT', lon_col='XLONG'):
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
    avail_lats = lat_lon_df[lat_col].values
    avail_lons = lat_lon_df[lon_col].values
    output_dict = {}

    for location in locations_dict:
        site_actual_location = locations_dict[location]
        distances = {}
        for t1, n1 in zip(avail_lats, avail_lons):
            dist = calc_haversine_dist(loc1=site_actual_location,
                                       loc2=(t1, n1))
            # dist = haversine_np(lat1=site_actual_location[0],
            #                     lon1=site_actual_location[1],
            #                     lat2=t1,
            #                     lon2=n1)
            distances[(t1, n1)] = dist
        min_dist_at = min(distances, key=distances.get)
        output_dict[location] = {'NC': min_dist_at, 'SITE': site_actual_location}
    return output_dict


def filter_xarray_df_for_locs(xarray_df, location_dict):
    """
    Filter xarray dataframe for incoming locations
    :param xarray_df: xarray df
    :param location_dict: Locations dict in format {site : (lat, lon), site2: (lat2, lon2). .. .}
    :return:
    """
    # GET AVAIL LOCS
    locations_dict_nc = get_closest_nc_distances(locations_dict=location_dict,
                                                 xarray_df=xarray_df)
    # ITER OVER SITES
    file_df = pd.DataFrame()
    for location_name, location_meta in locations_dict_nc.items():
        actual_site_loc = location_meta['SITE']
        nc_site_loc = location_meta['NC']

        loc_df = xarray_df[(xarray_df['XLAT'] == nc_site_loc[0]) &
                           (xarray_df['XLONG'] == nc_site_loc[1])].copy().reset_index(drop=True)
        loc_df['Times'] = make_timelike_from_float(timefloats=loc_df['Times'])
        loc_df['site_name'] = location_name
        loc_df['site_lat'] = actual_site_loc[0]
        loc_df['site_lon'] = actual_site_loc[1]
        loc_df['tz'] = 'IST'
        loc_df['distance_site_grid_point_km'] = calc_haversine_dist(loc1=actual_site_loc,
                                                                    loc2=nc_site_loc,
                                                                    unit='KM')
        file_df = pd.concat([file_df, loc_df], axis=0)

    return file_df, locations_dict_nc


def read_data_from_nc(nc_file_path, location_dict=None):
    """
    Wrapper to read nc file and filter out locations
    :param nc_file_path: path to nc file to read
    :param location_dict: locations containing dict {site : (lat, lon), site2: (lat2, lon2). .. .}
    :return:
    """
    xarray_df = open_xarray_df_from_nc(nc_file_path)

    if location_dict is None:
        file_df = xarray_df.copy()
        file_df['Times'] = make_timelike_from_float(timefloats=file_df['Times'])
        file_df.drop(['x', 'y'], axis=1, inplace=True)
        return file_df
    else:
        file_df, locations_dict_nc = filter_xarray_df_for_locs(xarray_df=xarray_df,
                                                               location_dict=location_dict)
        # OUTPUT FORMAT
        file_df.drop(['x', 'y'], axis=1, inplace=True)
        return file_df, locations_dict_nc


def read_nc_from_folder(folder_path, location_dict):
    """
    Wrapper to read nc files from an initialisation folder
    :param folder_path: folder path
    :type folder_path: path like
    :param location_dict: locations containing dict {site : (lat, lon), site2: (lat2, lon2). . . .}
    :return: dataframe,
    """
    files = [file for file in os.listdir(folder_path) if '.DS' not in file and 'd' not in file]
    init_date = os.path.basename(folder_path)

    data_frame = pd.DataFrame()

    for file in files:
        dat, _ = read_data_from_nc(nc_file_path=os.path.join(folder_path, file),
                                   location_dict=location_dict)
        dat['file'] = file.split('.nc')[0]
        data_frame = pd.concat([data_frame, dat], axis=0)

    # DROP DUP SITE-TIMESTAMP
    data_frame = data_frame.sort_values(by='file').reset_index(drop=True)
    data_frame = data_frame.drop_duplicates(subset=['Times', 'site_name'], keep='last')
    data_frame = data_frame.reset_index(drop=True).drop('file', axis=1)
    data_frame['init_date'] = init_date

    return data_frame


def read_nc_multiple_paths(path_list, locations_dict):
    """
    Reads multiple initialisation folders and drops duplicate timestamps for each site.
    Takes new timestamps from latest init dat
    :param path_list: paths to read / initialisation folders
    :type path_list: list of valid paths
    :param locations_dict: locations containing dict {site : (lat, lon), site2: (lat2, lon2). . . .}
    :return: data frame of site-timestamps and nc vars
    """
    data_frame = pd.DataFrame()
    for path in path_list:
        dat = read_nc_from_folder(folder_path=path,
                                  location_dict=locations_dict)
        data_frame = pd.concat([dat, data_frame], axis=0)
    data_frame = data_frame.sort_values(by='init_date', ascending=True)
    data_frame = data_frame.drop_duplicates(subset=['Times', 'site_name'],
                                            keep='last').reset_index(drop=True)

    return data_frame

