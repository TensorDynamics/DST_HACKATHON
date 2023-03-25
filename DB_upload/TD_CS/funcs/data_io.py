"""
local data io operations
"""
import os
import pandas as pd


def search_col_in_df(data_frame, search_str):
    """
    returns first match of search string in column names.

    :param data_frame: Dataframe to check
    :type data_frame: pd.DataFrame

    :param search_str: column str
    :type search_str: str

    :return: matching col name to search string
    :rtype: str
    """
    try:
        return [col for col in sorted(data_frame.columns) if search_str.lower() in col.lower()][0]
    except IndexError as ie:
        print(f'Unable to find any column name matching {search_str} in data frame.')
        raise ie


def process_site_table(site_df, site_type=None,):
    """
    Process site dataframe and returns a dictionary {'site_name' : (lat, lon)}
    :param site_df: site dataframe from local disk or database
    :param site_type: wind/solar
    :return: dict of key(site name) and value as (lat,lon)
    :rtype: dict
    """
    if site_type is not None:
        type_locations = site_df[site_df['Type'].apply(lambda x: x.lower()) == site_type]
    else:
        type_locations = site_df.copy()
    site_name_col = search_col_in_df(data_frame=site_df, search_str='site_name')
    lat_col = search_col_in_df(data_frame=site_df, search_str='lat')
    lon_col = search_col_in_df(data_frame=site_df, search_str='lon')
    return {r[site_name_col]: (r[lat_col], r[lon_col]) for _, r in type_locations.iterrows()}


def get_unmatching_rows(data_frame1, data_frame2, return_from='all'):
    """
    Finds different rows from outer and inner df and returns based on return_from.
    :param data_frame1: main dataframe
    :param data_frame2: secondary dataframe
    :param return_from: return different rows from 'datafram1', 'datafram2' or 'all'.
                        If outer_df, rows from outer_df will which do not match with inner_df be return
    :return: different rows from outer_df, inner_df
    :rtype: pd.DataFrame
    """
    outer = data_frame1.copy()
    inner = data_frame2.copy()
    outer['df_identifier'] = 'outer_df'
    inner['df_identifier'] = 'inner_df'
    df = pd.concat([outer, inner], axis=0)
    df = df.reset_index(drop=True)
    df.drop_duplicates(subset=[col for col in df.columns if 'df_identifier' not in col],
                       keep=False, inplace=True)

    if 'all' not in return_from:
        return (df[df['df_identifier'] == return_from]
                .drop('df_identifier', axis=1)
                .reset_index(drop=True))
    else:
        return (df
                .drop('df_identifier', axis=1)
                .reset_index(drop=True))


def get_folders_at_path(path, include_empty=False):
    """
    Lists all folders at a path.
    :param path: path to list dir for
    :type path: path like
    :param include_empty: whether to include empty subdirs at path.
        Optional defaults ot False i.e. all subdirs will be returned
    :type include_empty: bool
    :return: list of all subdirs
    :rtype: list
    """
    all_folders = [fold for fold in os.listdir(path) if os.path.isdir(os.path.join(path, fold))]
    if include_empty:
        return all_folders
    return [
        fold
        for fold in all_folders
        if [
            file
            for file in os.listdir(os.path.join(path, fold))
            if '.DS' not in file
        ]
    ]
