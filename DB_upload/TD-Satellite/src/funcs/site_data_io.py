"""
Site data fetch from DB classes
"""
import connectorx as cx
import pandas as pd
from src.funcs import db_io


class SiteConfigExtractor:

    def __init__(self, db_connection, table_name, schema_name, site_type=None):
        """
        Reads data table containing site meta
        :param db_connection: sql alchemy db connection
        :param table_name: str, table name in the db to extract from
        :param schema_name: str, schema name in the db to extract from
        :param site_type: str, ['solar', 'wind', None]. Defaults to None
            i.e. will read all rows irrespective of site types
        """
        self.db_connection = db_connection
        self.table_name = table_name
        self.schema_name = schema_name
        self.site_type = site_type

    def read_table_contents(self):
        """ reads entire table """
        return db_io.read_table_to_df(con=self.db_connection, 
                                      table_str=self.table_name, 
                                      schema=self.schema_name)

    @staticmethod
    def filter_site_type(site_df, site_type=None):
        if site_type is not None:
            type_locations = site_df[site_df['type'].apply(lambda x: x.lower()) == site_type.lower()].copy()
        else:
            type_locations = site_df.copy().drop_duplicates(subset=['site_name'])
        return type_locations

    def extract_locations(self):
        """
        Process site dataframe and returns a dictionary {'site_name' : (lat, lon)}
        :return: dict of key(site name) and value as (lat,lon)
        :rtype: dict
        """
        site_df = self.read_table_contents()
        type_locations = self.filter_site_type(site_df=site_df, site_type=self.site_type)
        return {r['site_name']: (r['latitude'], r['longitude']) for _, r in type_locations.iterrows()}

    def extract_site_types(self):
        """
        Process site dataframe and returns a dictionary {'site_name' : 'Solar', 'site_name2': 'Wind', ...}
        :return: dict of key(site name) and value as (lat,lon)
        :rtype: dict
        """
        site_df = self.read_table_contents()
        type_locations = self.filter_site_type(site_df=site_df, site_type=self.site_type)
        return {r['site_name']: r['type'] for _, r in type_locations.iterrows()}
    
    def extract_site_capacities(self):
        site_df = self.read_table_contents()
        type_locations = self.filter_site_type(site_df=site_df, site_type=self.site_type)
        return {r['site_name']: r['capacity'] for _, r in type_locations.iterrows()}
    
    def site_names(self):
        return list(self.read_table_contents()['site_name'].unique())


class SiteDataExtractor:

    def __init__(self, 
                 db_connection, 
                 table_name,
                 schema_name,
                 site_name,
                 site_column_label='site_name',
                 eng='pandas'):
        """
        General purpose class to extract data for a specific site from the schema.
            Table address of the db connection
        :param db_connection: sql alchemy db connection
        :param table_name: str, table name in the db to extract from
        :param schema_name: str, schema name in the db to extract from
        :param site_column_label: str, site identifier column label i.e. name of column containing the site names
        :param eng: str, ['connectorx', 'pandas']. Defaults to pandas. Can show time improvements in production.
        """
        self.db_connection = db_connection
        self.table_name = table_name
        self.schema_name = schema_name
        self.site_name = site_name
        self.site_column_label = site_column_label
        self.eng = eng
        self.db_str = 'postgresql://admin123:tensor123@tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com:5432/postgres'

    def parse_query(self, query):
        """ parses sql query via the desired engine """
        if self.eng.lower() == 'pandas':
            return pd.read_sql_query(sql=query, con=self.db_connection)
        elif self.eng.lower() == 'connectorx':
            return cx.read_sql(conn=self.db_str, query=query)
        else:
            raise NotImplementedError(f"Only 'pandas' and 'connectorx' are valid choices for eng in __init__ call. "
                                      f"{self.eng} was provided.")

    def read_data(self):
        """ reads data for a specific site from database """
        query = f"select * from {self.schema_name}.{self.table_name} " \
                f"where {self.site_column_label} = '{self.site_name}' "
        return self.parse_query(query=query)
