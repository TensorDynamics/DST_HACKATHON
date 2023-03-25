"""
Extract Data from Database
"""
from sqlalchemy import create_engine
import connectorx as cx
import pandas as pd


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


class SiteDataExtractor:

    def __init__(self,
                 table_name,
                 schema_name,
                 site_name,
                 date_start=None,
                 date_end=None,
                 db_connection=None,
                 site_column_label='site_name',
                 datetime_column_label='timestamp',
                 eng='connectorx'):
        """
        General purpose class to extract data for a specific site from the schema.table address of the db connection
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
        self.datetime_column_label = datetime_column_label
        self.eng = eng
        self.date_start = date_start
        self.date_end = date_end
        self.db_str = 'postgresql://admin123:tensor123@tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com:5432/postgres'

    def check_engine(self):
        if 'pandas' in self.eng:
            if self.db_connection is None:
                raise ValueError("For pandas engine, db connection (via sqlalchemy or such) is to be provided.")

    def parse_query(self, query):
        """ parses sql query via the desired engine """
        if self.eng.lower() == 'pandas':
            return pd.read_sql_query(sql=query, con=self.db_connection, parse_dates=[self.datetime_column_label])
        elif self.eng.lower() == 'connectorx':
            return cx.read_sql(conn=self.db_str, query=query)
        else:
            raise NotImplementedError("Only 'pandas' and 'connectorx' are valid choices for eng."
                                      f" In __init__ call. {self.eng} was provided.")

    def read_data(self):
        """ reads data for a specific site from database """
        self.check_engine()
        query = f"select * from {self.schema_name}.{self.table_name} " \
                f"where {self.site_column_label} = '{self.site_name}' "
        if self.date_start:
            query = f"""{query} and "{self.datetime_column_label}" >= '{self.date_start}' """
        if self.date_end:
            query = f"""{query} and "{self.datetime_column_label}" <= '{self.date_end}' """
        return self.parse_query(query=query)


def ev_data_meta():
   return  {'var units': {
                            'latitude': 'degree_north',
                            'longitude': 'degree_east',
                            'temp': 'c',
                            'wind_speed': 'm s-1',
                            'wind_direction': 'degree',
                            'water_vapor_mixing_ratio': 'kg kg-1',
                            'swdown': 'W m-2',
                            'swnorm': 'W m-2"',
                            'swddir': 'W m-2"',
                            'swddni': 'W m-2"',
                            'swddif': 'W m-2"'
                        },
            'var names': {
                            'latitude': 'Latitude',
                            'longitude': 'Longitude',
                            'temp': 'Temperature at 2m',
                            'wind_speed': 'Wind Speed at 10m',
                            'wind_direction': 'Wind direction at 10m',
                            'water_vapor_mixing_ratio': 'Water vapor mixing ratio at 2m',
                            'swdown': 'Downward shortwave flux at ground surface',
                            'swnorm': 'Normal short wave flux at ground surface (slope-dependent)',
                            'swddir': 'Shortwave surface downward direct irradiance',
                            'swddni': 'Shortwave surface downward direct normal irradiance',
                            'swddif': 'Shortwave surface downward diffuse irradiance '
                        },
            }