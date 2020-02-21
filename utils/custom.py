"""
Custom

This script is where all reporting configuration takes place
"""
import sys
import inspect
import pandas as pd


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization
    """
    # attributes
    prefix = ''

    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'dbms',
        'client',
        'project',
        'version',
        'recipients'
    ]
    supported_dbms = [
        'postgresql'
    ]
    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    # ### START EDITING HERE ###
    dbms = 'postgresql'
    client = '<CLIENT HERE>'
    project = '<PROJECT HERE>'
    version = '<VERSION HERE>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com'
    ]
    # ### END EDITING ###

    def __init__(self):
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
            if attribute == 'dbms':
                if getattr(self, attribute) not in self.supported_dbms:
                    return False
        return True


# DATA SOURCE SPECIFIC - REQUIRED FOR INDIVIDUAL SCRIPTS TO BE RUN

class GoogleAnalyticsTrafficCustomizer(Customizer):
    # class configuration
    prefix = 'google_analytics_traffic'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_view_id', '1234')
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_metrics', [
            'sessions',
            'percentNewSessions',
            'pageviews',
            'uniquePageviews',
            'pageviewsPerSession',
            'entrances',
            'bounces',
            'sessionDuration',
            'users',
            'newUsers'
        ])
        setattr(self, f'{self.prefix}_dimensions', [
            'date',
            'channelGrouping',
            'sourceMedium',
            'device',
            'campaign',
            'page',
        ])

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'googleanalytics_traffic',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'community', 'type': 'character varying', 'length': 100},
                {'name': 'service_line', 'type': 'character varying', 'length': 100},
                {'name': 'view_id', 'type': 'character varying', 'length': 25},
                {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                {'name': 'device', 'type': 'character varying', 'length': 50},
                {'name': 'campaign', 'type': 'character varying', 'length': 100},
                {'name': 'page', 'type': 'character varying', 'length': 500},
                {'name': 'sessions', 'type': 'character varying', 'length': 500},
                {'name': 'percent_new_sessions', 'type': 'double precision'},
                {'name': 'pageviews', 'type': 'bigint'},
                {'name': 'unique_pageviews', 'type': 'bigint'},
                {'name': 'pageviews_per_session', 'type': 'double precision'},
                {'name': 'entrances', 'type': 'bigint'},
                {'name': 'bounces', 'type': 'bigint'},
                {'name': 'session_duration', 'type': 'double precision'},
                {'name': 'users', 'type': 'bigint'},
                {'name': 'new_users', 'type': 'bigint'},
            ],
            'indexes': [
                {
                    'name': 'ix_google_analytics_traffic',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'medium', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                    ]
                }
            ],
            'owner': 'postgres'
        })

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'googleanalytics_backfilter',
            'active': 1,
            'code': """
            UPDATE ...
    
            SELECT 1;
            """,
            'return': 'integer',
            'owner': 'postgres'
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'googleanalytics_audit',
            'active': 1,
            'code': """
    
            """,
            'return': 'integer',
            'owner': 'postgres'
        })

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date'
        })

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in getattr(self, f'{self.prefix}_schema')['columns']:
            assert column['name'] in df.columns
            if column['type'] == 'character varying':
                assert 'length' in column.keys()
                df[column['name']] = df[column['name']].apply(lambda x: str(x)[:column['length']] if x else None)
            elif column['type'] == 'bigint':
                df[column['name']] = df[column['name']].apply(lambda x: int(x) if x else None)
            elif column['type'] == 'double precision':
                df[column['name']] = df[column['name']].apply(lambda x: float(x) if x else None)
            elif column['type'] == 'date':
                df[column['name']] = pd.to_datetime(df[column['name']])
            elif column['type'] == 'timestamp without time zone':
                df[column['name']] = pd.to_datetime(df[column['name']])
            elif column['type'] == 'datetime with time zone':
                # TODO(jschroeder) how better to interpret timezone data?
                df[column['name']] = pd.to_datetime(df[column['name']], utc=True)
        return df

    # methods
    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_parse(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse the data by pagePath to their respective entities
        :return:
        """
        return df


# NO EDITING BEYOND THIS POINT
# ````````````````````````````````````````````````````````````````````````````````````````````````````
# GRC UTILITY FUNCTIONS
def get_customizers() -> list:
    """
    Return all custom.py Class objects in [(name, <class>)] format
    :return:
    """
    cls_members = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    return cls_members  # as list


def get_customizer(calling_file: str) -> Customizer:
    """
    Loop through and initialize each class in custom.py
    Return the proper Customizer instance that will have the necessary attributes, methods and schema
    for the calling file
    :param calling_file:
    :return:
    """
    cls_members = get_customizers()
    target_attribute = f'{calling_file}_class'
    for cls in cls_members:
        ini_cls = cls[1]()  # initialize the class
        if hasattr(ini_cls, target_attribute):
            if getattr(ini_cls, target_attribute):
                return ini_cls
    raise AssertionError("No configured classes for data source {}".format(calling_file))
