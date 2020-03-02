"""
Custom

This script is where all reporting configuration takes place
"""
import os
import sys
import inspect
import pathlib
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
    client = '<CLIENT>'
    project = '<PROJECT>'
    version = '<VERSION>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com'
    ]
    db = {
        'DATABASE': 'fairfieldresidences_omnilocal',
        'USERNAME': 'python-2',
        'PASSWORD': 'pythonpipelines',
        'SERVER': '35.222.11.147'
    }
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

class GoogleAnalytics(Customizer):
    prefix = 'google_analytics'

    supported_metrics = [

    ]
    supported_dimensions = [
        'date',
        'channelGrouping',
        'sourceMedium',
        'deviceCategory',
        'campaign',
        'page',
    ]

    view_ids = [
        '107395718'
    ]

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent))
        setattr(self, f'{self.prefix}_client_name', self.client)
        setattr(self, f'{self.prefix}_get_view_ids', self.get_view_ids)

    def get_view_ids(self) -> list:
        """
        Required hook, user is free to provide this list of dictionaries as they choose
        """
        return [
            '107395718'
        ]


class GoogleAnalyticsTrafficCustomizer(GoogleAnalytics):
    # class configuration
    prefix = 'google_analytics_traffic'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'googleanalytics_traffic')
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
            'deviceCategory',
            'campaign',
            'pagePath',
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
                        {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
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
            'date': 'report_date',
            'view_id': 'view_id',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'page',
            'sessions': 'sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'pageviews': 'pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'entrances': 'entrances',
            'bounces': 'bounces',
            'sessionDuration': 'session_duration',
            'users': 'users',
            'new_users': 'new_users'
        })


    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in getattr(self, f'{self.prefix}_schema')['columns']:
            if column['name'] in df.columns:
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

    def google_analytics_traffic_post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


class GoogleAnalyticsEventsCustomizer(GoogleAnalytics):
    # class configuration
    prefix = 'google_analytics_events'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'googleanalytics_events')
        setattr(self, f'{self.prefix}_metrics', [
            'totalEvents',
            'uniqueEvents',
            'eventValue'
        ])
        setattr(self, f'{self.prefix}_dimensions', [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath',
            'eventLabel',
            'eventAction',
        ])

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'googleanalytics_events',
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
                    'name': 'ix_google_analytics_events',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
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
    def google_analytics_events_getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def google_analytics_events_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date',
            'view_id': 'view_id',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'page',
            'sessions': 'sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'pageviews': 'pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'entrances': 'entrances',
            'bounces': 'bounces',
            'sessionDuration': 'session_duration',
            'users': 'users',
            'new_users': 'new_users'
        })

    # noinspection PyMethodMayBeStatic
    def google_analytics_events_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in getattr(self, f'{self.prefix}_schema')['columns']:
            if column['name'] in df.columns:
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

    def google_analytics_event_post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


class GoogleAnalyticsGoalsCustomizer(GoogleAnalytics):
    # class configuration
    prefix = 'google_analytics_goals'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'googleanalytics_goals')
        setattr(self, f'{self.prefix}_metrics', [
            'goal1Completions',  # Contact Form
        ])
        setattr(self, f'{self.prefix}_dimensions', [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath'
        ])

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'googleanalytics_goals',
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
                    'name': 'ix_google_analytics_goals',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
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
    def google_analytics_goals_getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def google_analytics_goals_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date',
            'view_id': 'view_id',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'page',
            'sessions': 'sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'pageviews': 'pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'entrances': 'entrances',
            'bounces': 'bounces',
            'sessionDuration': 'session_duration',
            'users': 'users',
            'new_users': 'new_users'
        })

    # noinspection PyMethodMayBeStatic
    def google_analytics_goals_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in getattr(self, f'{self.prefix}_schema')['columns']:
            if column['name'] in df.columns:
                if column['type'] == 'character varying':
                    assert 'length' in column.keys()
                    df[column['name']] = df[column['name']].apply(
                        lambda x: str(x)[:column['length']] if x else None)
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

    def google_analytics_goals_post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


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
