import os
import pathlib
import sqlalchemy
import pandas as pd

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class GoogleAnalytics(Customizer):

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

    def __init__(self):
        super().__init__()
        # self.set_attribute('secret_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)
        setattr(self, f'{self.prefix}_get_view_ids', self.get_view_ids)

        setattr(self, f'{self.prefix}_drop_columns', {
          'status': False,
          'columns': ['zip', 'phone']
        })

    def get_view_ids(self) -> list:
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT
                    view_id
                FROM public.source_ga_views;
                """
            )
            results = con.execute(sql).fetchall()
            return [
                result['view_id'] for result in results
            ] if results else []


class GoogleAnalyticsTrafficCustomizer(GoogleAnalytics):

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', False)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'google_analytics_traffic')
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

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Google Analytics - Traffic',
            'property': None
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

        # reporting model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'google_analytics_traffic',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'channel_grouping', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
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

        # Add processing stages as class attributes
        #setattr(self, f'{self.prefix}_rename', self.rename)
        #setattr(self, f'{self.prefix}_type', self.type)

    # noinspection PyMethodMayBeStatic
    def getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'channel_grouping',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'percentNewSessions': 'percent_new_sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'sessionDuration': 'session_duration',
            'newUsers': 'new_users'
        }, inplace=True)

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

        return df

    def post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


class GoogleAnalyticsEventsCustomizer(GoogleAnalytics):

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

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Google Analytics - Events',
            'property': None
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
    def getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        return df.rename(columns={
            'date': 'report_date',
            'channelGrouping': 'channel_grouping',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'eventLabel': 'event_label',
            'eventAction': 'event_action',
            'totalEvents': 'total_events',
            'uniqueEvents': 'unique_events',
            'eventValue': 'event_value'
        })

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        df['view_id'] = df['view_id'].astype(str)[:25]
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['channel_grouping'] = df['channel_grouping'].astype(str)[:100]
        df['source_medium'] = df['source_medium'].astype(str)[:100]
        df['device'] = df['device'].astype(str)[:50]
        df['campaign'] = df['campaign'].astype(str)[:255]
        df['url'] = df['url'].astype(str)[:1000]
        df['event_label'] = df['event_label'].astype(str)[:1000]
        df['event_action'] = df['event_action'].astype(str)[:255]
        df['total_events'] = df['total_events'].fillna('0').apply(lambda x: int(x) if x else None)
        df['unique_events'] = df['total_events'].fillna('0').apply(lambda x: int(x) if x else None)
        df['event_value'] = df['event_value'].fillna('0').apply(lambda x: float(x) if x else None)

        # TODO: Later optimization... keeping the schema for the table in the customizer
        #   - and use it to reference typing command to df
        '''
        for column in self.get_attribute('schema')['columns']:
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
        '''
        return df

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

        return df

    def google_analytics_events_custom_column_assignment(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value
        return df

    def post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


class GoogleAnalyticsGoalsCustomizer(GoogleAnalytics):

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'googleanalytics_goals')
        setattr(self, f'{self.prefix}_metrics', [
            'goal5Completions',
            'goal7Completions',
            'goal6Completions',
            'goal3Completions',
            'goal4Completions',
        ])
        setattr(self, f'{self.prefix}_dimensions', [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath'
        ])

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Google Analytics - Goals',
            'property': None
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
    def getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date',
            'view_id': 'view_id',
            'channelGrouping': 'channel_grouping',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'page',
            'sessions': 'sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'pageviews': 'pageviews',
            'goal3Completions': 'request_a_quote',
            'goal4Completions': 'sidebar_contact_us',
            'goal5Completions': 'contact_us_form_submission',
            'goal6Completions': 'newsletter_signups',
            'goal7Completions': 'dialogtech_calls',

        })

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

        return df

    def post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return
