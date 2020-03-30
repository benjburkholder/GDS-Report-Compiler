import os
import pathlib
import pandas as pd

from utils.cls.core import Customizer


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

    # Enter list of view ids as str
    view_ids = []

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)
        setattr(self, f'{self.prefix}_get_view_ids', self.get_view_ids)

        setattr(self, f'{self.prefix}_drop_columns', {
          'status': False,
          'columns': ['zip', 'phone']
        })

    def get_view_ids(self) -> list:
        """
        Required hook, user is free to provide this list of dictionaries as they choose
        """
        return self.view_ids


class GoogleAnalyticsTrafficCustomizer(GoogleAnalytics):

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
            'sourceMedium': 'source_medium',
            'channelGrouping': 'channel_grouping',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'page',
            'sessions': 'sessions',
            'percentNewSessions': 'percent_new_sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'pageviews': 'pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'entrances': 'entrances',
            'bounces': 'bounces',
            'sessionDuration': 'session_duration',
            'users': 'users',
            'newUsers': 'new_users'
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
        # TODO(jschroeder) flesh out this example a bit more
        return df.rename(columns={
            'date': 'report_date',
            'channelGrouping': 'channel_grouping',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'pagePath': 'page',
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
