import pandas as pd
import sqlalchemy
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
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

    def __init__(self):
        super().__init__()
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

        # TODO: is there a way to optimize this?
        drop_columns = {
            'status': False,
            'columns': ['zip', 'phone']
        }
        self.set_attribute('drop_columns', drop_columns)

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

    metrics = [
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
    ]

    dimensions = [
        'date',
        'channelGrouping',
        'sourceMedium',
        'deviceCategory',
        'campaign',
        'pagePath',
    ]

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'Google Analytics - Traffic'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

    # noinspection PyMethodMayBeStatic
    def getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        # TODO: with a new version of GA that accepts function pointers
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
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'percentNewSessions': 'percent_new_sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'sessionDuration': 'session_duration',
            'newUsers': 'new_users'
        })

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        df['view_id'] = df['view_id'].astype(str).str[:25]
        # noinspection PyUnresolvedReferences
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['medium'] = df['medium'].astype(str).str[:100]
        df['source_medium'] = df['source_medium'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:50]
        df['campaign'] = df['campaign'].astype(str).str[:100]
        df['url'] = df['url'].astype(str).str[:500]
        df['sessions'] = df['sessions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['percent_new_sessions'] = df['percent_new_sessions'].fillna('0').apply(lambda x: float(x) if x else None)
        df['pageviews'] = df['pageviews'].fillna('0').apply(lambda x: int(x) if x else None)
        df['unique_pageviews'] = df['unique_pageviews'].fillna('0').apply(lambda x: int(x) if x else None)
        df['pageviews_per_session'] = df['pageviews_per_session'].fillna('0').apply(lambda x: float(x) if x else None)
        df['entrances'] = df['entrances'].fillna('0').apply(lambda x: int(x) if x else None)
        df['bounces'] = df['bounces'].fillna('0').apply(lambda x: int(x) if x else None)
        df['session_duration'] = df['session_duration'].fillna('0').apply(lambda x: float(x) if x else None)
        df['users'] = df['users'].fillna('0').apply(lambda x: int(x) if x else None)
        df['new_users'] = df['new_users'].fillna('0').apply(lambda x: int(x) if x else None)

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
            for row in getattr(self, f'{self.prefix}_custom_columns'):
                for key, value in row.items():
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

    metrics = [
        'totalEvents',
        'uniqueEvents',
        'eventValue'
    ]

    dimensions = [
        'date',
        'channelGrouping',
        'sourceMedium',
        'deviceCategory',
        'campaign',
        'pagePath',
        'eventLabel',
        'eventAction',
    ]

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'Google Analytics - Events'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

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
            'channelGrouping': 'medium',
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
        df['view_id'] = df['view_id'].astype(str).str[:25]
        # noinspection PyUnresolvedReferences
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['medium'] = df['medium'].astype(str).str[:100]
        df['source_medium'] = df['source_medium'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:50]
        df['campaign'] = df['campaign'].astype(str).str[:255]
        df['url'] = df['url'].astype(str).str[:1000]
        df['event_label'] = df['event_label'].astype(str).str[:200]
        df['event_action'] = df['event_action'].astype(str).str[:255]
        df['total_events'] = df['total_events'].fillna('0').apply(lambda x: int(x) if x else None)
        df['unique_events'] = df['unique_events'].fillna('0').apply(lambda x: int(x) if x else None)
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
            for row in getattr(self, f'{self.prefix}_custom_columns'):
                for key, value in row.items():
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
    metrics = [
            'goal5Completions',
            'goal7Completions',
            'goal6Completions',
            'goal3Completions',
            'goal4Completions',
    ]

    dimensions = [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath'
    ]

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'Google Analytics - Goals'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

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
            'channelGrouping': 'medium',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'campaign': 'campaign',
            'pagePath': 'url',
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
        df['view_id'] = df['view_id'].astype(str).str[:25]
        # noinspection PyUnresolvedReferences
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['medium'] = df['medium'].astype(str).str[:100]
        df['source_medium'] = df['source_medium'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:50]
        df['campaign'] = df['campaign'].astype(str).str[:255]
        df['url'] = df['url'].astype(str).str[:1000]
        df['request_a_quote'] = df['request_a_quote'].fillna('0').apply(lambda x: int(x) if x else None)
        df['sidebar_contact_us'] = df['sidebar_contact_us'].fillna('0').apply(lambda x: int(x) if x else None)
        df['contact_us_form_submission'] = df['contact_us_form_submission'].fillna('0').apply(lambda x: int(x) if x else None)
        df['newsletter_signups'] = df['newsletter_signups'].fillna('0').apply(lambda x: int(x) if x else None)
        df['dialogtech_calls'] = df['dialogtech_calls'].fillna('0').apply(lambda x: int(x) if x else None)

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
            for row in getattr(self, f'{self.prefix}_custom_columns'):
                for key, value in row.items():
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
