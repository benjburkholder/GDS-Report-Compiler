import os
import pathlib
import pandas as pd

from utils.cls.core import Customizer


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

    # Enter list of view ids as str
    view_ids = []

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)
        setattr(self, f'{self.prefix}_get_view_ids', self.get_view_ids)

        setattr(self, 'lookup_tables', {
            'status': {
                'table_type': 'ga',
                'active': True,
                'refresh_status': False,
                'lookup_source_sheet': 'URL to Property',
                'schema': 'lookup_url_schema',
                'table_name': 'lookup_urltolocation'
            }}),

        # Schema for URL lookup table
        setattr(self, f'{self.prefix}_lookup_url_schema', {
            'table': 'lookup_urltolocation',
            'schema': 'public',
            'type': 'lookup',
            'columns': [
                {'name': 'url', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'exact', 'type': 'bigint'},
            ],
            'owner': 'postgres'
        })

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

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Google Analytics - Traffic',
            'property': None
        })

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
            UPDATE public.googleanalytics_traffic TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page LIKE CONCAT('%', LOOKUP.url, '%')
            AND LOOKUP.exact = 0;

            UPDATE public.googleanalytics_traffic TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page = LOOKUP.url
            AND LOOKUP.exact = 1;

            UPDATE public.googleanalytics_traffic
            SET
                property = 'Non-Location Pages'
            WHERE property IS NULL;

            CLUSTER public.googleanalytics_traffic;

            SELECT 0;
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

    def google_analytics_traffic_parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

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

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Google Analytics - Events',
            'property': None
        })

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'googleanalytics_events',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'channel_grouping', 'type': 'character varying', 'length': 200},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'service_line', 'type': 'character varying', 'length': 100},
                {'name': 'view_id', 'type': 'character varying', 'length': 25},
                {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                {'name': 'device', 'type': 'character varying', 'length': 50},
                {'name': 'campaign', 'type': 'character varying', 'length': 100},
                {'name': 'page', 'type': 'character varying', 'length': 500},
                {'name': 'event_label', 'type': 'character varying', 'length': 200},
                {'name': 'event_action', 'type': 'character varying', 'length': 200},
                {'name': 'total_events', 'type': 'bigint'},
                {'name': 'unique_events', 'type': 'bigint'},
                {'name': 'event_value', 'type': 'double precision'},

            ],
            'indexes': [
                {
                    'name': 'ix_google_analytics_events',
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
            UPDATE public.googleanalytics_events TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page LIKE CONCAT('%', LOOKUP.url, '%')
            AND LOOKUP.exact = 0;

            UPDATE public.googleanalytics_events TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page = LOOKUP.url
            AND LOOKUP.exact = 1;

            UPDATE public.googleanalytics_events
            SET
                property = 'Non-Location Pages'
            WHERE property IS NULL;

            CLUSTER public.googleanalytics_events;

            SELECT 0;
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

    def google_analytics_events_parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

        return df

    def google_analytics_events_custom_column_assignment(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

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

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'googleanalytics_goals',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'channel_grouping', 'type': 'character varying', 'length': 150},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'service_line', 'type': 'character varying', 'length': 100},
                {'name': 'view_id', 'type': 'character varying', 'length': 25},
                {'name': 'source_medium', 'type': 'character varying', 'length': 100},
                {'name': 'device', 'type': 'character varying', 'length': 50},
                {'name': 'campaign', 'type': 'character varying', 'length': 100},
                {'name': 'page', 'type': 'character varying', 'length': 500},
                {'name': 'request_a_quote', 'type': 'bigint'},
                {'name': 'sidebar_contact_us', 'type': 'bigint'},
                {'name': 'contact_us_form_submission', 'type': 'bigint'},
                {'name': 'newsletter_signups', 'type': 'bigint'},
                {'name': 'dialogtech_calls', 'type': 'bigint'},

            ],
            'indexes': [
                {
                    'name': 'ix_google_analytics_goals',
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
           UPDATE public.googleanalytics_goals TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page LIKE CONCAT('%', LOOKUP.url, '%')
            AND LOOKUP.exact = 0;

            UPDATE public.googleanalytics_goals TARGET
            SET 
                property = LOOKUP.property
            FROM public.lookup_urltolocation LOOKUP
            WHERE TARGET.page = LOOKUP.url
            AND LOOKUP.exact = 1;

            UPDATE public.googleanalytics_goals
            SET
                property = 'Non-Location Pages'
            WHERE property IS NULL;

            CLUSTER public.googleanalytics_goals;

            SELECT 0;
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

    def google_analytics_goals_parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for key, value in getattr(self, f'{self.prefix}_custom_columns').items():
                df[key] = value

        return df

    def google_analytics_goals_post_processing(self):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements
        return


