import os
import pathlib
import sqlalchemy
import pandas as pd

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class GoogleMyBusiness(Customizer):

    def __init__(self):
        super().__init__()
        self.set_attribute("secrets_path", str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

    def get_gmb_accounts(self) -> list:
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT
                    account_name
                FROM public.source_gmb_accountmaster;
                """
            )
            results = con.execute(sql).fetchall()
            return [
                result['account_name'] for result in results
            ] if results else []


class GoogleMyBusinessInsights(GoogleMyBusiness):

    custom_columns = [
        {'data_source': 'Google My Business - Insights'},
        {'property': None},
        {'service_line': None}
    ]

    audit_procedure = {
        'name': 'googlemybusiness_audit',
        'active': 1,
        'code': """

            """,
        'return': 'integer',
        'owner': 'postgres'
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

        # audit procedure
        self.set_attribute('audit_procedure', self.audit_procedure)

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
            'Date': 'report_date',
            'Listing_ID': 'listing_id',
            'Listing_Name': 'listing_name',
            'VIEWS_MAPS': 'maps_views',
            'VIEWS_SEARCH': 'search_views',
            'ACTIONS_WEBSITE': 'website_click_actions',
            'ACTIONS_PHONE': 'phone_call_actions',
            'ACTIONS_DRIVING_DIRECTIONS': 'driving_direction_actions',
            'PHOTOS_VIEWS_CUSTOMERS': 'photo_views_customers',
            'PHOTOS_VIEWS_MERCHANT': 'photo_views_merchant',
            'QUERIES_CHAIN': 'branded_searches',
            'QUERIES_DIRECT': 'direct_searches',
            'QUERIES_INDIRECT': 'discovery_searches',
            'LOCAL_POST_VIEWS_SEARCH': 'post_views_on_search',
            # 'LOCAL_POST_ACTIONS_CALL_TO_ACTION': 'post_cta_actions'
        })

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        # noinspection PyUnresolvedReferences
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['listing_id'] = df['listing_id'].astype(str)
        df['listing_name'] = df['listing_name'].astype(str).str[:50]
        df['phone_call_actions'] = df['phone_call_actions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['driving_direction_actions'] = df['driving_direction_actions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['photo_views_customers'] = df['photo_views_customers'].fillna('0').apply(lambda x: int(x) if x else None)
        df['photo_views_merchant'] = df['photo_views_merchant'].fillna('0').apply(lambda x: int(x) if x else None)
        df['maps_views'] = df['maps_views'].fillna('0').apply(lambda x: int(x) if x else None)
        df['search_views'] = df['search_views'].fillna('0').apply(lambda x: int(x) if x else None)
        df['website_click_actions'] = df['website_click_actions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['branded_searches'] = df['branded_searches'].fillna('0').apply(lambda x: int(x) if x else None)
        df['direct_searches'] = df['direct_searches'].fillna('0').apply(lambda x: int(x) if x else None)
        df['discovery_searches'] = df['discovery_searches'].fillna('0').apply(lambda x: int(x) if x else None)
        df['post_views_on_search'] = df['post_views_on_search'].fillna('0').apply(lambda x: int(x) if x else None)
        # df['post_cta_actions'] = df['post_cta_actions'].fillna('0').apply(lambda x: int(x) if x else None)

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

    def post_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        df['photo_views'] = (df['photo_views_customers'] + df['photo_views_merchant'])
        del df['photo_views_customers']
        del df['photo_views_merchant']

        return df


class GoogleMyBusinessReviews(GoogleMyBusiness):

    custom_columns = [
        {'data_source': 'Google My Business - Reviews'},
        {'property': None},
        {'service_line': None}
    ]

    audit_procedure = {
        'name': 'googlemybusiness_audit',
        'active': 1,
        'code': """

                """,
        'return': 'integer',
        'owner': 'postgres'
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

        # audit procedure
        self.set_attribute('audit_procedure', self.audit_procedure)

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
            'channelGrouping': 'channel_grouping',
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
        df['channel_grouping'] = df['channel_grouping'].astype(str).str[:100]
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

