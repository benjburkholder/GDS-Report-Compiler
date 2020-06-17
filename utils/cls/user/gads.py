import pandas as pd
import sqlalchemy
import datetime
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer


class GoogleAds(Customizer):

    def __init__(self):
        super().__init__()
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

        # TODO: is there a way to optimize this?
        drop_columns = {
            'status': False,
            'columns': ['zip', 'phone']
        }
        self.set_attribute('drop_columns', drop_columns)

    def get_account_ids(self) -> list:
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT
                    account_id
                FROM public.source_gads_accountmaster;
                """
            )
            results = con.execute(sql).fetchall()
            return [
                result['account_id'] for result in results
            ] if results else []


class GoogleAdsCampaign(GoogleAds):

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'Google Ads - Campaign'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2018-01-01')
        self.set_attribute('historical_end_date', '2018-04-22')
        self.set_attribute('table', self.prefix)

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
            'Date': 'report_date',
            'Campaign': 'campaign',
            'Campaign_ID': 'campaign_id',
            'Clicks': 'clicks',
            'Cost': 'cost',
            'Device': 'device',
            'Impressions': 'impressions',
            'Search_Impression_Share': 'search_impression_share',
            'Search_Budget_Lost_Impression_Share': 'search_budget_lost_impression_share',
            'Search_Rank_Lost_Impression_Share': 'search_rank_lost_impression_share',
            'Content_Impression_Share': 'content_impression_share',
            'Content_Budget_Lost_Impression_Share': 'content_budget_lost_impression_share',
            'Content_Rank_Lost_Impression_Share': 'content_rank_lost_impression_share',
            'Medium': 'advertising_channel_type',
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
        df['campaign_id'] = df['campaign_id'].astype(str).str[:100]
        df['clicks'] = df['clicks'].fillna('0').apply(lambda x: int(x) if x else None)
        df['cost'] = df['cost'].fillna('0').apply(lambda x: float(x) if x else None)
        df['campaign'] = df['campaign'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:100]
        df['advertising_channel_type'] = df['advertising_channel_type'].astype(str).str[:100]
        df['impressions'] = df['impressions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['search_impression_share'] = df['search_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)
        df['search_budget_lost_impression_share'] = df['search_budget_lost_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)
        df['search_rank_lost_impression_share'] = df['search_rank_lost_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)
        df['content_impression_share'] = df['content_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)
        df['content_budget_lost_impression_share'] = df['content_budget_lost_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)
        df['content_rank_lost_impression_share'] = df['content_rank_lost_impression_share'].fillna('0').apply(lambda x: float(x) if x else None)

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


class GoogleAdsKeyword(GoogleAds):

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'Google Ads - Campaign'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2018-01-01')
        self.set_attribute('historical_end_date', '2018-04-22')
        self.set_attribute('table', self.prefix)

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
        df['pageviews_per_session'] = df['pageviews_per_session'].fillna('0').apply(
            lambda x: float(x) if x else None)
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
