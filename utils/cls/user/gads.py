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

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2018-01-01')
        self.set_attribute('historical_end_date', '2018-04-22')
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', 'Google Ads - Campaign')
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

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

        return df

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:

        return df

    def post_processing(self) -> None:
        """
        Handles custom sql UPDATE / JOIN post-processing needs for reporting tables,
        :return:
        """

        # CUSTOM SQL QUERIES HERE, ADD AS MANY AS NEEDED
        sql = """ CUSTOM SQL HERE """

        sql2 = """ CUSTOM SQL HERE """

        custom_sql = [
            sql,
            sql2
        ]

        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            for query in custom_sql:
                con.execute(query)

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
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

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

        return df

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        if getattr(self, f'{self.prefix}_custom_columns'):
            for row in getattr(self, f'{self.prefix}_custom_columns'):
                for key, value in row.items():
                    df[key] = value

        return df

    def post_processing(self) -> None:
        """
        Handles custom sql UPDATE / JOIN post-processing needs for reporting tables,
        :return:
        """

        # CUSTOM SQL QUERIES HERE, ADD AS MANY AS NEEDED
        sql = """ CUSTOM SQL HERE """

        sql2 = """ CUSTOM SQL HERE """

        custom_sql = [
            sql,
            sql2
        ]

        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            for query in custom_sql:
                con.execute(query)

        return
