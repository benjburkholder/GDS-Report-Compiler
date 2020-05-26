import pandas as pd
import sqlalchemy
import datetime
import pathlib
import os

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class Moz(Customizer):

    def __init__(self):
        super().__init__()
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

    def pull_moz_local_accounts(self):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_localaccountmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            accounts_cleaned = [{'account': account[0], 'label': account[1]} for account in accounts_raw if
                                accounts_raw]

            return accounts_cleaned

    def pull_moz_pro_accounts(self):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_procampaignmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            campaign_ids = [{'campaign_id': campaign_id} for campaign_id in accounts_raw[0]]

            return campaign_ids

    def exclude_moz_directories(self, df):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT *
                FROM public.source_moz_directoryexclusions;
                """
            )

            result = con.execute(sql)
            exclusions_raw = result.fetchall()

            exclusion_list = [exclusion[0] for exclusion in exclusions_raw]

            if exclusion_list:
                df_cleaned = df.loc[
                             ~(df['directory'].isin(exclusion_list))
                             ,
                             :

                             ]

            return df_cleaned if True else df


class MozProRankingsCustomizer(Moz):

    custom_columns = [

        {'data_source': 'Moz Pro - Rankings'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', datetime.date(2020, 1, 1))
        self.set_attribute('historical_end_date', datetime.date(2020, 4, 1))
        self.set_attribute('table', self.prefix)

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
        df['id'] = df['id'].astype(str).str[:100]
        df['search_id'] = df['search_id'].astype(str).str[:100]
        df['keyword'] = df['keyword'].astype(str).str[:100]
        df['search_engine'] = df['search_engine'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:100]
        df['geo'] = df['geo'].astype(str).str[:100]
        df['tags'] = df['tags'].astype(str).str[:250]
        df['url'] = df['url'].astype(str).str[:1000]
        # df['keyword_added_at'] = df['keyword_added_at'].dt.date
        df['rank'] = df['rank'].fillna('0').apply(lambda x: int(x) if x else None)
        df['branded'] = df['branded'].fillna('0').apply(lambda x: int(x) if x else None)

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


class MozProSerpCustomizer(Moz):

    custom_columns = [

        {'data_source': 'Moz Pro - SERP'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', datetime.date(2020, 1, 1))
        self.set_attribute('historical_end_date', datetime.date(2020, 4, 1))
        self.set_attribute('table', self.prefix)

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

        })

        # noinspection PyMethodMayBeStatic

    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['campaign_id'] = df['campaign_id'].astype(str).str[:100]
        df['id'] = df['id'].astype(str).str[:100]
        df['search_id'] = df['search_id'].astype(str).str[:100]
        df['keyword'] = df['keyword'].astype(str).str[:100]
        df['search_engine'] = df['search_engine'].astype(str).str[:100]
        df['device'] = df['device'].astype(str).str[:100]
        df['geo'] = df['geo'].astype(str).str[:100]
        df['tags'] = df['tags'].astype(str).str[:250]
        df['url'] = df['url'].astype(str).str[:1000]
        # df['keyword_added_at'] = df['keyword_added_at'].dt.date
        df['ads_bottom'] = df['ads_bottom'].fillna('0').apply(lambda x: int(x) if x else None)
        df['ads_top'] = df['ads_top'].fillna('0').apply(lambda x: int(x) if x else None)
        df['featured_snippet'] = df['featured_snippet'].fillna('0').apply(lambda x: int(x) if x else None)
        df['image_pack'] = df['image_pack'].fillna('0').apply(lambda x: int(x) if x else None)
        df['in_depth_articles'] = df['in_depth_articles'].fillna('0').apply(lambda x: int(x) if x else None)
        df['knowledge_card'] = df['knowledge_card'].fillna('0').apply(lambda x: int(x) if x else None)
        df['knowledge_panel'] = df['knowledge_panel'].fillna('0').apply(lambda x: int(x) if x else None)
        df['local_pack'] = df['local_pack'].fillna('0').apply(lambda x: int(x) if x else None)
        df['local_teaser'] = df['local_teaser'].fillna('0').apply(lambda x: int(x) if x else None)
        df['news_pack'] = df['news_pack'].fillna('0').apply(lambda x: int(x) if x else None)
        df['related_questions'] = df['related_questions'].fillna('0').apply(lambda x: int(x) if x else None)
        df['shopping_results'] = df['shopping_results'].fillna('0').apply(lambda x: int(x) if x else None)
        df['site_links'] = df['site_links'].fillna('0').apply(lambda x: int(x) if x else None)
        df['tweet'] = df['tweet'].fillna('0').apply(lambda x: int(x) if x else None)
        df['video'] = df['video'].fillna('0').apply(lambda x: int(x) if x else None)
        df['branded'] = df['branded'].fillna('0').apply(lambda x: int(x) if x else None)

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


class MozLocalVisibilityCustomizer(Moz):

    custom_columns = [

        {'data_source': 'Moz Local - Visibility Report'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', True)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-04-08')
        self.set_attribute('table', self.prefix)

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
        df['account_name'] = df['account_name'].astype(str).str[:100]
        df['listing_id'] = df['listing_id'].astype(str).str[:25]
        df['directory'] = df['directory'].astype(str).str[:100]
        df['points_reached'] = df['points_reached'].fillna('0').apply(lambda x: int(x) if x else None)
        df['max_points'] = df['max_points'].fillna('0').apply(lambda x: int(x) if x else None)

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


class MozLocalSyncCustomizer(Moz):

    custom_columns = [

        {'data_source': 'Moz Local - Sync Report'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)

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

        })

        # noinspection PyMethodMayBeStatic

    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['account_name'] = df['account_name'].astype(str).str[:100]
        df['listing_id'] = df['listing_id'].astype(str).str[:25]
        df['directory'] = df['directory'].astype(str).str[:100]
        df['field'] = df['field'].astype(str).str[:100]
        df['sync_status'] = df['sync_status'].fillna('0').apply(lambda x: int(x) if x else None)

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


