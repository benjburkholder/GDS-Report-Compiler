import pandas as pd
import numpy as np
import sqlalchemy
import pathlib
import os

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

    def assign_average_rating(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rather than use a rolling average
        Compute the historical to date average
        For each review in the dataset
        """
        df.sort_values(
            by='Date',
            ascending=False,
            inplace=True
        )
        df['Average_Rating'] = None
        first_review_date = df['Date'].unique()[-1]
        for date in df['Date'].unique()[::-1]:
            # get all reviews up to and including the ones on this date
            reviews = df.loc[df['Date'] > first_review_date, ['Date', 'Rating']]
            reviews = reviews.loc[reviews['Date'] <= date, :]
            # compute the mean
            average_rating = np.mean(list(reviews['Rating']))
            # assign the mean for this date
            df['Average_Rating'][df['Date'] == date] = average_rating
        # round to double precision
        df['Average_Rating'] = df['Average_Rating'].apply(lambda x: round(x, 2))
        return df


class GoogleMyBusinessInsights(GoogleMyBusiness):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', 'Google My Business - Insights')

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

        df['photo_views'] = (df['photo_views_customers'] + df['photo_views_merchant'])
        del df['photo_views_customers']
        del df['photo_views_merchant']

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


class GoogleMyBusinessReviews(GoogleMyBusiness):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', 'Google My Business - Reviews')

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
            'Reviewer': 'reviewer',
            'Rating': 'rating',
            'Review_Count': 'review_count',
            'Average_Rating': 'average_rating',
            'Listing_ID': 'listing_id',
            'Listing_Name': 'listing_name'

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
        df['listing_name'] = df['listing_name'].astype(str).str[:150]
        df['listing_id'] = df['listing_id'].astype(str).str[:150]
        df['average_rating'] = df['average_rating'].fillna('0').apply(lambda x: float(x) if x else None)
        df['rating'] = df['rating'].fillna('0').apply(lambda x: float(x) if x else None)
        df['review_count'] = df['review_count'].fillna('0').apply(lambda x: float(x) if x else None)
        df['reviewer'] = df['reviewer'].astype(str).str[:150]

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

        del df['Data_Source']

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

