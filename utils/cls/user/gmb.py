import pandas as pd
import numpy as np
import sqlalchemy
import datetime
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

from googlemybusiness.reporting.client.listing_report import GoogleMyBusinessReporting
TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class GoogleMyBusiness(Customizer):

    credential_name = 'GMBCredential'
    secrets_name = 'GoogleMyBusiness'

    rename_map = {
        'global': {}
    }

    def get_rename_map(self, account_name: str):
        return get_configured_item_by_key(key=account_name, lookup=self.rename_map)

    post_processing_sql_list = []

    def __get_post_processing_sql_list(self) -> list:
        """
        If you wish to execute post-processing on the SOURCE table, enter sql commands in the list
        provided below
        ====================================================================================================
        :return:
        """
        # put this in a function to leave room for customization
        return self.post_processing_sql_list

    def __init__(self):
        super().__init__()
        self.set_attribute("secrets_path", str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        self.get_secrets(include_dat=True)
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('date_col', DATE_COL)

    def ingest_by_listing_id(self, listing_id: str, df: pd.DataFrame, start_date: str, end_date: str) -> None:
        table_schema = self.get_attribute('table_schema')
        table = self.get_attribute('table')
        date_col = self.get_attribute('date_col')

        with self.engine.begin() as con:
            con.execute(
                sqlalchemy.text(
                    f"""
                    DELETE FROM
                    {table_schema}.{table}
                    WHERE {date_col} BETWEEN :start_date AND :end_date
                    AND listing_id = :listing_id;
                    """
                ),
                start_date=start_date,
                end_date=end_date,
                listing_id=listing_id
            )
            df.to_sql(
                table,
                con=con,
                if_exists='append',
                index=False,
                index_label=None
            )

        print(f'Data pulled for {start_date} and {end_date} for listing_id: {listing_id}.')

    @staticmethod
    def get_date_range(start_date: datetime.datetime, end_date: datetime.datetime) -> list:
        return pd.date_range(start=start_date, end=end_date).to_list()

    def calculate_date(self, start_date: bool = True) -> datetime.datetime:
        if self.get_attribute('historical'):
            if start_date:
                return datetime.datetime.strptime(self.get_attribute('historical_start_date'), '%Y-%m-%d')
            else:
                return datetime.datetime.strptime(self.get_attribute('historical_end_date'), '%Y-%m-%d')
        else:
            if start_date:
                return datetime.datetime.today() - datetime.timedelta(540)
            else:
                return datetime.datetime.today() - datetime.timedelta(1)

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
            by='create_time',
            ascending=False,
            inplace=True
        )
        df['Average_Rating'] = None
        first_review_date = df['create_time'].unique()[-1]
        for date in df['create_time'].unique()[::-1]:
            # get all reviews up to and including the ones on this date
            reviews = df.loc[df['create_time'] > first_review_date, ['create_time', 'star_rating']]
            reviews = reviews.loc[reviews['create_time'] <= date, :]
            # compute the mean
            average_rating = np.mean(list(reviews['star_rating']))
            # assign the mean for this date
            df['Average_Rating'][df['create_time'] == date] = average_rating
        # round to double precision
        df['Average_Rating'] = df['Average_Rating'].apply(lambda x: round(x, 2) if x else None)
        # fill empty cells with star_rating value
        df['Average_Rating'].fillna(df['star_rating'], inplace=True)

        return df

    def get_filtered_accounts(self, gmb_client: GoogleMyBusinessReporting) -> list:
        conf_accounts = self.get_gmb_accounts()
        all_accounts = gmb_client.get_accounts()
        return [
            account for account in all_accounts
            if account['account_name'] in conf_accounts
        ]

    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return: df
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

    def post_processing(self) -> None:
        """
        Handles custom SQL statements for the SOURCE table due to bad / mismatched data (if any)
        ====================================================================================================
        :return:
        """
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            for query in self.__get_post_processing_sql_list():
                con.execute(query)
        return

    def backfilter(self):
        self.backfilter_statement()
        print('SUCCESS: Table Backfiltered.')

    def ingest(self):
        self.ingest_statement()
        print('SUCCESS: Table Ingested.')

