import pandas as pd
import sqlalchemy
import calendar
import datetime
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

from googlesearchconsole.reporting.client.search_analytics import SearchAnalytics as SearchAnalyticsClient
TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class GoogleSearchConsole(Customizer):

    credential_name = 'OAuthCredential'
    secrets_name = 'GoogleAnalytics'

    rename_map = {
        'global': {}
    }

    def __get_rename_map(self, property_url: str):
        return get_configured_item_by_key(key=property_url, lookup=self.rename_map)

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
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        self.get_secrets(include_dat=True)
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('date_col', DATE_COL)

    table = None
    table_schema = None
    date_col = 'report_date'

    def ingest_by_property_url(self, property_url: str, df: pd.DataFrame, report_date: datetime.datetime) -> None:
        table_schema = self.get_attribute('table_schema')
        table = self.get_attribute('table')
        date_col = self.get_attribute('date_col')
        with self.engine.begin() as con:
            con.execute(
                sqlalchemy.text(
                    f"""
                        DELETE FROM
                        {table_schema}.{table}
                        WHERE {date_col} = :report_date
                        AND property_url = :property_url;
                        """
                ),
                report_date=datetime.datetime.strftime(report_date, '%Y-%m-%d'),
                property_url=property_url
            )
            df.to_sql(
                table,
                con=con,
                if_exists='append',
                index=False,
                index_label=None
            )

    @staticmethod
    def get_date_range(start_date: datetime.datetime, end_date: datetime.datetime) -> list:
        return pd.date_range(start=start_date, end=end_date).to_list()

    def calculate_date(self) -> datetime.datetime:
        if self.get_attribute('historical'):
            historical_date = datetime.datetime.strptime(self.get_attribute('historical_report_date'), '%Y-%m-%d')
            last_day_of_month = calendar.monthrange(historical_date.year, historical_date.month)
            report_date = datetime.date(historical_date.year, historical_date.month, last_day_of_month[1])
            return report_date

        else:
            today = datetime.date.today()

            if today.day in [2, 5, 15]:
                if today.month == 1:
                    last_month = 12
                    last_month_year = (today.year - 1)
                else:
                    last_month = (today.month - 1)
                    last_month_year = today.year

                report_date = datetime.date(
                    last_month_year,
                    last_month,
                    calendar.monthrange(last_month_year, last_month)[1])

                return report_date
    
    def get_property_urls(self) -> list:
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT
                    property_url
                FROM public.source_gsc_propertymaster;
                """
            )
            results = con.execute(sql).fetchall()
            return [
                dict(result) for result in results
            ] if results else []

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        ====================================================================================================
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

    def pull(self):
        """
        Extracts data from Agency Google Search Console PGSQL Table, transforms and loads it into the SQL database
        ====================================================================================================
        :return:
        """

        report_date = self.calculate_date()
        # initialize the client module for connecting to GSC

        gsc_client = SearchAnalyticsClient()

        # get all property_urls that are configured
        property_urls = self.get_property_urls()
        assert property_urls, "No " + self.__class__.__name__ + " property_urls setup!"
        # iterate over each date to pull, process and ingest to prevent sampling

        for property_url in property_urls:
            property_url = property_url['property_url']
            rename_map = self.__get_rename_map(property_url=property_url)

            df = gsc_client.get_monthly_search_analytics(
                report_date=report_date,
                property_url=property_url
            )

            if df.shape[0]:
                df.rename(columns=rename_map, inplace=True)
                df = self.type(df=df)
                df['property_url'] = property_url
                df['data_source'] = self.get_attribute('data_source')
                self.ingest_by_property_url(property_url=property_url, df=df, report_date=report_date)
            else:
                print(f'WARN: No data returned for {report_date} for property_url{property_url}.')

    def backfilter(self):
        self.backfilter_statement()
        print('SUCCESS: Table Backfiltered.')

    def ingest(self):
        self.ingest_statement()
        print('SUCCESS: Table Ingested.')
