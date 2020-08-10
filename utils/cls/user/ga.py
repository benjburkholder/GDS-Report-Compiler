import pandas as pd
import sqlalchemy
import datetime
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

from googleanalyticspy.reporting.client.reporting import GoogleAnalytics as GoogleAnalyticsClient
TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class GoogleAnalytics(Customizer):

    credential_name = 'OAuthCredential'
    secrets_name = 'GoogleAnalytics'

    metrics = {
        'global': []
    }

    def __get_metrics(self, view_id: str):
        return get_configured_item_by_key(key=view_id, lookup=self.metrics)

    dimensions = {
        'global': []
    }

    def __get_dimensions(self, view_id: str):
        return get_configured_item_by_key(key=view_id, lookup=self.dimensions)

    rename_map = {
        'global': {}
    }

    def __get_rename_map(self, view_id: str):
        return get_configured_item_by_key(key=view_id, lookup=self.rename_map)

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

        # if we have valid secrets after the request loop, let's update the db with the latest
        # we put the onus on the client library to refresh these credentials as needed
        # and to store them where they belong
        if getattr(self, 'secrets_dat', {}):
            self.set_customizer_secrets_dat()

    def ingest_by_view_id(self, view_id: str, df: pd.DataFrame, start_date: str, end_date: str) -> None:
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
                    AND view_id = :view_id;
                    """
                ),
                start_date=start_date,
                end_date=end_date,
                view_id=view_id
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

    def calculate_date(self, start_date: bool = True) -> datetime.datetime:
        if self.get_attribute('historical'):
            if start_date:
                return datetime.datetime.strptime(self.get_attribute('historical_start_date'), '%Y-%m-%d')
            else:
                return datetime.datetime.strptime(self.get_attribute('historical_end_date'), '%Y-%m-%d')
        else:
            if start_date:
                return datetime.datetime.today() - datetime.timedelta(7)
            else:
                return datetime.datetime.today() - datetime.timedelta(1)

    def get_views(self) -> list:
        sql = sqlalchemy.text(
            """
            SELECT DISTINCT
                view_id,
                property
            FROM public.source_ga_views;
            """
        )
        with self.engine.connect() as con:
            results = con.execute(
                sql
            ).fetchall()
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
        Extracts data from Google Analytics API, transforms and loads it into the SQL database
        ====================================================================================================
        :return:
        """
        start_date = self.calculate_date(start_date=True)
        end_date = self.calculate_date(start_date=False)
        # initialize the client module for connecting to GA
        ga_client = GoogleAnalyticsClient(
            customizer=self
        )
        # get all view that are configured
        views = self.get_views()
        assert views, "No " + self.__class__.__name__ + " views setup!"
        # iterate over each date to pull, process and ingest to prevent sampling
        date_idx = 0
        date_range = self.get_date_range(start_date=start_date, end_date=end_date)
        for _ in date_range:
            if date_idx != 0:
                start = date_range[date_idx - 1].strftime('%Y-%m-%d')
                end = date_range[date_idx].strftime('%Y-%m-%d')
                # for each, pull according to the dates, metrics and dimensions configured
                for view in views:
                    view_id = view['view_id']
                    prop = view['property']
                    dimensions = self.__get_dimensions(view_id=view_id)
                    metrics = self.__get_metrics(view_id=view_id)
                    rename_map = self.__get_rename_map(view_id=view_id)
                    assert dimensions and metrics, \
                        "Dimensions and metrics not properly configured for " + self.__class__.__name__
                    df = ga_client.query(
                        view_id=view_id,
                        raw_dimensions=dimensions,
                        raw_metrics=metrics,
                        start_date=start,
                        end_date=end
                    )

                    if df.shape[0]:
                        df.rename(columns=rename_map, inplace=True)
                        df = self.type(df=df)
                        df['view_id'] = view_id
                        df['property'] = prop
                        df['data_source'] = self.get_attribute('data_source')
                        self.ingest_by_view_id(view_id=view_id, df=df, start_date=start, end_date=end)
                    else:
                        print(f'WARN: No data returned for {start} for view {view_id} for property {prop}')
            # always increments the date_range idx
            date_idx += 1

    def backfilter(self):
        pass

    def ingest(self):
        pass


