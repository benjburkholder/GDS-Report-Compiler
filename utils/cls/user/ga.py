import pandas as pd
import sqlalchemy
import pathlib
import datetime
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer
TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class GoogleAnalytics(Customizer):

    credential_name = 'OAuthCredential'
    secrets_name = 'GoogleAnalytics'

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
        self.get_secrets(include_dat=True)
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('date_col', DATE_COL)

    table = None
    table_schema = None
    date_col = 'report_date'

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

    def calculate_date(self, start_date: bool = True):
        if start_date:
            return (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
        else:
            return (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

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
        self.set_attribute('data_source', 'Google Analytics - Events')
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

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
        self.set_attribute('data_source', 'Google Analytics - Goals')
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

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
