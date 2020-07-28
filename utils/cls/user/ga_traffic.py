"""
Google Analytics Traffic Module
"""
# STANDARD IMPORTS
import pandas as pd

# PLATFORM IMPORTS
from utils.cls.user.ga import GoogleAnalytics
from utils.dbms_helpers import postgres_helpers

# CUSTOM IMPORTS
from googleanalyticspy.reporting.client.reporting import GoogleAnalytics as GoogleAnalyticsClient
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-06-01'
HISTORICAL_END_DATE = '2020-06-30'


class GoogleAnalyticsTrafficCustomizer(GoogleAnalytics):
    """
    Handles Google Analytisc Traffic pulling, parsing and processing
    """

    metrics = [
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
    ]

    dimensions = [
        'date',
        'channelGrouping',
        'sourceMedium',
        'deviceCategory',
        'campaign',
        'pagePath',
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)
        self.set_attribute('data_source', 'Google Analytics - Traffic')
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

        return df

    def _get_post_processing_sql_list(self) -> list:
        return []

    def post_processing(self) -> None:
        """
        Handles custom sql UPDATE / JOIN post-processing needs for reporting tables,
        :return:
        """
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            for query in self._get_post_processing_sql_list():
                con.execute(query)
        return

    def pull(self):
        if self.get_attribute('historical'):
            start_date = self.get_attribute('historical_start_date')
            end_date = self.get_attribute('historical_end_date')
        else:
            start_date = self.calculate_date(start_date=True)
            end_date = self.calculate_date(start_date=False)
        # initialize the client module for connecting to GA
        ga_client = GoogleAnalyticsClient(
            customizer=self
        )
        # get all view that are configured
        views = self.get_views()
        assert views, "No " + self.__class__.__name__ + " views setup!"

        # for each, pull according to the dates, metrics and dimensions configured
        for view in views:
            view_id = view['view_id']
            prop = view['property']
            df = ga_client.query(
                view_id=view_id,
                raw_dimensions=self.dimensions,
                raw_metrics=self.metrics,
                start_date=start_date,
                end_date=end_date
            )
            # if we have valid secrets after the request, let's update the db with the latest
            # we put the onus on the client library to refresh these credentials as needed
            # and to store them where they belong
            if getattr(self, 'secrets_dat', {}):
                self.set_customizer_secrets_dat()

            if df.shape[0]:
                df = self.rename(df=df)
                df = self.type(df=df)
                df['view_id'] = view_id
                df['property'] = prop
                df['data_source'] = self.get_attribute('data_source')
                self.ingest_by_view_id(view_id=view_id, df=df, start_date=start_date, end_date=end_date)
            else:
                print(f'WARN: No data returned for view {view_id} for property {prop}')








