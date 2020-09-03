import pandas as pd
import sqlalchemy
import datetime

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

# LMPY PACKAGES
from googleadspy.reporting.client.reporting import GoogleAdsReporting

TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class GoogleAds(Customizer):

    credential_name = 'GoogleAdsYaml'
    secrets_name = ''

    rename_map = {
        'global': {

        }
    }

    def get_rename_map(self, account_id: str):
        return get_configured_item_by_key(key=account_id, lookup=self.rename_map)

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

    def build_client(self, manager_customer_id: str) -> GoogleAdsReporting:
        """
        Datasource-specific method to construct the client using preconfigured secrets
        ====================================================================================================
        :return:
        """
        self.get_secrets(include_dat=False)
        return GoogleAdsReporting(
            manager_customer_id=manager_customer_id,
            client_id=self.secrets['client_id'],
            client_secret=self.secrets['client_secret'],
            developer_token=self.secrets['developer_token'],
            refresh_token=self.secrets['refresh_token'],
            user_agent=self.secrets['user_agent']
        )

    def __init__(self):
        super().__init__()
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('date_col', DATE_COL)

    def ingest_by_account_id(self, df: pd.DataFrame, account_id: str, start_date: str, end_date: str) -> None:
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
                    AND account_id = :account_id;
                    """
                ),
                start_date=start_date,
                end_date=end_date,
                account_id=account_id
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

    def calculate_date(self, start_date: bool = True) -> str:
        if self.get_attribute('historical'):
            if start_date:
                return self.get_attribute('historical_start_date')
            else:
                return self.get_attribute('historical_end_date')
        else:
            if start_date:
                return (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
            else:
                return (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    def get_account_ids(self) -> list:
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT
                    manager_account_id,
                    account_id
                FROM public.source_gads_accountmaster;
                """
            )
            results = con.execute(sql).fetchall()
            return [
                dict(result) for result in results
            ] if results else []

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
