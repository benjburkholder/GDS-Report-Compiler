import pandas as pd
import sqlalchemy
import datetime

from utils import grc
from utils.dbms_helpers import postgres_helpers
from utils.gs_manager import GoogleSheetsManager
from utils.cls.core import Customizer, get_configured_item_by_key

TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class Inquiry(Customizer):

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
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('date_col', DATE_COL)

    @staticmethod
    def create_gs_object():
        gs = grc.get_customizer_secrets(GoogleSheetsManager(), include_dat=False)

        return gs

    def ingest_all(self, df: pd.DataFrame) -> None:
        table_schema = self.get_attribute('table_schema')
        table = self.get_attribute('table')

        with self.engine.begin() as con:
            con.execute(
                sqlalchemy.text(
                    f"""
                    DELETE FROM
                    {table_schema}.{table};
                    """
                ),
            )

            df.to_sql(
                table,
                con=con,
                if_exists='append',
                index=False,
                index_label=None
            )

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

    @staticmethod
    def calculate_inquiry_web_goals(raw_web_goals):
        data = []
        for row in raw_web_goals.iterrows():
            start_date = datetime.datetime.strptime(row[1]['Date Start'], '%Y-%m-%d')
            end_date = datetime.datetime.strptime(row[1]['Date End'], '%Y-%m-%d')
            mapped_property = row[1]['Property']
            mapped_community = row[1]['Community']
            ownership_group = row[1]['Ownership Group']
            region = row[1]['Region']
            inquiry_goal = float(row[1]['Inquiry Goal'].replace('$', '').replace(',', ''))

            if end_date is None:
                end_date = ''

            if end_date == '':
                end_date = datetime.date.today()

            # historical, iterate over a range of dates
            for iter_date in pd.date_range(start_date, end_date):
                start = datetime.date(start_date.year, start_date.month, start_date.day)
                end = datetime.date(end_date.year, end_date.month, end_date.day)
                max_days = (end - start).days

                daily_cost = (inquiry_goal / max_days)
                data.append({
                    'Date': iter_date,
                    'Property': mapped_property,
                    'Community': mapped_community,
                    'Ownership_Group': ownership_group,
                    'Region': region,
                    'Daily_Cost': daily_cost
                })
        return pd.DataFrame(data)

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

