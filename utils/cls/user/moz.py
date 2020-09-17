import pandas as pd
import sqlalchemy
import datetime

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class Moz(Customizer):

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

    # formatted so method can be utilized by both moz pro and moz local
    def ingest_by_custom_indicator(self, df: pd.DataFrame, id_value: str, report_date=None) -> None:
        table_schema = self.get_attribute('table_schema')
        table = self.get_attribute('table')
        date_col = self.get_attribute('date_col')

        # uses report_date presence to determine if calling data source is moz pro or moz local
        with self.engine.begin() as con:
            if report_date:
                con.execute(
                    sqlalchemy.text(
                        f"""
                        DELETE FROM
                        {table_schema}.{table}
                        WHERE {date_col} = :report_date
                        AND campaign_id = :id_value;
                        """
                    ),
                    report_date=report_date,
                    id_value=id_value
                )

            else:
                con.execute(
                    sqlalchemy.text(
                        f"""
                        DELETE FROM
                        {table_schema}.{table}
                        WHERE listing_id = :id_value;
                        """
                    ),
                    id_value=id_value
                )

            df.to_sql(
                table,
                con=con,
                if_exists='append',
                index=False,
                index_label=None
            )

    def get_date_range(self) -> datetime:
        if self.get_attribute('historical'):
            start = datetime.datetime.strptime(self.get_attribute('historical_start_date'), '%Y-%m-%d')
            end = datetime.datetime.strptime(self.get_attribute('historical_end_date'), '%Y-%m-%d')

            date_range = pd.date_range(start=start, end=end, freq='M')

            return date_range

        else:
            # automated setup - last month by default
            today = datetime.date.today()

            if today.day in [2, 3, 7, 15]:
                report_date = datetime.date(today.year, today.month, 1)

                return report_date

            else:
                print('Nothing to run, not the proper day')

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

