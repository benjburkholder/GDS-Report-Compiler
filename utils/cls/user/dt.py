import pandas as pd
import sqlalchemy
import datetime

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer, get_configured_item_by_key

TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class Dialogtech(Customizer):

    rename_map = {
        'global': {

        }
    }

    def get_rename_map(self, phone_label: str):
        return get_configured_item_by_key(key=phone_label, lookup=self.rename_map)

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

    def ingest_by_phone_label(self, df: pd.DataFrame, phone_label: str, start_date: datetime.datetime, end_date: datetime.datetime) -> None:
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
                    AND phone_label = :phone_label;
                    """
                ),
                start_date=start_date,
                end_date=end_date,
                phone_label=phone_label
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

    def pull_dialogtech_labels(self):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT DISTINCT *
                FROM public.lookup_dt_mapping;
                """
            )
            results = con.execute(sql).fetchall()

            return [
                {
                    'phone_label': result[0],
                    'medium': result[1],
                    'property': result[2],
                    'exact': result[3]
                }
                for result in results
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

