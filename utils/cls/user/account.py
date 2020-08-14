import pandas as pd
import sqlalchemy
import calendar
import datetime

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer

TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'


class AccountCost(Customizer):

    rename_map = {
        'global': {

        }
    }

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
                )
            )

            df.to_sql(
                table,
                con=con,
                if_exists='append',
                index=False,
                index_label=None
            )

    def pull_account_cost(self):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT *
                FROM public.source_account_cost;
                """
            )
            results = con.execute(sql).fetchall()

            return [
                result for result in results
            ] if results else []

    def get_account_cost_meta_data(self, cost_data):
        data = []
        for row in cost_data:
            start_date = row[0]
            mapped_location = row[2]
            end_date = row[1]
            medium = row[5]
            total_cost = float(row[4].replace('$', '').replace(',', ''))

            if end_date is None:
                end_date = ''

            if end_date == '':
                end_date = datetime.date.today().strftime('%Y-%m-%d')
            # historical, iterate over a range of dates
            for iter_date in pd.date_range(start_date, end_date):
                month = iter_date.month
                year = iter_date.year
                max_days = calendar.monthrange(year=year, month=month)[1]
                daily_cost = (total_cost / max_days)
                data.append({
                    'Date': iter_date,
                    'Property': mapped_location,
                    'Medium': medium,
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
        pass

    def ingest(self):
        pass
