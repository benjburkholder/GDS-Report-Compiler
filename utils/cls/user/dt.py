import pandas as pd
import sqlalchemy
import datetime
import pathlib
import os

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class DialogTech(Customizer):

    def __init__(self):
        super().__init__()
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

        # TODO: is there a way to optimize this?
        drop_columns = {
            'status': False,
            'columns': ['zip', 'phone']
        }
        self.set_attribute('drop_columns', drop_columns)

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
                result for result in results
            ] if results else []


class DialogtechCallDetail(DialogTech):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', datetime.date(2020, 1, 1))
        self.set_attribute('historical_end_date', datetime.date(2020, 2, 1))
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', 'DialogTech - Call Details')
        self.set_attribute('schema', {'columns': []})

    # noinspection PyMethodMayBeStatic
    def getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        # TODO: with a new version of GA that accepts function pointers
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        return df.rename(columns={
            'call_date': 'report_date',

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

        df = df[[
            'report_date',
            'data_source',
            'property',
            'campaign',
            'medium',
            'number_dialed',  # call tracking number
            'caller_id',
            'call_duration',
            'transfer_to_number',  # terminating number
            'phone_label',
            'call_transfer_status',
            'client_id'
        ]]

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
