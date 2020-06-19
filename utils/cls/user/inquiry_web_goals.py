import pandas as pd
import pathlib
import os

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer


class InquiryGoals(Customizer):

    def __init__(self):
        super().__init__()
        self.set_attribute('secrets_path', str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('table', self.prefix)
        self.set_attribute('class', True)
        self.set_attribute('data_source', 'Goals - Web Inquiries')

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
            'Property': 'property',
            'Community': 'community',
            'Ownership Group': 'ownership_group',
            'Region': 'region',
            'Date': 'report_date',
            'Inquiry Goal': 'inquiry_goal'
        })

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        # noinspection PyUnresolvedReferences
        df['property'] = df['property'].astype(str).str[:100]
        df['community'] = df['community'].astype(str).str[:100]
        df['ownership_group'] = df['ownership_group'].astype(str).str[:100]
        df['region'] = df['region'].astype(str).str[:100]
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['inquiry_goal'] = df['inquiry_goal'].fillna('0').apply(lambda x: float(x) if x else None)

        # TODO: Later optimization... keeping the schema for the table in the customizer
        #   - and use it to reference typing command to df
        '''
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
        '''
        return df

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:

        # float dtypes
        df['daily_cost'] = df['daily_cost'].astype(float)
        df['daily_cost'] = df['daily_cost'].apply(lambda x: round(x, 2))

        return df

    def post_processing(self):
        """
        Handles custom sql UPDATE / JOIN post-processing needs for reporting tables,
        :return:
        """

        # CUSTOM SQL QUERIES HERE, ADD AS MANY AS NEEDED
        sql = """
            UPDATE public.enquire_map_histories
            SET
                data_source = 'Enquire MAP - Histories';
            """

        sql2 = """ CUSTOM SQL HERE """

        custom_sql = [
            sql,
        ]

        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            for query in custom_sql:
                con.execute(query)

        return
