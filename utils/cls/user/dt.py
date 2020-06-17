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

    # Area for adding key / value pairs for columns which vary client to client
    # These columns are built out in the creation of the table, this simply assigns the proper default values to them
    custom_columns = [
        {'data_source': 'DialogTech - Call Details'},
        {'property': None},
        # {'service_line': None}
    ]

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', datetime.date(2020, 1, 1))
        self.set_attribute('historical_end_date', datetime.date(2020, 2, 1))
        self.set_attribute('table', self.prefix)

        # Used to set columns which vary from data source and client vertical
        self.set_attribute('custom_columns', self.custom_columns)

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
        # noinspection PyUnresolvedReferences
        df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        df['campaign'] = df['campaign'].astype(str).str[:150]
        df['medium'] = df['medium'].astype(str).str[:150]
        df['number_dialed'] = df['number_dialed'].astype(str).str[:25]
        df['caller_id'] = df['caller_id'].astype(str).str[:25]
        df['call_duration'] = df['call_duration'].fillna('0').apply(lambda x: float(x) if x else None)
        df['transfer_to_number'] = df['transfer_to_number'].astype(str).str[:25]
        df['phone_label'] = df['phone_label'].astype(str).str[:150]
        df['call_transfer_status'] = df['call_transfer_status'].astype(str).str[:100]
        df['client_id'] = df['client_id'].astype(str).str[:150]


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
        if getattr(self, f'{self.prefix}_custom_columns'):
            for row in getattr(self, f'{self.prefix}_custom_columns'):
                for key, value in row.items():
                    df[key] = value

        return df

    def post_processing(self, df):
        """
        Execute UPDATE... JOIN statements against the source table of the calling class
        :return:
        """
        # build engine
        # execute statements

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
