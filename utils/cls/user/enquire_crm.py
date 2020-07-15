import pandas as pd

from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer


class EnquireCRM(Customizer):
    pass


class EnquireCrmActivityDeposit(EnquireCRM):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

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

        return df

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """

        # noinspection PyUnresolvedReferences

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

        return df

    def post_processing(self):
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


class EnquireCrmActivityInquiry(EnquireCRM):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
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
        return df

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """

        # noinspection PyUnresolvedReferences

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

        return df

    def post_processing(self):
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


class EnquireCrmActivityMoveIn(EnquireCRM):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
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
        return df

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """

        # noinspection PyUnresolvedReferences
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

        return df

    def post_processing(self):
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


class EnquireCrmActivityTour(EnquireCRM):

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True),
        self.set_attribute('debug', True),
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
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
        return df

    # noinspection PyMethodMayBeStatic
    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """

        # noinspection PyUnresolvedReferences

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

        return df

    def post_processing(self):
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
