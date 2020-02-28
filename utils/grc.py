"""
Platform
"""
import os

from utils.dbms_helpers import postgres_helpers
from utils import stdlib


def get_script_name(file):
    """
    Returns the trimmed name of the script which acts as the key for the Customizer class interface
    :param file:
    :return:
    """
    return os.path.basename(file).replace('.py', '')


def get_required_attribute(cls, attribute):
    """
    Function for getting attributes from Customizer classes which may (or may not) be setup correctly
    :param cls:
    :param attribute:
    :return:
    """
    if hasattr(cls, f'{cls.prefix}_{attribute}'):
        return getattr(cls, f'{cls.prefix}_{attribute}')
    raise AssertionError(f"{cls.__class__.__name__} does not have an attribute {attribute}.")


def get_optional_attribute(cls, attribute):
    """
    Safe function for getting attributes from Customizer classes which may (or may not) be setup correctly
    :param cls:
    :param attribute:
    :return:
    """
    if hasattr(cls, f'{cls.prefix}_{attribute}'):
        return getattr(cls, f'{cls.prefix}_{attribute}')
    else:
        return None


def create_sql_engine(customizer):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.build_postgresql_engine(customizer=customizer)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def clear_non_golden_data(customizer, date_col, min_date, max_date):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.clear_postgresql_non_golden_data(
            customizer=customizer, date_col=date_col, min_date=min_date, max_date=max_date)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def insert_data(customizer, df):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.insert_postgresql_data(
            customizer=customizer, df=df)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def run_data_ingest_rolling_dates(df, customizer, date_col='report_date') -> int:
    min_date = df[date_col].min()
    max_date = df[date_col].max()
    # clear non-golden data
    clear_non_golden_data(customizer=customizer, date_col=date_col, min_date=min_date, max_date=max_date)
    # insert fresh data
    insert_data(customizer=customizer, df=df)
    return stdlib.EXIT_SUCCESS


def check_table_exists(customizer, schema) -> bool:
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    schema_name = schema['schema']
    table_name = schema['table']
    if customizer.dbms == 'postgresql':
        return postgres_helpers.check_postgresql_table_exists(
            customizer=customizer,
            schema=schema_name,
            table=table_name
        )
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def create_table_from_schema(customizer, schema) -> int:
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.create_table_from_schema(
            customizer=customizer,
            schema=schema
        )
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")

