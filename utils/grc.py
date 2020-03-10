"""
Platform
"""
import os
import pandas as pd

from utils.dbms_helpers import postgres_helpers
from utils import custom, stdlib


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

    table_existence = check_table_exists(customizer, getattr(customizer, f'{customizer.prefix}_schema'))

    if table_existence:
        # clear non-golden data
        clear_non_golden_data(customizer=customizer, date_col=date_col, min_date=min_date, max_date=max_date)
        # insert fresh data
        insert_data(customizer=customizer, df=df)
        return stdlib.EXIT_SUCCESS

    if not table_existence:
        create_table_from_schema(customizer, getattr(customizer, f'{customizer.prefix}_schema'))
        insert_data(customizer=customizer, df=df)
        return stdlib.EXIT_SUCCESS

    else:
        raise ValueError("Other Error: An error occurred regardless of the table's existence, "
                         "check custom.py schema attribute.")


def build_lookup_tables(customizer):

    if customizer.lookup_tables['ga']:
        url_lookup_table_existence = check_table_exists(customizer, getattr(customizer, f'{customizer.prefix}_lookup_urltolocation_schema'))

        if not url_lookup_table_existence:
            create_table_from_schema(customizer, getattr(customizer, f'{customizer.prefix}_lookup_urltolocation_schema'))

    if customizer.lookup_tables['moz']:
        moz_lookup_table_existence = check_table_exists(customizer, getattr(customizer, f'{customizer.prefix}_lookup_mozlocal_listingtolocation_schema'))

        if not moz_lookup_table_existence:
            create_table_from_schema(customizer, getattr(customizer, f'{customizer.prefix}_lookup_mozlocal_listingtolocation_schema'))

    if customizer.lookup_tables['gmb']:
        gmb_lookup_table_existence = check_table_exists(customizer, getattr(customizer, f'{customizer.prefix}_lookup_gmb_listingtolocation_schema'))

        if not gmb_lookup_table_existence:
            create_table_from_schema(customizer, getattr(customizer, f'{customizer.prefix}_lookup_gmb_listingtolocation_schema'))


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
        return postgres_helpers.create_postgresql_table_from_schema(
            customizer=customizer,
            schema=schema
        )
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def setup(script_name: str, required_attributes: list):
    """
    Before allowing any root-level script to execute
    Get the Customizer instance configured for script_name
    Run the customizer against configuration_check, a script-defined function
    Return the Customizer instance (initialized) if successful
    Upon failure raise whatever error configuration_check deems useful and informative

    :param script_name:
    :param required_attributes:
    :return:
    """
    customizer = custom.get_customizer(calling_file=script_name)
    assert customizer, f"{script_name} | No customizer returned. Please check your configuration"
    run_configuration_check(script_name=script_name, required_attributes=required_attributes, customizer=customizer)

    return customizer

def run_configuration_check(script_name: str, required_attributes: list, customizer: custom.Customizer):
    """
    In order to run the script, the configured Customier must be valid

    :param script_name:
    :param required_attributes:
    :param customizer:
    :return:
    """
    try:
        for attribute in required_attributes:
            get_required_attribute(cls=customizer, attribute=attribute)
    except AssertionError:
        print(f"{script_name} | run_configuration_check failed")
        raise


def run_prestart_assertion(script_name: str, attribute: list, label: str):
    assert attribute, f"{script_name} | Global error, {label} either not defined or empty"


def run_processing(df: pd.DataFrame, customizer: custom.Customizer, processing_stages: list):
    for stage in processing_stages:
        print(f'Checking for processing stage {stage}...')
        if get_optional_attribute(cls=customizer, attribute=stage):
            print(f'\tNow processing stage {stage}')
            df = get_optional_attribute(cls=customizer, attribute=stage)(df=df)
    return df
