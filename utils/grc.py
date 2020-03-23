"""
Platform
"""
import os
import pandas as pd
import datetime

from utils.dbms_helpers import postgres_helpers
import sqlalchemy
from utils import custom, stdlib
from utils.gs_manager import GoogleSheetsManager
from utils.dbms_helpers.postgres_helpers import build_postgresql_engine


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


def clear_lookup_table_data(customizer):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.clear_postgresql_lookup_table(
            customizer=customizer)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def insert_data(customizer, df):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.insert_postgresql_data(
            customizer=customizer, df=df)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def insert_lookup_data(customizer, df):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.insert_postgresql_lookup_data(
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


def build_lookup_tables(customizer) -> int:
    if customizer.lookup_tables['status']:
        lookup_table_existence = check_table_exists(customizer, getattr(customizer, f'{customizer.prefix}_{customizer.lookup_tables["status"]["schema"]}'))

        if not lookup_table_existence:
            print(f'{customizer.lookup_tables["status"]["schema"]} does not exist, creating...')
            create_table_from_schema(customizer, getattr(customizer, f'{customizer.prefix}_{customizer.lookup_tables["status"]["schema"]}'))

    return 0


def reshape_lookup_data(customizer, df):

    df.columns = map(str.lower, df.columns)
    df.columns = [col.replace(' ', '_') for col in df.columns]

    if hasattr(customizer, f'{customizer.prefix}_drop_columns'):
        if getattr(customizer, f'{customizer.prefix}_drop_columns')['status']:
            df.drop(columns=getattr(customizer, f'{customizer.prefix}_drop_columns')['columns'], inplace=True)

    for column in getattr(customizer, f'{customizer.prefix}_{customizer.lookup_tables["status"]["schema"]}')['columns']:
        if column['name'] in df.columns:
            if column['type'] == 'character varying':
                assert 'length' in column.keys()
                df[column['name']] = df[column['name']].apply(lambda x: str(x)[:column['length']] if x else None)
            elif column['type'] == 'bigint':
                df[column['name']] = df[column['name']].apply(lambda x: int(x) if x == 0 or x == 1 else None)
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


def refresh_lookup_tables(workbook: str, worksheet: str, customizer) -> int:
    raw_location_data = GoogleSheetsManager(customizer.client).get_spreadsheet_by_name(workbook_name=workbook, worksheet_name=worksheet)

    clear_lookup_table_data(customizer=customizer)
    df = reshape_lookup_data(df=raw_location_data, customizer=customizer)
    insert_lookup_data(customizer, df=df)

    return 0


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

    # Check if required lookup tables exist, create if not and do nothing if existing
    build_lookup_tables(customizer=customizer)

    # Lookup table refresh status, will be switched to True after first related script run then will not run for others

    if not customizer.lookup_tables['status']['refresh_status']:
        print(f'{customizer.lookup_tables["status"]["table_name"]} not refreshed, refreshing now...')
        refresh_lookup_tables(workbook=customizer.CONFIGURATION_WORKBOOK['config_sheet_name'],
                              worksheet=customizer.lookup_tables['status']['lookup_source_sheet'], customizer=customizer)

        # After lookup table is refreshed, set status of attribute to True
        customizer.lookup_tables['status']['refresh_status'] = True

        if customizer.lookup_tables['status']['refresh_status']:
            print("SUCCESS: Table Refreshed.")

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


def table_backfilter(customizer: custom.Customizer):
    if getattr(customizer, f'{customizer.prefix}_backfilter_procedure'):
        engine = build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            for block in getattr(customizer, f'{customizer.prefix}_backfilter_procedure')['code']:
                sql = sqlalchemy.text(block)
                con.execute(sql)

        print('SUCCESS: Table Backfiltered.')

# TODO (Ben) - Finish config of google sheet refresh
def refresh_google_sheet_table(customizer: custom.Customizer):
    today = datetime.date.today()

    if today.day in [1, 15]:
        for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
            df = pd.Dataframe(GoogleSheetsManager(customizer.CLIENT_NAME).get_spreadsheet_by_name(workbook_name=customizer.CONFIGURATION_WORKBOOK['config_sheet_name'],
                                                                                worksheet_name=sheet['sheet']))

            engine = build_postgresql_engine(customizer=customizer)
            with engine.connect() as con:
                df.to_sql(
                    sheet['table'],
                    con=con,
                    if_exists='append',
                    index=False,
                    index_label=False
                )

