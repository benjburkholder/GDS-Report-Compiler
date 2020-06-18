"""
Platform
"""
import pandas as pd
import datetime
import os

from utils.dbms_helpers.postgres_helpers import build_postgresql_engine
from utils.gs_manager import GoogleSheetsManager
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
    raise AssertionError(f"{cls.__class__.__name__} does not have an attribute {cls.prefix}_{attribute}.")


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


def clear_non_golden_data(customizer, date_col, min_date, max_date, table):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.clear_postgresql_non_golden_data(
            customizer=customizer, date_col=date_col, min_date=min_date, max_date=max_date, table=table)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def clear_lookup_table_data(customizer, sheet):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.clear_postgresql_other_table(
            customizer=customizer, sheet=sheet)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def clear_source_table_data(customizer, sheet):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.clear_postgresql_other_table(
            customizer=customizer, sheet=sheet)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def insert_data(customizer, df, table):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.insert_postgresql_data(
            customizer=customizer, df=df, table=table)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def insert_other_data(customizer, df, sheet):
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    if customizer.dbms == 'postgresql':
        return postgres_helpers.insert_postgresql_other_data(
            customizer=customizer, df=df, sheet=sheet)
    else:
        raise ValueError(f"{customizer.__class__.__name__} specifies unsupported 'dbms' {customizer.dbms}")


def run_data_ingest_rolling_dates(df, customizer, table, date_col='report_date') -> int:
    min_date = df[date_col].min()
    max_date = df[date_col].max()

    # clear non-golden data
    clear_non_golden_data(customizer=customizer, date_col=date_col, min_date=min_date, max_date=max_date, table=table)
    # insert fresh data
    insert_data(customizer=customizer, df=df, table=table)

    return stdlib.EXIT_SUCCESS


def build_lookup_tables(customizer) -> int:
    for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
        if sheet['table']['type'] == 'lookup':
            if sheet['table']['active']:
                lookup_table_existence = check_table_exists(customizer, schema=sheet)

                if not lookup_table_existence:
                    print(f'{sheet["table"]["name"]} does not exist, creating...')
                    create_table_from_schema(customizer, sheet)

    return 0


def build_reporting_tables(customizer) -> int:
    for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
        if sheet['table']['type'] == 'reporting':
            if sheet['table']['active']:
                print(f'Checking if {sheet["table"]["name"]} exists...')
                reporting_table_existence = check_table_exists(customizer, schema=sheet)

                if not reporting_table_existence:
                    print(f'{sheet["table"]["name"]} does not exist, creating...')
                    create_table_from_schema(customizer, sheet)

    return 0


def build_source_tables(customizer) -> int:
    for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
        if sheet['table']['type'] == 'source':
            if sheet['table']['active']:
                print(f'Checking if {sheet["table"]["name"]} exists...')
                source_table_existence = check_table_exists(customizer, schema=sheet)

                if not source_table_existence:
                    print(f'{sheet["table"]["name"]} does not exist, creating...')
                    create_table_from_schema(customizer, sheet)

    return 0


def build_marketing_table(customizer) -> int:

    print(f'Checking if {customizer.marketing_data["table"]["name"]} exists...')

    marketing_table_existence = check_table_exists(customizer, schema=customizer.marketing_data)

    if not marketing_table_existence:
        print(f'{customizer.marketing_data["table"]["name"]} does not exist, creating...')

        for sheets in customizer.CONFIGURATION_WORKBOOK['sheets']:
            if sheets['table']['type'] == 'reporting':
                if sheets['table']['active']:
                    for column in sheets['table']['columns']:
                        if column['master_include']:
                            if not any(column['name'] == d['name'] for d in customizer.marketing_data['table']['columns']):
                                customizer.marketing_data['table']['columns'].append(column)

        # Add custom columns to end of marketing table
        if customizer.custom_map_columns['table']['active']:
            if customizer.custom_map_columns['table']['columns']:
                customizer.marketing_data['table']['columns'].append(customizer.custom_map_columns['table']['columns'])

        create_table_from_schema(customizer, schema=customizer.marketing_data)

    return 0


def reshape_lookup_data(customizer, df, sheet):

    df.columns = map(str.lower, df.columns)
    df.columns = [col.replace(' ', '_') for col in df.columns]

    if hasattr(customizer, f'{customizer.prefix}_drop_columns'):
        if getattr(customizer, f'{customizer.prefix}_drop_columns')['status']:
            df.drop(columns=getattr(customizer, f'{customizer.prefix}_drop_columns')['columns'], inplace=True)

    for column in sheet['table']['columns']:
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


def reshape_source_table_data(customizer, df, sheet):

    df.columns = map(str.lower, df.columns)
    df.columns = [col.replace(' ', '_') for col in df.columns]

    if hasattr(customizer, f'{customizer.prefix}_drop_columns'):
        if getattr(customizer, f'{customizer.prefix}_drop_columns')['status']:
            df.drop(columns=getattr(customizer, f'{customizer.prefix}_drop_columns')['columns'], inplace=True)

    for column in sheet['table']['columns']:
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


def refresh_lookup_tables(customizer) -> int:
    if customizer.CONFIGURATION_WORKBOOK['lookup_refresh_status'] is False:
        for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
            if sheet['table']['type'] == 'lookup':
                if sheet['table']['active']:
                    raw_lookup_data = GoogleSheetsManager(customizer.client).get_spreadsheet_by_name(workbook_name=customizer.CONFIGURATION_WORKBOOK['config_sheet_name'],
                                                                                               worksheet_name=sheet['sheet'])

                    clear_lookup_table_data(customizer=customizer, sheet=sheet)
                    df = reshape_lookup_data(df=raw_lookup_data, customizer=customizer, sheet=sheet)
                    insert_other_data(customizer, df=df, sheet=sheet)

                    print(f"SUCCESS: {sheet['table']['name']} Refreshed.")

    # Once one script refreshed lookup tables, set global status to True to bypass with following scripts
    customizer.CONFIGURATION_WORKBOOK['lookup_refresh_status'] = True

    print("SUCCESS: Lookup Tables Refreshed.")

    return 0


def check_table_exists(customizer, schema) -> bool:
    assert hasattr(customizer, 'dbms'), "Invalid global Customizer configuration, missing 'dbms' attribute"
    schema_name = schema['table']['schema']
    table_name = schema['table']['name']
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


def setup(script_name: str, required_attributes: list, refresh_indicator, expedited: bool = False):
    """
    Before allowing any root-level script to execute
    Get the Customizer instance configured for script_name
    Run the customizer against configuration_check, a script-defined function
    Return the Customizer instance (initialized) if successful
    Upon failure raise whatever error configuration_check deems useful and informative

    :param script_name:
    :param required_attributes:
    :param expedited: (bool) skip the lookup table refreshment - good for debugging
    :return:
    """
    customizer = custom.get_customizer(calling_file=script_name)
    assert customizer, f"{script_name} | No customizer returned. Please check your configuration"
    run_configuration_check(script_name=script_name, required_attributes=required_attributes, customizer=customizer)

    # check if command there are more than one command line argument
    if len(refresh_indicator) == 2:

        # 'Run' means it's the first script being run
        if 'run' in refresh_indicator:
            expedited = False

        # Else skip the table checks
        else:
            expedited = True

    if not expedited:

        # Build marketing data table
        build_marketing_table(customizer=customizer)

        # Build reporting tables
        build_reporting_tables(customizer=customizer)

        # Build listed lookup tables
        build_lookup_tables(customizer=customizer)

        # Build listed source tables
        build_source_tables(customizer=customizer)

        # Lookup table refresh
        print('Refreshing lookup tables...')
        refresh_lookup_tables(customizer=customizer)

        # Source table refresh
        print('Refreshing source tables...')
        refresh_source_tables(customizer=customizer)

    # Delete extra argument (necessary for GMB API)
    if len(refresh_indicator) == 2:
        del refresh_indicator[1]

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
            if stage == 'post_processing':
                get_optional_attribute(cls=customizer, attribute=stage)()
            else:
                df = get_optional_attribute(cls=customizer, attribute=stage)(df=df)
    return df


def table_backfilter(customizer: custom.Customizer):
    engine = build_postgresql_engine(customizer=customizer)
    target_sheets = [
        sheet for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']
        if sheet['table']['name'] == customizer.get_attribute('table')
    ]
    assert len(target_sheets) == 1
    sheet = target_sheets[0]
    assert sheet['table']['type'] == 'reporting'
    statements = customizer.build_backfilter_statements()
    with engine.connect() as con:
        for statement in statements:
            con.execute(statement)
    print('SUCCESS: Table Backfiltered.')


def ingest_procedures(customizer: custom.Customizer):
    engine = build_postgresql_engine(customizer=customizer)

    master_columns = []
    for sheets in customizer.CONFIGURATION_WORKBOOK['sheets']:
        if sheets['table']['type'] == 'reporting':
            if sheets['table']['active']:
                for column in sheets['table']['columns']:
                    if column['master_include']:
                        master_columns.append(column)
    target_sheets = [
        sheet for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']
        if sheet['table']['name'] == customizer.get_attribute('table')]
    ingest_procedure = customizer.create_ingest_statement(customizer, master_columns, target_sheets)
    with engine.connect() as con:
        for statement in ingest_procedure:
            con.execute(statement)
    print('SUCCESS: Table Ingested.')


def audit_automation(customizer: custom.Customizer):
    for sheets in customizer.CONFIGURATION_WORKBOOK['sheets']:
        if sheets['table']['type'] == 'reporting':
            if sheets['table']['audit_cadence']:
                if sheets['table']['name'] == customizer.get_attribute('table'):
                    audit_automation_indicator = [column['name'] for column in sheets['table']['columns'] if 'ingest_indicator' in column][0]
                    customizer.audit_automation_procedure(index_column=customizer.get_attribute(audit_automation_indicator), cadence=sheets['table']['audit_cadence'])


def refresh_source_tables(customizer: custom.Customizer):
    today = datetime.date.today()

    if today.day in customizer.CONFIGURATION_WORKBOOK['source_refresh_dates']:
        for sheet in customizer.CONFIGURATION_WORKBOOK['sheets']:
            if sheet['table']['type'] == 'source':
                if sheet['table']['active']:
                    raw_source_data = GoogleSheetsManager(customizer.client).get_spreadsheet_by_name(workbook_name=customizer.CONFIGURATION_WORKBOOK['config_sheet_name'], worksheet_name=sheet['sheet'])

                    clear_source_table_data(customizer=customizer, sheet=sheet)
                    df = reshape_source_table_data(customizer=customizer, df=raw_source_data, sheet=sheet)
                    insert_other_data(customizer, df=df, sheet=sheet)

                    print(f"SUCCESS: {sheet['table']['name']} Refreshed.")
    else:
        print('Not listed refresh day.')

    return 0



