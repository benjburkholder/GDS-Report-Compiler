"""
Platform Functions
"""
import platform
import datetime
import json
import os
import sys

import pandas as pd
import sqlalchemy

from utils import custom, stdlib
from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers
from utils.dbms_helpers.postgres_helpers import build_postgresql_engine
from utils.gs_manager import GoogleSheetsManager

APPLICATION_DATABASE = 'applications'


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


def create_application_sql_engine():
    """
    Use the static APPLICATION_DATABASE to alter existing Customizer setup to support interacting
    with the applications database
    ====================================================================================================
    :return:
    """
    return create_sql_engine(
        customizer=Customizer(database=APPLICATION_DATABASE)
    )


def get_customizer_secrets(customizer: Customizer, include_dat: bool = True) -> Customizer:
    customizer = __get_customizer_secrets(customizer=customizer)
    if include_dat:
        customizer = __get_customizer_secrets_dat(customizer=customizer)
    return customizer


def set_customizer_secrets_dat(customizer: Customizer) -> None:
    client_name = customizer.client  # camel-case client name
    name_value = getattr(customizer, 'secrets_name', '')
    assert name_value, f"Invalid name_value {name_value} provided"
    content_value = getattr(customizer, 'secrets_dat', '')
    assert content_value, f"Invalid content_value {content_value} provided"
    content_value = json.dumps(content_value) if type(content_value) == dict else content_value
    with create_application_sql_engine().connect() as con:
        count_result = con.execute(
            sqlalchemy.text(
                """
                SELECT COUNT(*) as count_value
                FROM public.gds_compiler_credentials_dat
                WHERE client_name = :client_name
                AND name_value = :name_value;
                """
            ),
            client_name=client_name,
            name_value=name_value
        ).first()
        if count_result['count_value'] == 0:
            con.execute(
                sqlalchemy.text(
                    """
                    INSERT INTO public.gds_compiler_credentials_dat
                    (
                        client_name,
                        name_value,
                        content_value
                    )
                    VALUES
                    (
                        :client_name, 
                        :name_value,
                        :content_value
                    );
                    """
                ),
                client_name=client_name,
                name_value=name_value,
                content_value=content_value
            )
        else:
            con.execute(
                sqlalchemy.text(
                    """
                    UPDATE public.gds_compiler_credentials_dat
                    SET content_value = :content_value
                    WHERE client_name = :client_name
                    AND name_value = :name_value;
                    """
                ),
                client_name=client_name,
                name_value=name_value,
                content_value=content_value
            )
    return


def __get_customizer_secrets_dat(customizer: Customizer) -> Customizer:
    """
    Get DAT credentials by the client and credential_name
    ====================================================================================================
    :param customizer:
    :return:
    """
    name_value = getattr(customizer, 'secrets_name')
    with create_application_sql_engine().connect() as con:
        result = con.execute(
            sqlalchemy.text(
                """
                SELECT
                    client_name,
                    name_value,
                    content_value
                FROM public.gds_compiler_credentials_dat
                WHERE client_name = :client_name
                AND name_value = :name_value;
                """
            ),
            client_name=customizer.client,  # camel-case client name
            name_value=name_value
        ).first()
    customizer.secrets_dat = json.dumps(result['content_value']) if result else {}
    return customizer


def __get_customizer_secrets(customizer: Customizer) -> Customizer:
    """
    Get OAuth credentials by the client and credential_name
    ====================================================================================================
    :param customizer:
    :return:
    """
    name_value = getattr(customizer, 'credential_name')
    with create_application_sql_engine().connect() as con:
        result = con.execute(
            sqlalchemy.text(
                """
                SELECT content_value
                FROM public.gds_compiler_credentials
                WHERE name_value = :name_value;
                """
            ),
            name_value=name_value
        ).first()
    customizer.secrets = result['content_value'] if result else {}
    return customizer


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
    for sheet in customizer.configuration_workbook['sheets']:
        if sheet['table']['type'] == 'lookup':
            if sheet['table']['active']:
                lookup_table_existence = check_table_exists(customizer, schema=sheet)

                if not lookup_table_existence:
                    print(f'{sheet["table"]["name"]} does not exist, creating...')
                    create_table_from_schema(customizer, sheet)

    return 0


def build_reporting_tables(customizer) -> int:
    for sheet in customizer.configuration_workbook['sheets']:
        if sheet['table']['type'] == 'reporting':
            if sheet['table']['active']:
                print(f'Checking if {sheet["table"]["name"]} exists...')
                reporting_table_existence = check_table_exists(customizer, schema=sheet)

                if not reporting_table_existence:
                    print(f'{sheet["table"]["name"]} does not exist, creating...')
                    create_table_from_schema(customizer, sheet)

    return 0


def build_source_tables(customizer) -> int:
    for sheet in customizer.configuration_workbook['sheets']:
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

        for sheets in customizer.configuration_workbook['sheets']:
            if sheets['table']['type'] == 'reporting':
                if sheets['table']['active']:
                    for column in sheets['table']['columns']:
                        if column['master_include']:
                            if not any(
                                    column['name'] == d['name'] for d in customizer.marketing_data['table']['columns']
                            ):
                                customizer.marketing_data['table']['columns'].append(column)

        # Add custom columns to end of marketing table
        if customizer.custom_marketing_data_columns['table']['active']:
            if customizer.custom_marketing_data_columns['table']['columns']:
                customizer.marketing_data['table']['columns'].append(
                    customizer.custom_marketing_data_columns['table']['columns']
                )

        create_table_from_schema(customizer, schema=customizer.marketing_data)

    return 0


def reshape_lookup_data(customizer, df, sheet):

    df.columns = map(str.lower, df.columns)
    df.columns = [col.replace(' ', '_') for col in df.columns]

    if customizer.columns_to_drop:
        if customizer.columns_to_drop['status']:
            df.drop(columns=customizer.columns_to_drop['columns'], inplace=True)

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

    if customizer.columns_to_drop:
        if customizer.columns_to_drop['status']:
            df.drop(columns=customizer.columns_to_drop['columns'], inplace=True)

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
    if customizer.configuration_workbook['lookup_refresh_status'] is False:
        for sheet in customizer.configuration_workbook['sheets']:
            if sheet['table']['type'] == 'lookup':
                if sheet['table']['active']:
                    # 2020-07-27: patch by jws to handle dynamic credential retrieval
                    gs = get_customizer_secrets(GoogleSheetsManager(), include_dat=False)
                    raw_lookup_data = gs.get_spreadsheet_by_name(

                        workbook_name=customizer.configuration_workbook['config_sheet_name'],
                        worksheet_name=sheet['sheet'],

                    )

                    clear_lookup_table_data(customizer=customizer, sheet=sheet)
                    df = reshape_lookup_data(df=raw_lookup_data, customizer=customizer, sheet=sheet)
                    insert_other_data(customizer, df=df, sheet=sheet)

                    print(f"SUCCESS: {sheet['table']['name']} Refreshed.")

    # Once one script refreshed lookup tables, set global status to True to bypass with following scripts
    customizer.configuration_workbook['lookup_refresh_status'] = True

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


def get_sheets_for_tablespace(customizer: Customizer, tablespace: str) -> list:
    keep_sheets = []
    tablespace = tablespace
    assert tablespace, "No tablespace setup for " + customizer.__class__.__name__
    sheets = customizer.configuration_workbook['sheets']
    for sheet in sheets:
        table = sheet['table']
        if tablespace in table['tablespace']:
            keep_sheets.append(sheet)
    return keep_sheets


def check_stages_and_attributes(stages: list, attributes: list) -> None:
    assert stages and attributes, \
        "One of startup variables are empty (PROCESSING_STAGES, REQUIRED_ATTRIBUTES)"


def _get_value_from_args_by_flag(argv: list, flag: str, default: int = 1) -> int:
    """
    Under the premise that flags are used to disable functionality, we assume the functionality is set to 1
    unless otherwise stated by the given flag
    :param argv:
    :param flag:
    :return:
    """
    argv = [arg for arg in argv if flag in arg]
    if argv:
        return int(argv[0].replace(flag, ''))
    else:
        return default


def get_pull_from_args(argv: list) -> int:
    flag = '--pull='
    return _get_value_from_args_by_flag(argv=argv, flag=flag)


def get_ingest_from_args(argv: list) -> int:
    flag = '--ingest='
    return _get_value_from_args_by_flag(argv=argv, flag=flag)


def get_backfilter_from_args(argv: list) -> int:
    flag = '--backfilter='
    return _get_value_from_args_by_flag(argv=argv, flag=flag)


def get_expedited_from_args(argv: list) -> int:
    flag = '--expedited='
    return _get_value_from_args_by_flag(argv=argv, flag=flag, default=0)


def get_args(argv: list) -> tuple:
    pull = get_pull_from_args(argv=argv)
    ingest_only = get_ingest_from_args(argv=argv)
    backfilter_only = get_backfilter_from_args(argv=argv)
    expedited = get_expedited_from_args(argv=argv)
    print(f'ingest_only: {ingest_only}, backfilter_only: {backfilter_only}, expedited: {expedited}')
    return pull, ingest_only, backfilter_only, expedited


def setup(script_name: str, expedited: int):
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

    if not expedited:

        # Dynamically inserts correct vertical specific alert slack channel to recipients list
        insert_vertical_specific_alert_channel(customizer=customizer)

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


def run_processing(df: pd.DataFrame, customizer: custom.Customizer, processing_stages: list):
    for stage in processing_stages:
        print(f'Checking for processing stage {stage}...')
        if get_optional_attribute(cls=customizer, attribute=stage):
            if stage != 'post_processing':
                print(f'\tNow processing stage {stage}')
                df = get_optional_attribute(cls=customizer, attribute=stage)(df=df)
    return df


def dynamic_typing(customizer: custom.Customizer):
    for sheet in customizer.configuration_workbook['sheets']:
        if sheet['table']['name'] == customizer.get_attribute('table'):
            customizer.get_attribute('schema')['columns'].extend(sheet['table']['columns'])


def refresh_source_tables(customizer: custom.Customizer):
    today = datetime.date.today()

    if today.day in customizer.configuration_workbook['source_refresh_dates']:
        for sheet in customizer.configuration_workbook['sheets']:
            if sheet['table']['type'] == 'source':
                if sheet['table']['active']:
                    # 2020-07-27: patch by jws to handle dynamic credential retrieval
                    gs = get_customizer_secrets(GoogleSheetsManager(), include_dat=False)
                    raw_source_data = gs.get_spreadsheet_by_name(
                        workbook_name=customizer.configuration_workbook['config_sheet_name'],
                        worksheet_name=sheet['sheet']
                    )

                    clear_source_table_data(customizer=customizer, sheet=sheet)
                    df = reshape_source_table_data(customizer=customizer, df=raw_source_data, sheet=sheet)
                    insert_other_data(customizer, df=df, sheet=sheet)

                    print(f"SUCCESS: {sheet['table']['name']} Refreshed.")
    else:
        print('Not listed refresh day.')

    return 0


# TODO (bburkho) Update to load from workbook.json
def systematic_procedure_execution() -> list:
    table_names = []
    for sheet in Customizer().configuration_workbook['sheets']:
        if sheet['table']['type'] == 'reporting':
            if sheet['table']['active']:
                table_names.append(sheet['table']['name'])

    return table_names if True else None


def procedure_flag_indicator(refresh_indicator: sys.argv, back_filter: bool, ingest: bool) -> (bool, bool):

    if 'backfill' in refresh_indicator:
        back_filter = True

    if 'ingest' in refresh_indicator:
        ingest = True

    return back_filter, ingest


def insert_vertical_specific_alert_channel(customizer: custom.Customizer):
    for slack_vertical in customizer.vertical_specific_slack_alerts:
        core_vertical = customizer.vertical

        if core_vertical == slack_vertical:
            slack_vertical_present = True
            break

        else:
            slack_vertical_present = False

    assert slack_vertical_present, f'No slack address found for vertical: "{customizer.vertical}", check vertical field in app.json.'
    customizer.recipients.append(customizer.vertical_specific_slack_alerts[core_vertical])


def build_path_by_os(root):
    client_os = platform.system()

    if client_os == 'Windows':
        venv_path = os.path.join(root, 'venv', 'Scripts', 'python.exe')

    elif client_os == 'Darwin':
        venv_path = os.path.join(root, 'venv', 'bin', 'python')

    elif client_os == 'Linux':
        venv_path = os.path.join(root, 'venv', 'bin', 'python')

    else:
        venv_path = None

    assert venv_path, "Unknown OS detected, or return value from 'platform.systems()' function has changed."

    return venv_path




