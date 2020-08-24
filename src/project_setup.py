"""
SETUP

USER WILL RUN THIS FILE FIRST TO CONFIGURE PROJECT VALUES.
"""

import pathlib
import json


def main():

    # basic path to directory for final file generation
    stored_json_directory = pathlib.Path(__file__).parents[1] / 'conf' / 'stored'

    # load workbook template into path variable
    stored_workbook_json = pathlib.Path(__file__).parents[1] / 'conf' / 'stored' / 'workbook_template.json'

    # load app template into path variable
    stored_app_json = pathlib.Path(__file__).parents[1] / 'conf' / 'stored' / 'app_template.json'

    # converts json to python dict
    converted_workbook_json = __convert_json_to_dict(raw_json=stored_workbook_json)
    converted_app_json = __convert_json_to_dict(raw_json=stored_app_json)

    # determine which type of file to start generating
    file_to_generate = input("ENTER FILE TYPE TO GENERATE (workbook, app): ")

    if file_to_generate == 'workbook':

        # start user input flow
        configuration_workbook_name = input('CONFIGURATION WORKBOOK NAME: ')
        configuration_workbook_key = input('CONFIGURATION WORKBOOK KEY (IN URL): ')

        # filter and collect user selected data sources
        active_data_sources = __get_active_data_sources(data=converted_workbook_json)

        # configure new workbook from template
        generate_new_configuration_workbook(
            stored_json_directory=stored_json_directory,
            converted_json=converted_workbook_json,
            active_data_sources=active_data_sources,
            configuration_workbook_name=configuration_workbook_name,
            configuration_workbook_key=configuration_workbook_key
        )

    elif file_to_generate == 'app':

        client_db_name = input('CLIENT DATABASE NAME: ')
        client_full_name = input('CLIENT FULL NAME: ')
        client_vertical = input(
            'CLIENT VERTICAL ("senior_living", "self_storage", "omni_local", "all_in", "addiction"): ')

        # configure new app file from template
        generate_new_app_file(
            stored_json_directory=stored_json_directory,
            converted_json=converted_app_json,
            client_db_name=client_db_name,
            client_vertical=client_vertical,
            client_name=client_full_name
        )


def __convert_json_to_dict(raw_json):
    with open(raw_json) as f:
        data = json.load(f)

    return data


# ~~~~~~~~~~~~~ START: WORKBOOK.JSON UPDATES ~~~~~~~~~~~~~
def __get_active_data_sources(data):
    active_data_sources = []
    data_source_tables = data['sheets']
    for table in data_source_tables:
        if table['table']['type'] == 'reporting':
            activation_flag = input(f'ACTIVATE {table["table"]["name"]}?: ')
            if activation_flag == 'y':
                active_data_sources.append(table["table"]['name'])

    return active_data_sources


def __activate_source_tables(active_tables, updated_workbook):
    sheets = updated_workbook
    for table in sheets['sheets']:
        if table['table']['type'] == 'source':
            result = [x for x in active_tables for y in table['table']['tablespace'] if y in x]

            if result:
                table['table']['active'] = True

    return sheets


def __activate_lookup_tables(active_tables, updated_workbook):
    sheets = updated_workbook
    for table in sheets['sheets']:
        if table['table']['type'] == 'lookup':
            result = [x for x in active_tables for y in table['table']['tablespace'] if y in x]

            if result:
                table['table']['active'] = True

    return sheets


def __activate_reporting_tables(converted_json, active_tables):
    sheets = converted_json
    for table in sheets['sheets']:
        if table['table']['type'] == 'reporting':

            if table['table']['name'] in active_tables:
                table['table']['active'] = True

    return sheets


def __update_workbook_name(updated_workbook, configuration_workbook_name):
    updated_workbook['config_sheet_name'] = configuration_workbook_name

    return updated_workbook


def __update_workbook_url_key(updated_workbook, configuration_workbook_key):
    updated_workbook['key'] = configuration_workbook_key

    return updated_workbook


def generate_new_configuration_workbook(stored_json_directory, converted_json, active_data_sources, configuration_workbook_name, configuration_workbook_key):
    new_configured_configuration_workbook = __activate_reporting_tables(
        converted_json=converted_json,
        active_tables=active_data_sources
    )

    new_configured_configuration_workbook = __activate_lookup_tables(
        active_tables=active_data_sources,
        updated_workbook=new_configured_configuration_workbook
    )

    new_configured_configuration_workbook = __activate_source_tables(
        active_tables=active_data_sources,
        updated_workbook=new_configured_configuration_workbook
    )

    new_configured_configuration_workbook = __update_workbook_name(
        updated_workbook=new_configured_configuration_workbook,
        configuration_workbook_name=configuration_workbook_name
    )

    new_configured_configuration_workbook = __update_workbook_url_key(
        updated_workbook=new_configured_configuration_workbook,
        configuration_workbook_key=configuration_workbook_key
    )

    with open(stored_json_directory / 'workbook.json', 'w') as file:
        json.dump(new_configured_configuration_workbook, file)

    print("SUCCESS: WORKBOOK CONFIGURATION GENERATED.")
# ~~~~~~~~~~~~~ END: WORKBOOK.JSON UPDATES ~~~~~~~~~~~~~


# ~~~~~~~~~~~~~ START: APP.JSON UPDATES ~~~~~~~~~~~~~
def __update_client_db_name(updated_app_data, client_db_name):
    updated_app_data['db']['DATABASE'] = client_db_name

    return updated_app_data


def __update_client_vertical(updated_app_data, client_vertical):
    updated_app_data['vertical'] = client_vertical

    return updated_app_data


def __update_client_name(updated_app_data, client_full_name):
    updated_app_data['client'] = client_full_name

    return updated_app_data


def generate_new_app_file(stored_json_directory, converted_json, client_db_name, client_vertical, client_name):
    new_configured_app_file = __update_client_db_name(
        updated_app_data=converted_json,
        client_db_name=client_db_name
    )

    new_configured_app_file = __update_client_vertical(
        updated_app_data=new_configured_app_file,
        client_vertical=client_vertical
    )

    new_configured_app_file = __update_client_name(
        updated_app_data=new_configured_app_file,
        client_full_name=client_name
    )

    with open(stored_json_directory / 'app.json', 'w') as file:
        json.dump(new_configured_app_file, file)
# ~~~~~~~~~~~~~ END: APP.JSON UPDATES ~~~~~~~~~~~~~


if __name__ == '__main__':
    main()
