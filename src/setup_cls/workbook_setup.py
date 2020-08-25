"""
WORKBOOK SETUP CLASS

"""

import json

from src.setup_cls.main_setup_class import MainSetupClass


class WorkbookSetupClass(MainSetupClass):

    def workbook_flow(self) -> None:

        # filter and collect user selected data sources
        self.__get_active_data_sources(
            data=self.converted_workbook_json
        )

        # configure new workbook from template
        self.generate_new_configuration_workbook(
        )

    def __get_active_data_sources(self, data: dict) -> None:
        print('- DATA SOURCE SELECTION -')
        active_data_sources = []
        data_source_tables = data['sheets']
        for table in data_source_tables:
            if table['table']['type'] == 'reporting':
                activation_flag = input(f'ACTIVATE {table["table"]["name"]}?: ')
                if activation_flag == 'y':
                    active_data_sources.append(table["table"]['name'])

        self.active_data_sources = active_data_sources

    def __activate_source_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'source':
                result = [x for x in self.active_data_sources for y in table['table']['tablespace'] if y in x]

                if result:
                    table['table']['active'] = True

    def __activate_lookup_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'lookup':
                result = [x for x in self.active_data_sources for y in table['table']['tablespace'] if y in x]

                if result:
                    table['table']['active'] = True

    def __activate_reporting_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'reporting':

                if table['table']['name'] in self.active_data_sources:
                    table['table']['active'] = True

    def generate_new_configuration_workbook(self) -> None:
        self.__activate_reporting_tables(
        )

        self.__activate_lookup_tables(
        )

        self.__activate_source_tables(
        )

        with open(self.stored_json_directory / 'workbook.json', 'w') as file:
            json.dump(self.converted_workbook_json, file)

        print("SUCCESS: WORKBOOK CONFIGURATION GENERATED.")

