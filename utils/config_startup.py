"""
Configuration Startup Module
"""

from utils.config_manager import ConfigManager


class ConfigStartup(ConfigManager):
    def __init__(self):
        super().__init__()
        # converts json to python dict
        self.converted_workbook_json = self.get_workbook_config(workbook_name=self.template_workbook_name)
        self.converted_app_json = self.get_app_config(app_name=self.template_app_name)

    def workbook_flow(self) -> None:

        # filter and collect user selected data sources
        self._get_active_data_sources(
            data=self.converted_workbook_json
        )

        # configure new workbook.json from template
        self.generate_new_configuration_workbook(
        )

    def _get_active_data_sources(self, data: dict) -> None:
        print('- DATA SOURCE SELECTION -')
        active_data_sources = []
        data_source_tables = data['sheets']
        for table in data_source_tables:
            if table['table']['type'] == 'reporting':
                activation_flag = input(f'ACTIVATE {table["table"]["name"]}?: ')
                if activation_flag == 'y':
                    active_data_sources.append(table["table"]['name'])

        self.active_data_sources = active_data_sources

    def _activate_source_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'source':
                result = [x for x in self.active_data_sources for y in table['table']['tablespace'] if y in x]

                if result:
                    table['table']['active'] = True

    def _activate_lookup_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'lookup':
                result = [x for x in self.active_data_sources for y in table['table']['tablespace'] if y in x]

                if result:
                    table['table']['active'] = True

    def _activate_reporting_tables(self) -> None:
        for table in self.converted_workbook_json['sheets']:
            if table['table']['type'] == 'reporting':

                if table['table']['name'] in self.active_data_sources:
                    table['table']['active'] = True

    def generate_new_configuration_workbook(self) -> None:
        self._activate_reporting_tables(
        )
        self._activate_lookup_tables(
        )
        self._activate_source_tables(
        )
        self.write_workbook_config(config=self.converted_workbook_json, workbook_name=self.workbook_name)

        print("SUCCESS: DATA-SOURCE SCHEMA CONFIGURATION COMPLETE.")

    def app_flow(self):
        client_db_name = input('CLIENT DATABASE NAME: ')
        client_full_name = input('CLIENT FULL NAME: ')
        client_vertical = input(
            """ENTER CLIENT VERTICAL:
                    senior_living
                    self_storage
                    omni_local 
                    all_in 
                    addiction
                                    : """)

        # configure new app.json from template
        self.generate_new_app_file(
            client_db_name=client_db_name,
            client_vertical=client_vertical,
            client_name=client_full_name
        )

    def _set_client_db_name(self, client_db_name: str) -> None:
        self.converted_app_json['db']['DATABASE'] = client_db_name

    def _set_client_vertical(self, client_vertical: str) -> None:
        self.converted_app_json['vertical'] = client_vertical

    def _set_client_name(self, client_full_name: str) -> None:
        self.converted_app_json['client'] = client_full_name

    def generate_new_app_file(self, client_db_name: str, client_vertical: str, client_name: str) -> None:
        self._set_client_db_name(
            client_db_name=client_db_name
        )
        self._set_client_vertical(
            client_vertical=client_vertical
        )
        self._set_client_name(
            client_full_name=client_name
        )
        self.write_app_config(config=self.converted_app_json, app_name=self.app_name)

        print("SUCCESS: APP CONFIGURATION GENERATED.")

