"""
APP SETUP CLASS

"""

import json

from src.setup_cls.main_setup_class import MainSetupClass


class AppSetupClass(MainSetupClass):

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

        # configure new app file from template
        self.generate_new_app_file(
            client_db_name=client_db_name,
            client_vertical=client_vertical,
            client_name=client_full_name
        )

    def __set_client_db_name(self, client_db_name: str) -> None:
        self.converted_app_json['db']['DATABASE'] = client_db_name

    def __set_client_vertical(self, client_vertical: str) -> None:
        self.converted_app_json['vertical'] = client_vertical

    def __set_client_name(self, client_full_name: str) -> None:
        self.converted_app_json['client'] = client_full_name

    def generate_new_app_file(self, client_db_name: str, client_vertical: str, client_name: str) -> None:
        self.__set_client_db_name(
            client_db_name=client_db_name
        )

        self.__set_client_vertical(
            client_vertical=client_vertical
        )

        self.__set_client_name(
            client_full_name=client_name
        )

        with open(self.stored_json_directory / 'app.json', 'w') as file:
            json.dump(self.converted_app_json, file)

        print("SUCCESS: APP CONFIGURATION GENERATED.")

