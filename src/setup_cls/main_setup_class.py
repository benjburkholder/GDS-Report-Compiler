"""
MAIN SETUP CLASS

Central class containing attributes / methods to be inherited by other setup classes.
"""

import pathlib
import json


class MainSetupClass:

    def __init__(self):

        # basic path to directory for final file generation
        self.stored_json_directory = pathlib.Path(__file__).parents[2] / 'conf' / 'stored'

        # load workbook template into path variable
        self.stored_workbook_json = pathlib.Path(__file__).parents[2] / 'conf' / 'stored' / 'workbook_template.json'

        # load app template into path variable
        self.stored_app_json = pathlib.Path(__file__).parents[2] / 'conf' / 'stored' / 'app_template.json'

        # converts json to python dict
        self.converted_workbook_json = self.__convert_json_to_dict(raw_json=self.stored_workbook_json)
        self.converted_app_json = self.__convert_json_to_dict(raw_json=self.stored_app_json)

    @staticmethod
    def __convert_json_to_dict(raw_json: json) -> dict:
        with open(raw_json) as f:
            data = json.load(f)

        return data

