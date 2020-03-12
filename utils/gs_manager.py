# -*- coding: utf-8 -*-
"""
@title: Google Sheet Configuration Module
@author: Jake Schroeder / Ben Burkholder
@status: Production
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
import os
import pathlib
import gspread
import datetime
import calendar
import pandas as pd
from utils import custom
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheetsManager:
    """
    Self-Storage Vertical account configuration class module
    """

    def __init__(self, client_name: str):
        self.base_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent
        self.service_credentials = os.path.join(self.base_path, 'secrets', '{}_Service.json'.format(client_name))
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

    def get_spreadsheet_by_name(self, workbook_name: str, worksheet_name: str) -> list:
        """
        High-level helper method to get a spreadsheet by name
        Returns list of dictionaries (record format)
        """

        creds = ServiceAccountCredentials.from_json_keyfile_name(self.service_credentials, self.scope)
        client = gspread.authorize(creds)

        data_dict = client.open(
            workbook_name
        ).worksheet(
            worksheet_name
        ).get_all_records()

        return data_dict
