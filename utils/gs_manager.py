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
import pandas as pd
from utils.cls.core import Customizer
from oauth2client.service_account import ServiceAccountCredentials


class GoogleSheetsManager(Customizer):
    """
    Self-Storage Vertical account configuration class module
    """

    credential_name = 'ServiceAccount'

    def __init__(self):
        super().__init__()
        self.base_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self.secrets = ''

    def create_client(self):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.secrets, self.scope)
        return gspread.authorize(creds)

    def get_spreadsheet_by_name(self, workbook_name: str, worksheet_name: str) -> pd.DataFrame:
        """
        High-level helper method to get a spreadsheet by name
        Returns list of dictionaries (record format)
        """
        client = self.create_client()
        data_dict = client.open(
            workbook_name
        ).worksheet(
            worksheet_name
        ).get_all_records()
        df = pd.DataFrame(data_dict)
        return df
