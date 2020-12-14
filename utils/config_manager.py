"""
Configuration Manager Module
"""
import os
import json
import gspread
import pathlib
import webbrowser
from time import sleep
import gspread_formatting as gsf

from conf.static import SHEETS


class ConfigManager:

    def __init__(self):
        self.whitelist_emails = SHEETS['WHITELIST_EMAILS']
        self.template_workbook_name = 'workbook_template.json'
        self.template_app_name = 'app_template.json'
        self.workbook_name = 'workbook.json'
        self.app_name = 'app.json'

    rows_default = 100

    def initialize_workbook(self, client: gspread.client) -> None:
        # check if key exists in current configuration
        wb_config = self.get_workbook_config(workbook_name=self.workbook_name)
        app_config = self.get_app_config(app_name=self.app_name)
        client_name = app_config.get('client')
        release_version = app_config.get('version')
        key = wb_config.get('key')
        if not key:
            sh = self.create_workbook(client_name=client_name, release_version=release_version, client=client)
        else:
            sh = self.update_workbook(key=key, client=client)

        webbrowser.open(sh.url)
        wb_config['key'] = input('ENTER WORKBOOK KEY (IN URL): ')
        wb_config['config_sheet_name'] = sh.title
        self.write_workbook_config(config=wb_config, workbook_name=self.workbook_name)

        # for each sheet in workbook
        worksheets = sh.worksheets()
        sheet_titles = [sheet.title for sheet in worksheets]
        for sheet in wb_config.get('sheets', []):
            sleep(15)
            if sheet['table'].get('type') in ('lookup', 'source'):
                sheet_name = sheet['sheet']
                columns = sheet['table']['columns']
                num_cols = len(columns)
                num_rows = sheet['table'].get('max_rows') or self.rows_default
                if sheet['sheet'] not in sheet_titles:
                    print(f"Now creating {sheet['sheet']}")
                    ws = sh.add_worksheet(
                        title=sheet_name,
                        rows=num_rows,
                        cols=num_cols
                    )
                    idx = 1
                    for col in columns:
                        value = col['name']
                        sleep(15)
                        ws.update_cell(1, idx, value)
                        zero_idx = idx - 1
                        ltr = self._letter_from_idx(idx=zero_idx)
                        address = f'{ltr.upper()}1'
                        fmt = gsf.CellFormat(
                            textFormat=gsf.TextFormat(
                                bold=True,
                                foregroundColor=gsf.Color(0, 0, 0),
                                fontSize=18
                            )
                        )
                        gsf.format_cell_range(
                            worksheet=ws,
                            name=address,
                            cell_format=fmt
                        )
                        gsf.set_column_width(ws, ltr, 150)
                        idx += 1
        return

    project_root_idx = 1

    # the ASCII base to start from when converting a number to a letter
    base_ord = 97

    @staticmethod
    def _letter_from_idx(idx: int) -> str:
        return chr(idx + 97)

    def get_project_root(self):
        """
        Returns the global location of the project root
        ====================================================================================================
        :return:
        """
        return pathlib.Path(__file__).parents[self.project_root_idx]

    def write_workbook_config(self, config: dict, workbook_name: str) -> None:
        path = os.path.join(
            self.get_project_root(),
            'conf',
            'stored',
            workbook_name
        )
        with open(path, 'w') as file:
            file.write(json.dumps(config))

    def get_workbook_config(self, workbook_name: str) -> dict:
        path = os.path.join(
            self.get_project_root(),
            'conf',
            'stored',
            workbook_name
        )
        with open(path, 'r') as file:
            return json.load(file)

    def get_app_config(self, app_name: str) -> dict:
        path = os.path.join(
            self.get_project_root(),
            'conf',
            'stored',
            app_name
        )
        with open(path, 'r') as file:
            return json.load(file)

    def write_app_config(self, config: dict, app_name: str) -> None:
        path = os.path.join(
            self.get_project_root(),
            'conf',
            'stored',
            app_name
        )
        with open(path, 'w') as file:
            file.write(json.dumps(config))

    @staticmethod
    def _get_spreadsheet_title(client_name: str, release_version: str) -> str:
        return f'{client_name} | Configuration Workbook (v{release_version})'

    def _share_workbook_with_whitelist(self, sh: gspread.Spreadsheet) -> None:
        for email in self.whitelist_emails:
            sh.share(
                email,
                perm_type='user',
                role='writer'
            )

    def create_workbook(self, client_name: str, release_version: str, client: gspread.client) -> gspread.Spreadsheet:
        tries = 3
        while True:
            try:
                sh = client.create(
                    self._get_spreadsheet_title(client_name=client_name, release_version=release_version)
                )
                self._share_workbook_with_whitelist(sh=sh)
                return sh
            except gspread.exceptions.APIError as api_err:
                print(api_err)
                sleep(tries * 10)
                tries -= 1
                if tries == 0:
                    raise api_err

    def update_workbook(self, key: str, client: gspread.client) -> gspread.Spreadsheet:
        sh = client.open_by_key(key=key)
        self._share_workbook_with_whitelist(sh=sh)
        return sh
