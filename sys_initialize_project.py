"""
Initialize Client Project

This script will:
 - Prompt user to choose which data sources to be activated for client project, generating workbook.json from template.
 - Prompt user to enter app level values, generating app.json from template.
 - Create a brand new workbook from the configuration given in workbook.json.


"""
from utils import grc
from utils.config_manager import ConfigManager
from utils.gs_manager import GoogleSheetsManager

from src.setup_cls.workbook_setup import WorkbookSetupClass
from src.setup_cls.app_setup import AppSetupClass


def main() -> None:

    # user selects active data sources
    workbook_cls = WorkbookSetupClass()
    workbook_cls.workbook_flow()

    # user enters app level values
    app_cls = AppSetupClass()
    app_cls.app_flow()

    # start workbook initialization
    gs = GoogleSheetsManager()
    gs = grc.get_customizer_secrets(
        customizer=gs,
        include_dat=False
    )
    # noinspection PyUnresolvedReferences
    client = gs.create_client()

    # create a google sheets client with the help of GoogleSheetsManager
    ConfigManager(client=client).initialize_workbook()
    return


if __name__ == '__main__':
    main()
