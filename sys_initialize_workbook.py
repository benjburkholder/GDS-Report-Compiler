"""
Initialize Workbook Module
This script will create a brand new workbook from the configuration given in workbook.json
"""
from utils import grc
from utils.config_startup import ConfigStartup
from utils.gs_manager import GoogleSheetsManager


def main() -> None:

    # collect input and generate json files
    config_startup = ConfigStartup()
    config_startup.workbook_flow()
    config_startup.app_flow()

    gs = GoogleSheetsManager()
    gs = grc.get_customizer_secrets(
        customizer=gs,
        include_dat=False
    )
    # noinspection PyUnresolvedReferences
    client = gs.create_client()

    # create a google sheets client with the help of GoogleSheetsManager
    config_startup.initialize_workbook(client=client)

    return


if __name__ == '__main__':
    main()
