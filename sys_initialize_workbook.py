"""
Initialize Workbook Module

This script will create a brand new workbook from the configuration given in workbook.json


"""
from utils import grc
from utils.config_manager import ConfigManager
from utils.gs_manager import GoogleSheetsManager


def main() -> None:

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
