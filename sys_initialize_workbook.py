"""
Initialize Workbook Module

This script will create a brand new workbook from the configuration given in workbook.json


"""
from utils import grc
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

    # pass the client to ConfigurationManager, and run initialize_workbook()

    return


if __name__ == '__main__':
    main()
