"""
Test Config Manager
"""
import unittest

from utils import grc
from utils.gs_manager import GoogleSheetsManager
from utils.config_manager import ConfigManager


class TestConfigManager(unittest.TestCase):

    @staticmethod
    def __get_client():
        gs = GoogleSheetsManager()
        gs = grc.get_customizer_secrets(
            customizer=gs,
            include_dat=False
        )
        # noinspection PyUnresolvedReferences
        return gs.create_client()

    def test_create(self):
        self.client = self.__get_client()
        self.cm = ConfigManager(client=self.client)
        self.cm.initialize_workbook()
        # TODO: write quality assertions here
        self.assertTrue(True)
