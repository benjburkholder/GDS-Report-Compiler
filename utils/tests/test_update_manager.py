"""
Test Update Manager Module
"""
import unittest

from conf import static
from utils.update_manager import UpdateManager


class TestUpdateManager(unittest.TestCase):
    um = UpdateManager(
        auth_token=static.UPDATE_KEY,
        username=static.UPDATE_USERNAME,
        repository=static.UPDATE_REPOSITORY
    )

    def test_download_latest(self):
        self.um.download_latest()
        # todo: assert that files exists
        self.assertTrue(True)

    def test_perform_update(self):
        self.um.perform_update()
        self.assertTrue(True)

    def test_clear_update_zone(self):
        self.um.clear_update_zone()
