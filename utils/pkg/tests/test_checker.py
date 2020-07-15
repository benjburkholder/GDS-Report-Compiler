"""
Test GRC Package Checker Module
"""
import unittest
from ..checker import PackageChecker


class TestPackageChecker(unittest.TestCase):

    valid_name = 'lmpy'
    invalid_name = 'lumpy'

    def test_check_command_line_tool_installed(self):
        pc = PackageChecker()

        valid_result = pc.check_command_line_tool_installed(
            name=self.valid_name
        )
        self.assertTrue(valid_result)

        invalid_result = pc.check_command_line_tool_installed(
            name=self.invalid_name
        )
        self.assertFalse(invalid_result)
