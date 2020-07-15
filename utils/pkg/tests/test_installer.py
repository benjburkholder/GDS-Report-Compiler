"""
Test GRC Package Installer Module
"""
import unittest
from ..installer import PackageInstaller


class TestPackageChecker(unittest.TestCase):

    package_install = {
        'name': 'google-analytics-py',
        'pip_alias': 'googleanalyticspy',
        'version': 'latest'
    }

    src_cls = PackageInstaller()

    def test_install_packages(self):
        result = self.src_cls.install_package(
            package=self.package_install
        )
        self.assertTrue(result)

    def test_remove_package(self):
        result = self.src_cls.remove_package(
            package=self.package_install
        )
        self.assertTrue(result)

    def test_update_package(self):
        result = self.src_cls.update_package(
            package=self.package_install
        )
        self.assertTrue(result)
