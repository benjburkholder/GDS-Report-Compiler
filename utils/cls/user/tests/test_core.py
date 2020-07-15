"""
Core Testing Module

Provides inherited methods for other data source tests to use
"""
# STANDARD IMPORTS
import unittest
import sqlalchemy

# CUSTOM IMPORTS
from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers
CORE_CLS_NAME = 'TestHelper'


class TestHelper(unittest.TestCase):

    src_cls = Customizer()

    def _get_table_schema(self):
        """
        Helper function to get and return the table schema from the src_cls Customizer instance
        ====================================================================================================
        :return:
        """
        table = self.src_cls.get_attribute('table')
        workbook = self.src_cls.CONFIGURATION_WORKBOOK
        for sheet in workbook.get('sheets', []):
            if sheet.get('table'):
                if sheet.get('table', {}).get('name') == table:
                    return sheet['table']

    def _provision_engine(self):
        """
        Helper function to provision a database engine using the pre-configured src_cls Customizer instance
        ====================================================================================================
        :return:
        """
        return postgres_helpers.build_postgresql_engine(customizer=self.src_cls)

    def _run_pretest_assertions(self):
        """
        This method must be called before ANY inherited test method begins
        Ensures core class is not invoked for testing (Customizer)
        Ensures non-active data sources are not invoked erroneously
        ====================================================================================================
        :return:
        """
        # protections to ensure core.Customizer is not audited / invoked directly
        if self.__class__.__name__ == CORE_CLS_NAME:
            raise unittest.SkipTest('Core Testing Class')
        # support for setting Customizer instances active or inactive
        if self.src_cls.get_attribute('active') is False:
            raise unittest.SkipTest('Data Source Not Active')

    def test_valid(self):
        """
        Basic tests to ensure data is present - applicable to all data sources and is inherited by
        default
        ====================================================================================================
        :return:
        """
        self._run_pretest_assertions()
        # construct the statement to check for existence of data on the target table of the data source
        table_schema = self._get_table_schema()
        engine = self._provision_engine()
        sql = sqlalchemy.text(
            f"""
            SELECT COUNT(*) as count_value
            FROM {table_schema['schema']}.{table_schema['name']};
            """
        )
        with engine.connect() as con:
            result = con.execute(sql).first()
        self.assertIsNotNone(result)
        self.assertGreater(result['count_value'], 0)
        self.assertTrue(True)

