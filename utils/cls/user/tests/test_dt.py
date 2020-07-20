"""
Unit Tests for DialogTech (dt.py)
"""
import sqlalchemy

from .test_core import TestHelper
from utils.cls.user.dt import DialogtechCallDetail


class TestDialogtechCallDetail(TestHelper):
    """
    AccountCost variant of TestHelper
    """
    src_cls = DialogtechCallDetail()

    def test_unmapped_phone_labels_all(self):
        self._run_pretest_assertions()
        table_schema = self._get_table_schema()
        engine = self._provision_engine()
        data_source_name = self.src_cls.get_attribute(attrib='data_source')
        where_clause = self.generate_where_clause(data_source_name=data_source_name)
        sql = sqlalchemy.text(
            f"""
            SELECT
                DISTINCT phone_label
            FROM {table_schema['schema']}.marketing_data
            {where_clause};
            """
        )
        with engine.connect() as con:
            results = con.execute(
                sql
            ).fetchall()

        # TODO: improve the way we fetch the offending records
        self.assertEqual(len(results), 0)

