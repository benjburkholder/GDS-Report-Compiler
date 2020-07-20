"""
Unit Tests for Enquire MAP (enquire_map.py)
"""
import sqlalchemy

from .test_core import TestHelper
from ..enquire_map import (
    EnquireMapEmailPerformanceSummary,
    EnquireMapHistories
)


class TestEnquireMapEmailPerformanceSummary(TestHelper):
    """
    Enquire MAP variant of TestHelper

    Use this to test data quality of email performance report
    """

    src_cls = EnquireMapEmailPerformanceSummary()


class TestEnquireMapHistories(TestHelper):
    """
    Enquire MAP variant of TestHelper

    Use this to test data quality of histories data source
    """

    src_cls = EnquireMapHistories()

    def test_unmapped_ctns_all_marketing_data(self):
        """
        Check MARKETING DATA table for instances where ALL levels of entity column are unmapped

        Note that we cannot rely on the data source identifier since call data is commonly derived
        from Enquire Map - Histories data source through post-processing
        ====================================================================================================
        :return:
        """
        self._run_pretest_assertions()
        table_schema = self._get_table_schema()
        engine = self._provision_engine()
        where_clause = self.generate_where_clause(
            data_source_name=None,
            levels=None
        )
        sql = sqlalchemy.text(
            f"""
            SELECT
                DISTINCT
                "to" AS call_tracking_number
            FROM {table_schema['schema']}.marketing_data
            {where_clause}
            AND history_type IN ('PhoneCall')
            AND report_date IS NOT NULL
            """
        )
        with engine.connect() as con:
            results = con.execute(
                sql
            ).fetchall()

        unmapped_numbers = [
            row['call_tracking_number'] for row in results
        ] if results else []

        self.assertEqual(
            len(unmapped_numbers),
            0,
            'The following numbers are not mapped to an entity! ' + str(unmapped_numbers)
        )
