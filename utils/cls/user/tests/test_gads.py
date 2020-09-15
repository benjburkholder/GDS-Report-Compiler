"""
Unit Tests for Google Ads (google_ads.py)
"""
import sqlalchemy

from .test_core import TestHelper
from ..google_ads import (
    GoogleAdsCampaign,
    GoogleAdsKeyword
)


class TestGoogleAdsCampaign(TestHelper):
    """
    Google Ads variant of TestHelper

    Use this class to test campaign data for quality / mapping / functionality
    """

    src_cls = GoogleAdsCampaign()

    def test_unmapped_campaigns_all_source_table(self):
        """
        Check the configured SOURCE TABLE for unmapped campaigns at all entity levels
        ====================================================================================================
        :return:
        """
        self._run_pretest_assertions()
        table_schema = self._get_table_schema()
        engine = self._provision_engine()
        data_source_name = self.src_cls.get_attribute(attrib='data_source')
        where_clause = self.generate_where_clause(
            data_source_name=data_source_name,
            levels=None
        )
        sql = sqlalchemy.text(
            f"""
            SELECT 
                campaign AS campaign_name,
                campaign_id AS campaign_id
            FROM {table_schema['schema']}.marketing_data
            {where_clause}
            """
        )
        with engine.connect() as con:
            results = con.execute(
                sql
            ).fetchall()

        unmapped_campaigns = [
            str(row['campaign_name']) + '-' + str(row['campaign_id']) for row in results
        ] if results else []

        self.assertEqual(
            len(unmapped_campaigns),
            0,
            'The following Google Ads campaigns are not mapped to an entity! ' + str(unmapped_campaigns)
        )


class TestGoogleAdsKeyword(TestHelper):
    """
    Google Ads variant of TestHelper

    Use this class to test campaign (keyword) data for quality / mapping / functionality
    """
    src_cls = GoogleAdsKeyword()

    def test_unmapped_campaigns_all_source_table(self):
        """
        Check the configured SOURCE TABLE for unmapped campaigns at all entity levels
        ====================================================================================================
        :return:
        """
        self._run_pretest_assertions()
        table_schema = self._get_table_schema()
        engine = self._provision_engine()
        data_source_name = self.src_cls.get_attribute(attrib='data_source')
        where_clause = self.generate_where_clause(
            data_source_name=data_source_name,
            levels=None
        )
        sql = sqlalchemy.text(
            f"""
            SELECT 
                campaign AS campaign_name,
                campaign_id AS campaign_id
            FROM {table_schema['schema']}.marketing_data
            {where_clause}
            """
        )
        with engine.connect() as con:
            results = con.execute(
                sql
            ).fetchall()

        unmapped_campaigns = [
            str(row['campaign_name']) + '-' + str(row['campaign_id']) for row in results
        ] if results else []

        self.assertEqual(
            len(unmapped_campaigns),
            0,
            'The following Google Ads campaigns are not mapped to an entity! ' + str(unmapped_campaigns)
        )
