"""
Marketing Data Customizer Test Suite

"""
from unittest import TestCase

from utils.cls.pltfm.marketing_data import MarketingData


class TestMarketingData(TestCase):
    src_cls = MarketingData()

    def test_find_post_processing_scripts(self) -> list:
        """

        :return:
        """
        queries = self.src_cls.find_post_processing_scripts()
        self.assertTrue(type(queries) == list)
        self.assertGreater(len(queries), 0)
        return queries

    def test_execute_scripts(self):
        result = self.src_cls.execute_scripts(
            scripts=self.src_cls.find_post_processing_scripts()
        )
        self.assertTrue(result)
