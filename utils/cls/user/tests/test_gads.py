"""
Unit Tests for Google Ads (gads.py)
"""
from .test_core import TestHelper
from ..gads import (
    GoogleAdsCampaign,
    GoogleAdsKeyword
)


class TestGoogleAdsCampaign(TestHelper):
    """
    Google Ads variant of TestHelper

    Use this class to test campaign data for quality / mapping / functionality
    """

    src_cls = GoogleAdsCampaign()


class TestGoogleAdsKeyword(TestHelper):
    """
    Google Ads variant of TestHelper

    Use this class to test campaign (keyword) data for quality / mapping / functionality
    """
    src_cls = GoogleAdsKeyword()
