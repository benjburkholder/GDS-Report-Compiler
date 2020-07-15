"""
Unit Test for Google Analytics (ga.py)
"""
from .test_core import TestHelper
from ..ga import (
    GoogleAnalyticsEventsCustomizer,
    GoogleAnalyticsGoalsCustomizer,
    GoogleAnalyticsTrafficCustomizer
)


class TestGoogleAnalyticsEventsCustomizer(TestHelper):
    """
    Google Analytics variant of TestHelper

    Use this class to test GA events data quality and functionality
    """
    src_cls = GoogleAnalyticsEventsCustomizer()


class TestGoogleAnalyticsGoalsCustomizer(TestHelper):
    """
    Google Analytics variant of TestHelper

    Use this class to test GA goals data quality and functionality
    """
    src_cls = GoogleAnalyticsGoalsCustomizer()


class TestGoogleAnalyticsTrafficCustomizer(TestHelper):
    """
    Google Analytics variant of TestHelper

    Use this class to test GA traffic data quality and functionality
    """
    src_cls = GoogleAnalyticsTrafficCustomizer()
