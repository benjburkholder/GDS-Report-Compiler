"""
Unit Tests for Google Search Console (gsc.py)
"""
from .test_core import TestHelper
from ..gsc import GoogleSearchConsoleMonthly


class TestGoogleSearchConsoleMonthly(TestHelper):
    """
    Google Search Console variant of TestHelper

    Use this class to check quality and functionality of monthly data
    """
    src_cls = GoogleSearchConsoleMonthly()
