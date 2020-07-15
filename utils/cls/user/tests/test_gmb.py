"""
Unit Tests for Google My Business (gmb.py)
"""
from .test_core import TestHelper
from ..gmb import (
    GoogleMyBusinessInsights,
    GoogleMyBusinessReviews
)


class TestGoogleMyBusinessInsights(TestHelper):
    """
    Google My Business variant of TestHelper

    Use this class to check data / functionality of the insights report
    """
    src_cls = GoogleMyBusinessInsights()


class TestGoogleMyBusinessReviews(TestHelper):
    """
    Google My Business variant of TestHelper

    Use this class to check data / functionality of the reviews report
    """
    src_cls = GoogleMyBusinessReviews()
