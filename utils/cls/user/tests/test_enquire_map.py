"""
Unit Tests for Enquire MAP (enquire_map.py)
"""
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
