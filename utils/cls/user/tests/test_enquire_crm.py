"""
Unit Tests for Enqurie CRM (enquire_crm.py)
"""
from .test_core import TestHelper
from utils.cls.user.enquire_crm import (
    EnquireCrmActivityDeposit,
    EnquireCrmActivityInquiry,
    EnquireCrmActivityMoveIn,
    EnquireCrmActivityTour
)


class TestEnquireCrmActivityDeposit(TestHelper):
    """
    Enquire variant of TestHelper

    Use this class to test for deposit activity data quality
    """
    src_cls = EnquireCrmActivityDeposit


class TestEnquireCrmActivityInquiry(TestHelper):
    """
    Enquire variant of TestHelper

    Use this class to test for inquiry activity data quality
    """

    src_cls = EnquireCrmActivityInquiry


class TestEnquireCrmActivityMoveIn(TestHelper):
    """
    Enquire variant of TestHelper

    Use this class to test for move-in activity data quality
    """

    src_cls = EnquireCrmActivityMoveIn


class TestEnquireCrmActivityTour(TestHelper):
    """
    Enquire variant of TestHelper

    Use this class to test for tour activity data quality
    """

    src_cls = EnquireCrmActivityTour
