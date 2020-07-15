"""
Unit Test for Client Inquiry Web Goals (inquiry_web_goals.py)
"""
from .test_core import TestHelper
from ..inquiry_web_goals import InquiryGoals


class TestInquiryGoals(TestHelper):
    """
    Inquiry Goals variant of TestHelper

    Use this class to audit configuration / setup / functionality of client inquiry goals
    """
    src_cls = InquiryGoals()
