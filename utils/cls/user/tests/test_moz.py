"""
Unit Tests for Moz Pro / Local (moz.py)
"""
from .test_core import TestHelper
from ..moz import (
    MozLocalSyncCustomizer,
    MozLocalVisibilityCustomizer,
    MozProRankingsCustomizer,
    MozProSerpCustomizer
)


class TestMozLocalSyncCustomizer(TestHelper):
    """
    Moz Local variant of Test Helper

    Use this class to check the sync report for data quality and functionality
    """
    src_cls = MozLocalSyncCustomizer()


class TestMozLocalVisibilityCustomizer(TestHelper):
    """
    Moz Pro variant of Test Helper

    Use this class to check the visibility report for data quality and functionality
    """
    src_cls = MozLocalVisibilityCustomizer()


class TestMozProRankingsCustomizer(TestHelper):
    """
    Moz Pro variant of Test Helper

    Use this class to check the rankings report for data quality and functionality
    """
    src_cls = MozProRankingsCustomizer()


class TestMozProSerpCustomizer(TestHelper):
    """
    Moz Pro variant of Test Helper

    Use this class to check the serp report for dat quality and functionality
    """
    src_cls = MozProSerpCustomizer()
