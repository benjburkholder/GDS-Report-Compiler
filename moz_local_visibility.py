"""
Moz Local Visibility Report
"""
import logging
import datetime
import pandas as pd

from mozpy.reporting.client.local.llm_reporting import LLMReporting
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
]