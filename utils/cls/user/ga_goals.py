"""
Google Analytics Goals Module

NOTES:
    - is there a better way to marry self.metrics/dimensions with the workbook.json data?
    - can we store historical / historical dates in workbook.json or some other config?
    - can we store the renaming dictionary in some stored config?
"""
# PLATFORM IMPORTS
from utils.cls.user.ga import GoogleAnalytics

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-01-01'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google Analytics - Goals'


class GoogleAnalyticsGoalsCustomizer(GoogleAnalytics):
    """
    Handles Google Analytics Goals pulling, parsing and processing
    """

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('audit_type', 'monthly')
        self.set_attribute('schema', {'columns': []})

    # place custom sql here
    post_processing_sql_list = []
        
    metrics = {
        'global': [
        ]
    }

    dimensions = {
        'global': [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath'
        ]
    }

    rename_map = {
        'global': {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url'
        }
    }
