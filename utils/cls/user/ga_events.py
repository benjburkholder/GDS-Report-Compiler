"""
Google Analytics Traffic Module
"""
# PLATFORM IMPORTS
from utils.cls.user.ga import GoogleAnalytics

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-01-01'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google Analytics - Events'


class GoogleAnalyticsEventsCustomizer(GoogleAnalytics):
    """
    Handles Google Analytics Events pulling, parsing and processing
    """

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('table', self.prefix)
        self.set_attribute('metrics', self.metrics)
        self.set_attribute('dimensions', self.dimensions)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})

    # place custom sql here
    post_processing_sql_list = []
        
    metrics = {
        'global': [
            'totalEvents',
            'uniqueEvents',
            'eventValue'
        ]
    }

    dimensions = {
        'global': [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'campaign',
            'pagePath',
            'eventLabel',
            'eventAction',
        ]
    }

    rename_map = {
        'global': {
            'date': 'report_date',
            'channelGrouping': 'medium',
            'sourceMedium': 'source_medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'eventLabel': 'event_label',
            'eventAction': 'event_action',
            'totalEvents': 'total_events',
            'uniqueEvents': 'unique_events',
            'eventValue': 'event_value'
        }
    }
