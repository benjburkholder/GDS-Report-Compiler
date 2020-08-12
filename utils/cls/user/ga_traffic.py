"""
Google Analytics Traffic Module
"""
# PLATFORM IMPORTS
from utils.cls.user.ga import GoogleAnalytics

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = True
HISTORICAL_START_DATE = '2019-08-27'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google Analytics - Traffic'


class GoogleAnalyticsTrafficCustomizer(GoogleAnalytics):
    """
    Handles Google Analytisc Traffic pulling, parsing and processing
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

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

    # place custom sql here
    post_processing_sql_list = []
        
    metrics = {
        'global': [
            'sessions',
            'percentNewSessions',
            'pageviews',
            'uniquePageviews',
            'pageviewsPerSession',
            'entrances',
            'bounces',
            'sessionDuration',
            'users',
            'newUsers'
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
        ]
    }

    rename_map = {
        'global': {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'percentNewSessions': 'percent_new_sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'sessionDuration': 'session_duration',
            'newUsers': 'new_users'
        }
    }
