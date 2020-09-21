"""
Google Search Console Analytics Module
"""

# PLATFORM IMPORTS
from utils.cls.user.gsc import GoogleSearchConsole

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-03-01'
HISTORICAL_END_DATE = '2019-03-01'
DATA_SOURCE = 'Google Search Console - Analytics'


class GoogleSearchConsoleAnalyticsCustomizer(GoogleSearchConsole):

    rename_map = {
        'global': {
            # 'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            # 'page': 'url',
            'percentNewSessions': 'percent_new_sessions',
            'percentNewPageviews': 'percent_new_pageviews',
            'uniquePageviews': 'unique_pageviews',
            'pageviewsPerSession': 'pageviews_per_session',
            'sessionDuration': 'session_duration',
            'newUsers': 'new_users'
        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', self.transform_historical_date(HISTORICAL_START_DATE))
        self.set_attribute('historical_end_date', self.transform_historical_date(HISTORICAL_END_DATE))
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})


