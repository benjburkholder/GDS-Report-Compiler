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
        self.set_attribute('schema', {'columns': []})

    # place custom sql here
    post_processing_sql_list = []
        
    metrics = {
        'global': [
            'goal1Completions',
            'goal2Completions'
        ],
        # waters edge of bradenton
        '93397410': [
            'goal1Completions',
            'goal2Completions',
            'goal3Completions'
        ],
        # waters edge of lake wales - same as bradenton
        '93397216': '93397410',
        # avondale - same as bradenton
        '122822546': '93397410',
        # broadway park only has one goal setup
        '201135214': [
            'goal1Completions'
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
            'pagePath': 'url',
            'goal1Completions': 'contact_us_form',
            'goal2Completions': 'brochure_request_form'
        },
        # lake wales - 2/1/3 configuration
        '93397216':  {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'goal1Completions': 'contact_us_form',
            'goal2Completions': 'brochure_request_form',
            'goal3Completions': 'ppc_form'
        },
        # avondale - same as lake wales
        '122822546': '93397216',
        # bradenton - 1/2/3 configuration
        '93397410':  {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'goal1Completions': 'brochure_request_form',
            'goal2Completions': 'contact_us_form',
            'goal3Completions': 'ppc_form'
        },
        # broadway park - only one goal
        '201135214': {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'goal1Completions': 'contact_us_form',
        },
    }
