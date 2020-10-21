"""
Google Analytics Ecommerce Module
"""
# PLATFORM IMPORTS
from utils.cls.user.ga import GoogleAnalytics

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-08-27'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google Analytics - Ecommerce'


class GoogleAnalyticsEcommerceCustomizer(GoogleAnalytics):
    """
    Handles Google Analytics Ecommerce pulling, parsing and processing
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

        ]
    }

    dimensions = {
        'global': [
            'date',
            'channelGrouping',
            'sourceMedium',
            'deviceCategory',
            'pagePath',
            'transactionId',
            'productName',
            'productBrand'
        ]
    }

    rename_map = {
        'global': {
            'date': 'report_date',
            'sourceMedium': 'source_medium',
            'channelGrouping': 'medium',
            'deviceCategory': 'device',
            'pagePath': 'url',
            'transactionId': 'transaction_id',
            'productName': 'product_name',
            'productBrand': 'product_brand'
        }
    }
