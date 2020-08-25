"""
Google Ads Keyword Performance Customizer Module
"""
# PLATFORM IMPORTS
from utils.cls.user.google_ads import GoogleAds

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
DATA_SOURCE = 'Google Ads - Keyword Performance'


class GoogleAdsKeywordCustomizer(GoogleAds):
    """
    Handles GAds pulling, parsing and processing
    """

    rename_map = {
        'global': {
            'date': 'report_date',
            'campaign_name': 'campaign',
            'campaign_id': 'campaign_id',
        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

    def pull(self):

        start_date = self.calculate_date(start_date=True)
        end_date = self.calculate_date(start_date=False)

        account_pairs = self.get_account_ids()

        for pair in account_pairs:
            manager_account_id = pair['manager_account_id']
            account_id = pair['account_id']
            client = self.build_client(manager_customer_id=manager_account_id)
            df = client.keyword_performance(
                start_date=start_date,
                end_date=end_date,
                customer_id=account_id
            )

            if df.shape[0]:
                df['data_source'] = DATA_SOURCE
                df['property'] = None
                df['account_id'] = account_id
                df = self.type(df=df)
                rename_map = self.get_rename_map(account_id=account_id)
                df.rename(columns=rename_map, inplace=True)
                df.drop(columns=['match_type', 'conversions'], inplace=True)
                df = self.__parse_medium(df=df)
                df = self.__parse_device(df=df)

                self.ingest_by_account_id(
                    df=df,
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date
                )

            else:
                print('INFO: No data returned for ' + str(account_id))

    @staticmethod
    def __parse_medium(df):
        """
            Parse medium based on ad network type
            """
        medium_map = {
            'UNSPECIFIED': 'Paid Search',
            'UNKNOWN': 'Paid Search',
            'SEARCH': 'Paid Search',
            'SEARCH_PARTNERS': 'Paid Search',
            'CONTENT': 'Display',
            'YOUTUBE_SEARCH': 'Youtube',
            'YOUTUBE_WATCH': 'Youtube',
            'MIXED': 'Cross-Network'
        }
        df['advertising_channel_type'] = df['advertising_channel_type'].map(medium_map)
        return df

    @staticmethod
    def __parse_device(df):
        """
            Parse device into normalized format
            """
        print(df['device'].unique())
        df['device'][df['device'] == 'DESKTOP'] = 'Desktop'
        df['device'][df['device'] == 'MOBILE'] = 'Mobile'
        df['device'][df['device'] == 'TABLET'] = 'Tablet'
        df['device'][df['device'] == 'CONNECTED_TV'] = 'Tv'
        df['device'][df['device'] == 'UNSPECIFIED'] = 'Desktop'
        df['device'][df['device'] == 'UNKNOWN'] = 'Desktop'
        df['device'][df['device'] == 'OTHER'] = 'Desktop'
        return df
