"""
Google Ads Campaign Conversions Customizer Module
"""

# PLATFORM IMPORTS
import pathlib
from utils.cls.user.gads import GoogleAds
from googleadspy.reporting.client.reporting import GoogleAdsReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
DATA_SOURCE = 'Google Ads - Campaign Conversions'
CUSTOMER_ID = '9664678140'

# TODO: replace once GAds package is updated for dynamic cred retrieval
yaml_path = pathlib.Path('secrets')


class GoogleAdsCampaignConversionsCustomizer(GoogleAds):
    """
    Handles GAds pulling, parsing and processing
    """

    rename_map = {
        'global': {

            'Date': 'report_date',
            'Campaign': 'campaign',
            'Campaign_ID': 'campaign_id',
            'Conversion_Name': 'goal_name',
            'Conversions': 'goal_completions',
            'Conversions_All': 'all_goal_completions',
            'Account_ID': 'account_id',
            'Device': 'device',
            'Medium': 'advertising_channel_type',
            'Market': 'property',
            'Store': 'community',
            'Data_Source': 'data_source'

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

        # TODO: replace once GAds package is updated for dynamic cred retrieval
        gads = GoogleAdsReporting(customer_id=CUSTOMER_ID, yaml_path=yaml_path)
        account_ids = self.get_account_ids()

        for account_id in account_ids:
            df = gads.campaign_conversions_performance(
                customer_id=account_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.shape[0]:
                df['data_source'] = DATA_SOURCE
                df['property'] = None
                df['account_id'] = account_id
                df = self.type(df=df)
                rename_map = self.get_rename_map(account_id=account_id)
                df.rename(columns=rename_map, inplace=True)

                self.ingest_by_account_id(
                    df=df,
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date
                )

            else:
                print('INFO: No data returned for ' + str(account_id))

