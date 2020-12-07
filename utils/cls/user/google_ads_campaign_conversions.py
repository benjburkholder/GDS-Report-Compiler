"""
Google Ads Campaign Conversions Customizer Module
"""

import datetime

# PLATFORM IMPORTS
from utils.cls.user.google_ads import GoogleAds

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
DATA_SOURCE = 'Google Ads - Campaign Conversions'


class GoogleAdsCampaignConversionsCustomizer(GoogleAds):
    """
    Handles GAds pulling, parsing and processing
    """

    rename_map = {
        'global': {

            'date': 'report_date',
            'campaign_name': 'campaign',
            'campaign_id': 'campaign_id',
            'conversion_name': 'goal_name',
            'conversions': 'goal_completions',
            'all_conversions': 'all_goal_completions'
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
        self.set_attribute('audit_type', 'weekly')
        self.set_attribute('schema', {'columns': []})

    def pull(self):

        start_date = self.calculate_date(start_date=True).strftime('%Y-%m-%d')
        end_date = self.calculate_date(start_date=False).strftime('%Y-%m-%d')

        # Assign rolling date range to customizer
        self.set_attribute('start_date', start_date)
        self.set_attribute('end_date', end_date)

        account_pairs = self.get_account_ids()

        for pair in account_pairs:
            manager_account_id = pair['manager_account_id']
            account_id = pair['account_id']
            client = self.build_client(manager_customer_id=manager_account_id)
            df = client.campaign_conversions_performance(
                start_date=start_date,
                end_date=end_date,
                customer_id=account_id
            )

            if df.shape[0]:
                df['data_source'] = DATA_SOURCE
                df['property'] = None
                df['community'] = None
                df['account_id'] = account_id
                df = self.type(df=df)
                rename_map = self.get_rename_map(account_id=account_id)
                df.rename(columns=rename_map, inplace=True)

                # remove unnecessary column from standardized report
                if 'conversion_action' in df.columns:
                    del df['conversion_action']

                self.ingest_by_account_id(
                    df=df,
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date
                )

            else:
                print('INFO: No data returned for ' + str(account_id))
