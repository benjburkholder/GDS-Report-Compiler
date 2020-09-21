"""
Account Cost Customizer Module
"""

import datetime

# PLATFORM IMPORTS
from utils.cls.user.account import AccountCost

# CUSTOM IMPORTS
IS_CLASS = True
DATA_SOURCE = 'Account - Cost'


class AccountCostCustomizer(AccountCost):
    """
    Handles Account Cost pulling, parsing and processing
    """

    rename_map = {
        'global': {

            'Date': 'report_date',
            'Property': 'property',
            'Medium': 'medium',
            'Daily_Cost': 'daily_cost'

        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})

    def pull(self):

        # pull account cost data from configuration sheet
        cost_data = self.pull_account_cost()
        df = self.get_account_cost_meta_data(cost_data=cost_data)

        if df.shape[0]:
            df = self.type(df=df)
            df.rename(columns=self.rename_map['global'], inplace=True)
            df['data_source'] = DATA_SOURCE
            df['daily_cost'] = df['daily_cost'].astype(float)
            df['daily_cost'] = df['daily_cost'].apply(lambda x: round(x, 2))
            self.ingest_all(
                df=df
            )

        else:
            print('INFO: No Account Cost data returned.')

