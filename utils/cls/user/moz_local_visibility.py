"""
Moz Local Visibility Customizer Module
"""

import pandas as pd

# PLATFORM IMPORTS
from utils.cls.user.moz import Moz
from mozpy.reporting.client.local.llm_reporting import LLMReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-03-12'
DATA_SOURCE = 'Moz Local - Visibility Report'


class MozLocalVisibilityCustomizer(Moz):
    """
    Handles Moz Local pulling, parsing and processing
    """

    rename_map = {
        'global': {

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

    def pull(self):

        moz_local_accounts = self.pull_moz_local_accounts()
        moz = LLMReporting(
            account_label_pairs=moz_local_accounts
        )

        df_listings = moz.get_listings()

        # pull report from Linkmedia360 database
        listing_ids = df_listings.loc[:, ['listing_id', 'account_name']].drop_duplicates().to_dict(orient='records')
        count = 1
        total = len(listing_ids)

        for listing_id in listing_ids:
            df = moz.get_visibility_report(
                listing_id=listing_id['listing_id'],
                account_name=listing_id['account_name']
            )

            if df.shape[0]:
                # add data source
                df['data_source'] = DATA_SOURCE
                df['property'] = None
                df['listing_id'] = df['listing_id'].astype(int)

                # drop duplicate data
                df.drop_duplicates(inplace=True)
                df = self.exclude_moz_directories(df=df)

                # set index
                # noinspection PyUnresolvedReferences
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
                df = self.type(df=df)
                self.ingest_by_custom_indicator(
                    id_value=str(listing_id['listing_id']),
                    df=df,
                )

                print('INFO: Ingest complete.')

            else:
                print('INFO: No data returned for listing id' + str(listing_id))

            print(listing_id, count, total)
            count += 1


