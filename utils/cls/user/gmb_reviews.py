"""
GMB Reviews Customizer Module
"""

import datetime
import pandas as pd

# PLATFORM IMPORTS
from utils.cls.user.gmb import GoogleMyBusiness
from googlemybusiness.reporting.client.listing_report import GoogleMyBusinessReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-08-27'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google My Business - Reviews'


class GoogleMyBusinessReviewsCustomizer(GoogleMyBusiness):
    """
    Handles Google My Business pulling, parsing and processing
    """

    rename_map = {
        'global': {
            'create_time': 'report_date',
            'reviewer_display_name': 'reviewer',
            'star_rating': 'rating',
            'Review_Count': 'review_count',
            'Average_Rating': 'average_rating',
            'Listing_ID': 'listing_id',
            'Listing_Name': 'listing_name'
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
        self.set_attribute('audit_type', 'monthly')
        self.set_attribute('schema', {'columns': []})

    @staticmethod
    def __calculate_date_range(df: pd.DataFrame) -> (str, str):
        start_date = df['report_date'].min().split('T')[0]
        end_date = df['report_date'].max().split('T')[0]

        return start_date, end_date

    def pull(self):

        gmb_client = GoogleMyBusinessReporting(
            customizer=self
        )
        accounts = self.get_filtered_accounts(gmb_client=gmb_client)
        for account in accounts:
            # get account name using first key (account human name) to access API Name
            account_name = account['name']

            # get all listings
            listings = gmb_client.get_listings(account=account_name)

            # for each listing, get review data
            for listing in listings:
                listing_id = listing['store_code']
                report = gmb_client.get_reviews(
                    location=listing)

                self.set_customizer_secrets_dat()

                if report:
                    df = pd.DataFrame(report)
                    df = self.assign_average_rating(df=df)
                    df['Listing_ID'] = listing_id
                    df['Listing_Name'] = listing['location_name']
                    df['account_name'] = account_name
                    df['data_source'] = DATA_SOURCE
                    df['property'] = None
                    df['community'] = None
                    df.rename(columns=self.get_rename_map(account_name=account_name), inplace=True)

                    df = df[['reviewer',
                             'rating',
                             'report_date',
                             'average_rating',
                             'listing_id',
                             'listing_name',
                             'data_source',
                             'property']]

                    start_date, end_date = self.__calculate_date_range(df=df)
                    self.set_attribute('start_date', start_date)
                    self.set_attribute('end_date', end_date)

                    self.ingest_by_listing_id(listing_id=listing_id, df=df, start_date=start_date, end_date=end_date)
                else:
                    print('INFO: No data returned for ' + str(listing))

