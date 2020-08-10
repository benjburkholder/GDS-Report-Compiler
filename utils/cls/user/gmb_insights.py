"""
GMB Insights Customizer Module
"""

import pandas as pd

# PLATFORM IMPORTS
from utils.cls.user.gmb import GoogleMyBusiness
from googlemybusiness.reporting.client.listing_report import GoogleMyBusinessReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2019-08-27'
HISTORICAL_END_DATE = '2020-07-28'
DATA_SOURCE = 'Google My Business - Insights'


class GoogleMyBusinessInsightsCustomizer(GoogleMyBusiness):
    """
    Handles Google My Business pulling, parsing and processing
    """

    rename_map = {
        'global': {
            'Date': 'report_date',
            'Listing_ID': 'listing_id',
            'Listing_Name': 'listing_name',
            'VIEWS_MAPS': 'maps_views',
            'VIEWS_SEARCH': 'search_views',
            'ACTIONS_WEBSITE': 'website_click_actions',
            'ACTIONS_PHONE': 'phone_call_actions',
            'ACTIONS_DRIVING_DIRECTIONS': 'driving_direction_actions',
            'PHOTOS_VIEWS_CUSTOMERS': 'photo_views_customers',
            'PHOTOS_VIEWS_MERCHANT': 'photo_views_merchant',
            'QUERIES_CHAIN': 'branded_searches',
            'QUERIES_DIRECT': 'direct_searches',
            'QUERIES_INDIRECT': 'discovery_searches',
            'LOCAL_POST_VIEWS_SEARCH': 'post_views_on_search',
            # 'LOCAL_POST_ACTIONS_CALL_TO_ACTION': 'post_cta_actions'
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

        start_date = self.calculate_date(start_date=True).strftime('%Y-%m-%d')
        end_date = self.calculate_date(start_date=False).strftime('%Y-%m-%d')

        # TODO: update gmb library to pass a customizer with credentials

        gmb_client = GoogleMyBusinessReporting(
            secrets_path=self.get_attribute('secrets_path')
        )
        accounts = self.get_filtered_accounts(gmb_client=gmb_client)
        for account in accounts:
            # get account name using first key (account human name) to access API Name
            account_name = account['name']

            # get all listings
            listings = gmb_client.get_listings(account=account_name)

            # for each listing, get insight data
            for listing in listings:
                listing_id = listing['store_code']
                report = gmb_client.get_insights(
                    start_date=start_date,
                    end_date=end_date,
                    account=account,
                    location=listing)

                if report:
                    df = pd.DataFrame(report)
                    df['Listing_ID'] = listing_id
                    df['Listing_Name'] = listing['location_name']
                    df['account_name'] = account_name
                    df['data_source'] = self.get_attribute('data_source')
                    df['property'] = None
                    df.rename(columns=self.get_rename_map(account_name=account_name), inplace=True)
                    df = self.type(df=df)
                    df['photo_views'] = (df['photo_views_customers'] + df['photo_views_merchant'])
                    df = df[['report_date', 'data_source', 'property', 'listing_name', 'listing_id', 'maps_views', 'search_views',
                             'website_click_actions', 'phone_call_actions', 'driving_direction_actions', 'photo_views', 'branded_searches',
                             'direct_searches', 'direct_searches', 'discovery_searches', 'post_views_on_search']]
                    self.ingest_by_listing_id(listing_id=listing_id, df=df, start_date=start_date, end_date=end_date)
                else:
                    print('INFO: No data returned for ' + str(listing))

