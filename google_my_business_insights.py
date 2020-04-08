"""
Google My Business - Insights
"""
import logging
import datetime
import pandas as pd

from utils import grc
from googlemybusiness.reporting.client.listing_report import GoogleMyBusinessReporting
SCRIPT_NAME = grc.get_script_name(__file__)
DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
    'post_processing'
]

REQUIRED_ATTRIBUTES = [
    'historical',
    'historical_start_date',
    'historical_end_date',
    'table'
]
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def main() -> int:
    # run startup global checks
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=PROCESSING_STAGES, label='PROCESSING_STAGES')
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=REQUIRED_ATTRIBUTES, label='REQUIRED_ATTRIBUTES')

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES, expedited=DEBUG)

    if grc.get_required_attribute(customizer, 'historical'):
        start_date = grc.get_required_attribute(customizer, 'historical_start_date')
        end_date = grc.get_required_attribute(customizer, 'historical_end_date')

    else:
        # automated setup - last week by default
        start_date = (datetime.datetime.today() - datetime.timedelta(540)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    gmb_client = GoogleMyBusinessReporting(
            secrets_path=grc.get_required_attribute(customizer, 'secrets_path')
            )

    accounts = grc.get_required_attribute(customizer, 'get_gmb_accounts')()
    gmb_accounts = gmb_client.get_gmb_accounts()

    filtered_accounts = [account for account in gmb_accounts
                         if list(account.keys())[0] in accounts]

    print(filtered_accounts)

    df_list = []

    for account in filtered_accounts:
        # get account name using first key (account human name) to access API Name
        account_name = account[list(account.keys())[0]]['API_Name']

        # get all listings
        listings = gmb_client.get_gmb_listings(account=account_name)

        # for each listing, get insight data
        for listing in listings:
            report = gmb_client.get_gmb_insights(
                start_date=start_date,
                end_date=end_date,
                account=account_name,
                location_name=listing['name'])

            if report:
                df = pd.DataFrame(report)
                df['Listing_ID'] = int(listing['storeCode']) \
                    if str(listing['storeCode']).isdigit() else str(listing['storeCode'])
                df['Listing_Name'] = listing['locationName']
                df_list.append(df)

        if df_list:
            df = pd.concat(df_list)
            df = grc.run_processing(
                df=df,
                customizer=customizer,
                processing_stages=PROCESSING_STAGES
            )
            grc.run_data_ingest_rolling_dates(
                df=df,
                customizer=customizer,
                date_col='report_date',
                table=grc.get_required_attribute(customizer, 'table')
            )
            grc.table_backfilter(customizer=customizer)
        else:
            logger.warning('No data returned for dates {} - {}'.format(start_date, end_date))
    return 0


if __name__ == '__main__':
    main()
