"""
Moz Local - Sync Report
"""
import pandas as pd
import datetime
import traceback
import logging
import sys

from mozpy.reporting.client.local.llm_reporting import LLMReporting
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.core import Customizer
from utils import grc

SCRIPT_NAME = grc.get_script_name(__file__)

DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

# Toggle for manually running ingest and backfilter procedures
INGEST_ONLY = False
BACK_FILTER_ONLY = False

PROCESSING_STAGES = [
    # 'rename',
    'type',
    # 'parse',
    # 'post_processing'
]

REQUIRED_ATTRIBUTES = [
    'historical',
    'historical_start_date',
    'historical_end_date',
    'table'
]
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def main(refresh_indicator) -> int:
    # run startup global checks
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=PROCESSING_STAGES, label='PROCESSING_STAGES')
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=REQUIRED_ATTRIBUTES, label='REQUIRED_ATTRIBUTES')

    global BACK_FILTER_ONLY, INGEST_ONLY
    BACK_FILTER_ONLY, INGEST_ONLY = grc.procedure_flag_indicator(refresh_indicator=refresh_indicator, back_filter=BACK_FILTER_ONLY, ingest=INGEST_ONLY)

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES, refresh_indicator=refresh_indicator, expedited=DEBUG)

    if not INGEST_ONLY and not BACK_FILTER_ONLY:

        accounts = grc.get_required_attribute(customizer, 'pull_moz_local_accounts')()

        if grc.get_required_attribute(customizer, 'historical'):
            start_date = grc.get_required_attribute(customizer, 'historical_start_date')
            end_date = grc.get_required_attribute(customizer, 'historical_end_date')

        else:
            # automated setup - last week by default
            start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
            end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

        moz = LLMReporting(account_label_pairs=accounts)
        df_listings = moz.get_listings()

        # pull report from Linkmedia360 database
        listing_ids = df_listings.loc[:, ['listing_id', 'account_name']].drop_duplicates().to_dict(orient='records')
        count = 1
        total = len(listing_ids)
        for listing_id in listing_ids:
            df = moz.get_visibility_report(listing_id=listing_id['listing_id'], account_name=listing_id['account_name'])

            if df.shape[0]:
                # add data source
                df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
                df['property'] = None

                df['listing_id'] = df['listing_id'].astype(int)

                # drop duplicate data
                df.drop_duplicates(inplace=True)

                df = grc.get_required_attribute(customizer, 'exclude_moz_directories')(df)

                df = grc.run_processing(
                    df=df,
                    customizer=customizer,
                    processing_stages=PROCESSING_STAGES)

                # set index
                # noinspection PyUnresolvedReferences
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
                df.set_index(df['report_date'], drop=True, inplace=True)
                del df['report_date']

                grc.get_required_attribute(customizer, 'clear_moz_local')(listing_id)
                grc.get_required_attribute(customizer, 'push_moz_local')(df)

                print('INFO: Ingest complete.')

            else:
                logger.warning('No data returned for dates {} - {}'.format(start_date, end_date))

            print(listing_id, count, total)
            count += 1

        # Executes post_processing stage after all data is pulled and ingested
        grc.run_post_processing(customizer=customizer, processing_stages=PROCESSING_STAGES)

        grc.table_backfilter(customizer=customizer)
        grc.ingest_procedures(customizer=customizer)
        grc.audit_automation(customizer=customizer)

    else:
        if BACK_FILTER_ONLY:
            print('Running manual backfilter...')
            grc.table_backfilter(customizer=customizer)

        if INGEST_ONLY:
            print('Running manual ingest...')
            grc.ingest_procedures(customizer=customizer)

    return 0


if __name__ == '__main__':
    try:
        main(refresh_indicator=sys.argv)
    except Exception as error:
        if not DEBUG:
            send_error_email(
                client_name=Customizer.client,
                script_name=SCRIPT_NAME,
                to=Customizer.recipients,
                error=error,
                stack_trace=traceback.format_exc(),
                engine=grc.create_application_sql_engine()
            )
        raise
