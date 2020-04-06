"""
Moz Local - Sync Report
"""
import logging
import datetime
import pandas as pd

from mozpy.reporting.client.local.llm_reporting import LLMReporting
from utils.cls.user.moz import Moz
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)

PROCESSING_STAGES = [
    # 'rename',
    'type',
    'parse',
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
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES)

    accounts = grc.get_required_attribute(customizer, 'pull_moz_local_accounts')()

    if grc.get_required_attribute(customizer, 'historical'):
        start_date = grc.get_required_attribute(customizer, 'historical_start_date')
        end_date = grc.get_required_attribute(customizer, 'historical_end_date')

    else:
        # automated setup - last week by default
        start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    for account in accounts:
        df_listings = LLMReporting().get_listings(account_label=account['account'], client_label=account['label'])

        # pull report from Linkmedia360 database
        listing_ids = list(df_listings['listing_id'].unique())
        for listing_id in listing_ids:
            df = LLMReporting().get_sync_report(
                listing_id=listing_id,
                start_date=datetime.datetime.strptime(start_date, '%Y-%m-%d'),
                end_date=datetime.datetime.strptime(end_date, '%Y-%m-%d')
            )

        if df.shape[0]:
            df = grc.get_required_attribute(customizer, 'exclude_moz_directories')(df)
            df = grc.run_processing(
                 df=df,
                 customizer=customizer,
                 processing_stages=PROCESSING_STAGES)

            grc.run_data_ingest_rolling_dates(
                df=df,
                customizer=customizer,
                date_col='report_date',
                table=grc.get_required_attribute(customizer, 'table')
            )

            grc.table_backfilter(
                customizer=customizer,
                calling_script=SCRIPT_NAME)

        else:
            logger.warning('No data returned for dates {} - {}'.format(start_date, end_date))

    return 0


if __name__ == '__main__':
    main()