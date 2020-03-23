"""
Moz Local Sync Report
"""
import logging
import datetime
import pandas as pd

from mozpy.reporting.client.local.llm_reporting import LLMReporting
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
]

REQUIRED_ATTRIBUTES = [
    'account_label_pairs',
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

    if getattr(customizer, f'{customizer.prefix}_historical'):
        start_date = getattr(customizer, f'{customizer.prefix}_historical_start_date')
        end_date = getattr(customizer, f'{customizer.prefix}_historical_end_date')

    else:
        # automated setup - last week by default
        start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    for account in getattr(customizer, f'{customizer.prefix}_account_label_pairs')['account_label_pairs']:
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
            grc.refresh_source_tables(customizer=customizer)
            df = grc.run_processing(df=df, customizer=customizer, processing_stages=PROCESSING_STAGES)
            grc.run_data_ingest_rolling_dates(df=df, customizer=customizer, date_col='report_date')
            grc.table_backfilter(customizer=customizer)

        else:
            logger.warning('No data returned for view id {} for dates {} - {}'.format(start_date, end_date))
    grc.run_update_join(
        customizer=customizer,
        target_table=grc.get_required_attribute(customizer, 'table'),
        lookup_table=grc.get_required_attribute(customizer, 'lookup_table'),
        on='page_path',
        exact_match=True,
        default=grc.get_required_attribute(customizer, 'entity_defaults')
    )
    grc.run_update_join(customizer=customizer, on='page_path', exact_match=True)

    return 0


if __name__ == '__main__':
    main()