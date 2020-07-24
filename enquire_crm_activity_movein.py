"""
Enquire CRM - Movein
"""
import datetime
import logging
import sys
import traceback
from pathlib import Path

import pandas as pd
from googleadspy.reporting.client.reporting import GoogleAdsReporting

from utils import grc
from utils.cls.core import Customizer
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.pltfm.marketing_data import execute_post_processing_scripts_for_process


SCRIPT_NAME = grc.get_script_name(__file__)
yaml_path = Path('secrets')

DEBUG = True
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

# Toggle for manually running ingest and backfilter procedures
INGEST_ONLY = True
BACK_FILTER_ONLY = False

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


def main(refresh_indicator) -> int:
    # run startup global checks
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=PROCESSING_STAGES, label='PROCESSING_STAGES')
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=REQUIRED_ATTRIBUTES, label='REQUIRED_ATTRIBUTES')

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES, refresh_indicator=refresh_indicator, expedited=DEBUG)

    if not INGEST_ONLY and not BACK_FILTER_ONLY:

        if grc.get_required_attribute(customizer, 'historical'):
            start_date = grc.get_required_attribute(customizer, 'historical_start_date')
            end_date = grc.get_required_attribute(customizer, 'historical_end_date')

        else:
            # automated setup - last week by default
            start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
            end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

        gads = GoogleAdsReporting(yaml_path=yaml_path, customer_id='9664678140')

        df_list = []
        for account_id in grc.get_required_attribute(customizer, 'get_account_ids')():
            df = gads.campaign_performance(
                customer_id=account_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.shape[0]:
                df['account_id'] = account_id
                df_list.append(df)

        df = pd.concat(df_list)

        if df.shape[0]:
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
            # grc.table_backfilter(customizer=customizer)
            grc.ingest_procedures(customizer=customizer)
            grc.audit_automation(customizer=customizer)

        else:
            logger.warning('No data returned for dates {} - {}'.format(start_date, end_date))

    else:
        if BACK_FILTER_ONLY:
            print('Running manual backfilter...')
            grc.table_backfilter(customizer=customizer)

        if INGEST_ONLY:
            print('Running manual ingest...')
            # Executes post_processing stage after all data is pulled and ingested

            grc.run_post_processing(customizer=customizer, processing_stages=PROCESSING_STAGES)
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
                stack_trace=traceback.format_exc()
            )
        raise
