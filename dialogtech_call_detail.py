"""
DialogTech - Call Details
"""
import datetime
import logging
import sys
import traceback

import pandas as pd
from dialogtech.reporting.client.call_detail import CallDetailReporting

from utils import grc
from utils.cls.core import Customizer
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.pltfm.marketing_data import execute_post_processing_scripts_for_process

SCRIPT_NAME = grc.get_script_name(__file__)
SCRIPT_FILTER = SCRIPT_NAME.replace('.py')

DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

# Toggle for manually running ingest and backfilter procedures
INGEST_ONLY = False
BACK_FILTER_ONLY = False

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
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

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES,
                           refresh_indicator=refresh_indicator, expedited=DEBUG)

    if not INGEST_ONLY and not BACK_FILTER_ONLY:

        if grc.get_required_attribute(customizer, 'historical'):
            start_date = grc.get_required_attribute(customizer, 'historical_start_date')
            end_date = grc.get_required_attribute(customizer, 'historical_end_date')

        else:
            # automated setup - last week by default
            start_date = (datetime.datetime.today() - datetime.timedelta(540))
            end_date = (datetime.datetime.today() - datetime.timedelta(1))

        dialog_tech = CallDetailReporting(vertical='<ENTER VERTICAL>')

        df_list = []
        for phone_label in grc.get_required_attribute(customizer, 'pull_dialogtech_labels')():
            df = dialog_tech.get_call_detail_report(
                start_date=start_date,
                end_date=end_date,
                phone_label=phone_label[0]
            )
            if df.shape[0]:
                df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
                df['property'] = None
                df['medium'] = phone_label[1]
                df_list.append(df)

        if len(df_list):
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

            # Executes post_processing stage after all data is pulled and ingested
            grc.run_post_processing(customizer=customizer, processing_stages=PROCESSING_STAGES)

            grc.table_backfilter(customizer=customizer)
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
            grc.ingest_procedures(customizer=customizer)

    # find post processing SQL scripts with this file's name as a search key and execute
    execute_post_processing_scripts_for_process(
        script_filter=SCRIPT_FILTER
    )

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
