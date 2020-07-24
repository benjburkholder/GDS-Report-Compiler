"""
Moz Pro - Rankings
"""
import datetime
import logging
import traceback
import sys

from mozpy.reporting.client.pro.seo_reporting import SEOReporting
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.core import Customizer
from utils import grc
import pandas as pd
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
    BACK_FILTER_ONLY, INGEST_ONLY = grc.procedure_flag_indicator(
        refresh_indicator=refresh_indicator,
        back_filter=BACK_FILTER_ONLY,
        ingest=INGEST_ONLY
    )

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(
        script_name=SCRIPT_NAME,
        required_attributes=REQUIRED_ATTRIBUTES,
        refresh_indicator=refresh_indicator,
        expedited=DEBUG
    )

    if not INGEST_ONLY and not BACK_FILTER_ONLY:

        accounts = grc.get_required_attribute(customizer, 'pull_moz_pro_accounts')()

        if grc.get_required_attribute(customizer, 'historical'):
            start_date = grc.get_required_attribute(customizer, 'historical_start_date')
            end_date = grc.get_required_attribute(customizer, 'historical_end_date')

            date_range = pd.date_range(start=start_date, end=end_date, freq='M')

        else:
            # automated setup - last month by default
            today = datetime.date.today()

            if today.day in [2, 3, 7, 15]:
                report_date = datetime.date(today.year, today.month, 1)

            else:
                print('Nothing to run, not the proper day')
                return 0

        master_list = []
        if grc.get_required_attribute(customizer, 'historical'):
            for campaign_id in accounts:
                for date in date_range:
                    report_date = datetime.date(date.year, date.month, 1)

                    # pull report from Linkmedia360 database
                    print(f'Pulling data for: {report_date}...')
                    df = SEOReporting().get_ranking_performance(report_date=report_date, campaign_id=campaign_id['id'])

                    if df.shape[0]:

                        df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
                        df['property'] = None

                        df = grc.run_processing(
                            df=df,
                            customizer=customizer,
                            processing_stages=PROCESSING_STAGES)
                        master_list.append(df)

                    else:
                        logger.warning('No data returned for date {}.'.format(report_date))

        else:
            for campaign_id in accounts:
                # pull report from Linkmedia360 database
                df = SEOReporting().get_ranking_performance(report_date=report_date, campaign_id=campaign_id['id'])

                if df.shape[0]:

                    df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
                    df['property'] = None

                    df = grc.run_processing(
                        df=df,
                        customizer=customizer,
                        processing_stages=PROCESSING_STAGES)
                    master_list.append(df)

                else:
                    logger.warning('No data returned for date {}.'.format(report_date))

        grc.run_data_ingest_rolling_dates(
            df=pd.concat(master_list),
            customizer=customizer,
            date_col='report_date',
            table=grc.get_required_attribute(customizer, 'table'))

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
                stack_trace=traceback.format_exc(),
                engine=grc.create_application_sql_engine(customizer=Customizer)
            )
        raise
