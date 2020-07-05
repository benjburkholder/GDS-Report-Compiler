"""
Google Analytics - Events
"""
import datetime
import logging
import sys

from googleanalyticspy.reporting.client.reporting import GoogleAnalytics
from utils.email_manager import EmailClient
from utils.cls.core import Customizer
from utils import grc
import pandas as pd

SCRIPT_NAME = grc.get_script_name(__file__)

DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

# Toggle for manually running ingest and backfilter procedures
INGEST_ONLY = False
BACK_FILTER_ONLY = False

PROCESSING_STAGES = [
    'rename',
    'type',
    # 'parse',
    # 'post_processing'
]

REQUIRED_ATTRIBUTES = [
    'get_view_ids',
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

        ga_client = GoogleAnalytics(
                client_name=customizer.client,
                secrets_path=grc.get_required_attribute(customizer, 'secrets_path')
                )

        master_list = []
        for view_id in grc.get_required_attribute(customizer, 'get_view_ids')():
            df = ga_client.query(
                view_id=view_id,
                raw_dimensions=grc.get_required_attribute(customizer, 'dimensions'),
                raw_metrics=grc.get_required_attribute(customizer, 'metrics'),
                start_date=start_date,
                end_date=end_date
            )

            if df.shape[0]:
                df['view_id'] = view_id
                df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
                df['property'] = None

                df = grc.run_processing(
                    df=df,
                    customizer=customizer,
                    processing_stages=PROCESSING_STAGES
                )
                master_list.append(df)

            else:
                logger.warning('No data returned for view id {} for dates {} - {}'.format(view_id, start_date, end_date))

        grc.run_data_ingest_rolling_dates(
            df=pd.concat(master_list),
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
            EmailClient().send_error_email(
                to=Customizer.recipients,
                script_name=SCRIPT_NAME,
                error=error,
                client=Customizer.client
            )
        raise
