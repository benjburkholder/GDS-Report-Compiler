"""
Google Analytics - Goals
"""
import datetime
import logging
import traceback
import sys

from googleanalyticspy.reporting.client.reporting import GoogleAnalytics
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.core import Customizer
from utils import grc
import pandas as pd
from utils.cls.pltfm.marketing_data import execute_post_processing_scripts_for_process


SCRIPT_NAME = grc.get_script_name(__file__)
SCRIPT_FILTER = SCRIPT_NAME.replace('.py')

DEBUG = True
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
    customizer = grc.setup(
        script_name=SCRIPT_NAME,
        required_attributes=REQUIRED_ATTRIBUTES,
        refresh_indicator=refresh_indicator,
        expedited=DEBUG
    )

    if not INGEST_ONLY and not BACK_FILTER_ONLY:

        if grc.get_required_attribute(customizer, 'historical'):
            start_date = grc.get_required_attribute(customizer, 'historical_start_date')
            end_date = grc.get_required_attribute(customizer, 'historical_end_date')

        else:
            # automated setup - last week by default
            start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
            end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

        customizer = grc.get_customizer_secrets(customizer=customizer)

        ga_client = GoogleAnalytics(
            customizer=customizer
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

            # if the ga client has secrets after the request, lets update the database with those
            # for good housekeeping
            if getattr(ga_client.customizer, 'secrets_dat', {}):
                customizer = update_credentials(customizer=customizer, ga_client=ga_client)

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

        # update credentials at script termination to ensure things are up-to-date
        update_credentials(customizer=customizer, ga_client=ga_client)
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


# TODO: could probably add this as a method of GoogleAnalytics Customizer instances
# noinspection PyUnresolvedReferences
def update_credentials(customizer: Customizer, ga_client: GoogleAnalytics) -> Customizer:
    """
    Ensure the application database has the most recent data on-file for the client
    and script / data source

    :param customizer:
    :param ga_client:
    :return:
    """
    customizer.secrets_dat = ga_client.customizer.secrets_dat
    customizer.secrets = ga_client.customizer.secrets
    grc.set_customizer_secrets_dat(customizer=customizer)
    return customizer


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
