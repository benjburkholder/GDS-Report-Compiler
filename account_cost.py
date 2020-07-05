"""
Account - Cost
"""
import datetime
import logging
import sys

from utils.email_manager import EmailClient
from utils.cls.core import Customizer
from utils import grc

SCRIPT_NAME = grc.get_script_name(__file__)

DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

# Toggle for manually running ingest and backfilter procedures
INGEST_ONLY = False

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
    # 'post_processing'
]

REQUIRED_ATTRIBUTES = [
    # 'historical',
    # 'historical_start_date',
    # 'historical_end_date',
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

    if not INGEST_ONLY:

        # Pull all account cost data
        cost_data = grc.get_required_attribute(customizer, 'pull_account_cost')()
        df = grc.get_required_attribute(customizer, 'get_account_cost_meta_data')(cost_data)

        if df.shape[0]:
            df['data_source'] = grc.get_required_attribute(customizer, 'data_source')

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

            grc.ingest_procedures(customizer=customizer)

        else:
            logger.warning('No Account Cost data.')

    else:
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
