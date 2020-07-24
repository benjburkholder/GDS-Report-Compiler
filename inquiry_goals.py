"""
Goals - Web Inquiries
"""
import traceback
import logging
import sys

from utils.gs_manager import GoogleSheetsManager
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.core import Customizer
from utils import grc
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
    'post_processing'
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

    if not INGEST_ONLY:

        df = GoogleSheetsManager(
            customizer.client
        ).get_spreadsheet_by_name(
            workbook_name=customizer.configuration_workbook['config_sheet_name'],
            worksheet_name='Inquiry Goals'
        )

        if df.shape[0]:
            df = grc.run_processing(
                df=df,
                customizer=customizer,
                processing_stages=PROCESSING_STAGES
            )

            df['data_source'] = grc.get_required_attribute(customizer, 'data_source')

            grc.run_data_ingest_rolling_dates(
                df=df,
                customizer=customizer,
                date_col='report_date',
                table=grc.get_required_attribute(customizer, 'table')
            )
            grc.ingest_procedures(customizer=customizer)

        else:
            logger.warning('No Web Goals data.')

    else:
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
