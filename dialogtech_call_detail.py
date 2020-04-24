"""
DialogTech - Call Details
"""
import pandas as pd
import datetime
import calendar
import logging
import sys

from dialogtech.reporting.client.call_detail import CallDetailReporting
from utils.email_manager import EmailClient
from utils.cls.core import Customizer
from utils import grc

SCRIPT_NAME = grc.get_script_name(__file__)

DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

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
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES,
                           refresh_indicator=refresh_indicator, expedited=DEBUG)

    if grc.get_required_attribute(customizer, 'historical'):
        start_date = grc.get_required_attribute(customizer, 'historical_start_date')
        end_date = grc.get_required_attribute(customizer, 'historical_end_date')

    else:
        # automated setup - last week by default
        start_date = (datetime.datetime.today() - datetime.timedelta(540)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    dialog_tech = CallDetailReporting(vertical='')

    df_list = []
    for phone_label in grc.get_required_attribute(customizer, 'pull_dialogtech_labels')():
        df = dialog_tech.get_call_detail_report(
            start_date=start_date,
            end_date=end_date,
            phone_label=phone_label['Label']
        )
        if df.shape[0]:
            df['medium'] = phone_label['Medium']
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
        grc.table_backfilter(customizer=customizer)
        grc.ingest_procedures(customizer=customizer)
        grc.audit_automation(customizer=customizer)

    else:
        logger.warning('No data returned for dates {} - {}'.format(start_date, end_date))
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
