"""
Google Search Console - Analytics
"""
import pandas as pd
import datetime
import calendar
import logging
import sys

from googlesearchconsole.reporting.client.search_analytics import SearchAnalytics
from utils.email_manager import EmailClient
from utils.cls.core import Customizer
from utils import grc

SCRIPT_NAME = grc.get_script_name(__file__)

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
    # 'historical_start_date',
    # 'historical_end_date',
    'historical_report_date',
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
            report_date = grc.get_required_attribute(customizer, 'historical_report_date')

        else:
            # automated setup - last week by default
            today = datetime.date.today()

            if today.day in [2, 5, 15]:
                if today.month == 1:
                    last_month = 12
                    last_month_year = (today.year - 1)
                else:
                    last_month = (today.month - 1)
                    last_month_year = today.year

                report_date = datetime.date(
                    last_month_year,
                    last_month,
                    calendar.monthrange(last_month_year, last_month)[1])

        df_list = []
        for property_url in grc.get_required_attribute(customizer, 'get_property_urls')():
            print(property_url)
            df = SearchAnalytics().get_monthly_search_analytics(
                report_date=report_date, property_url=property_url)
            df_list.append(df)

        if df_list:
            df = pd.concat(df_list)

            df['data_source'] = grc.get_required_attribute(customizer, 'data_source')
            df['property'] = None

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
            logger.warning('No data returned for dates - {}'.format(report_date))

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
