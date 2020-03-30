"""
Moz Pro - Rankings
"""
import logging
import datetime
import pandas as pd

from mozpy.reporting.client.pro.seo_reporting import SEOReporting
from utils.cls.user.moz import Moz
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
]

REQUIRED_ATTRIBUTES = [
    'historical',
    'historical_report_date',
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

    grc.refresh_source_tables(customizer=customizer)
    accounts = Moz().pull_moz_pro_accounts(customizer)

    if getattr(customizer, f'{customizer.prefix}_historical'):
        report_date = getattr(customizer, f'{customizer.prefix}_historical_report_date')

    else:
        # automated setup - last month by default
        today = datetime.date.today()
        report_date = datetime.date(today.year, today.month, 1)

    for campaign_id in accounts:
        # pull report from Linkmedia360 database
        df = SEOReporting().get_ranking_performance(report_date=report_date, campaign_id=campaign_id['campaign_id'])

        if df.shape[0]:
            df['data_source'] = 'Moz Pro - Rankings'
            df = grc.run_processing(df=df, customizer=customizer, processing_stages=PROCESSING_STAGES)
            grc.run_data_ingest_rolling_dates(df=df, customizer=customizer, date_col='report_date', table='moz_pro_rankings')
            grc.table_backfilter(customizer=customizer, calling_script=SCRIPT_NAME)

        else:
            logger.warning('No data returned for date {}.'.format(report_date))

    return 0


if __name__ == '__main__':
    main()