"""
Moz Pro Rankings Customizer Module
"""

import datetime

# PLATFORM IMPORTS
from utils.cls.user.moz import Moz
from mozpy.reporting.client.pro.seo_reporting import SEOReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
DATA_SOURCE = 'Moz Pro - Rankings'


class MozProRankingsCustomizer(Moz):
    """
    Handles Moz Pro pulling, parsing and processing
    """

    rename_map = {
        'global': {

        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('audit_type', 'monthly')
        self.set_attribute('schema', {'columns': []})

    def pull(self):

        date_range = self.get_date_range()
        moz_pro_accounts = self.pull_moz_pro_accounts()

        self.set_attribute('start_date', date_range.strftime('%Y-%m-%d'))
        self.set_attribute('end_date', None)

        if HISTORICAL:
            for campaign_id in moz_pro_accounts:
                for date in date_range:
                    report_date = datetime.date(date.year, date.month, 1)

                    # pull report from Linkmedia360 database
                    print(f'Pulling data for: {report_date}...')
                    df = SEOReporting().get_ranking_performance(
                        report_date=report_date,
                        campaign_id=campaign_id['campaign_id'])

                    if df.shape[0]:
                        df['data_source'] = DATA_SOURCE
                        df['property'] = None
                        df = self.type(df=df)
                        self.ingest_by_custom_indicator(
                            id_value=campaign_id['campaign_id'],
                            df=df,
                            report_date=report_date
                        )

                    else:
                        print('INFO: No data returned for ' + str(campaign_id))

        else:
            for campaign_id in moz_pro_accounts:
                # pull report from Linkmedia360 database
                df = SEOReporting().get_ranking_performance(
                    report_date=date_range,
                    campaign_id=campaign_id['campaign_id'])

                if df.shape[0]:
                    df['data_source'] = DATA_SOURCE
                    df['property'] = None
                    df['community'] = None
                    df = self.type(df=df)
                    self.ingest_by_custom_indicator(
                        id_value=campaign_id['campaign_id'],
                        df=df,
                        report_date=date_range
                    )

                else:
                    print('INFO: No data returned for ' + str(campaign_id))

