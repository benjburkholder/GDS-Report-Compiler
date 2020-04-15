"""
Google Analytics - Goals
"""
import logging
import datetime

from utils import grc
from googleanalyticspy.reporting.client.reporting import GoogleAnalytics
SCRIPT_NAME = grc.get_script_name(__file__)
DEBUG = False
if DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")

PROCESSING_STAGES = [
    'rename',
    'type',
    'parse',
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


def main() -> int:
    # run startup global checks
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=PROCESSING_STAGES, label='PROCESSING_STAGES')
    grc.run_prestart_assertion(script_name=SCRIPT_NAME, attribute=REQUIRED_ATTRIBUTES, label='REQUIRED_ATTRIBUTES')

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES, expedited=DEBUG)

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
            logger.warning('No data returned for view id {} for dates {} - {}'.format(view_id, start_date, end_date))
    return 0


if __name__ == '__main__':
    main()
