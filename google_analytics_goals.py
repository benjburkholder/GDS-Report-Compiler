"""
Google Analytics Goals
"""
import logging
import datetime
import pandas as pd

from googleanalyticspy.reporting.client.reporting import GoogleAnalytics
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)

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
    customizer = grc.setup(script_name=SCRIPT_NAME, required_attributes=REQUIRED_ATTRIBUTES)

    if getattr(customizer, f'{customizer.prefix}_historical'):
        start_date = getattr(customizer, f'{customizer.prefix}_historical_start_date')
        end_date = getattr(customizer, f'{customizer.prefix}_historical_end_date')

    else:
        # automated setup - last week by default
        start_date = (datetime.datetime.today() - datetime.timedelta(7)).strftime('%Y-%m-%d')
        end_date = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')

    for view_id in customizer.view_ids:
        df = GoogleAnalytics(client_name=customizer.client,
                             secrets_path=getattr(customizer, f'{customizer.prefix}_secrets_path')).query(
                             view_id=view_id, raw_dimensions=getattr(customizer, f'{customizer.prefix}_dimensions'),
                             raw_metrics=getattr(customizer, f'{customizer.prefix}_metrics'),
                             start_date=start_date, end_date=end_date
        )
        if df.shape[0]:
            df['view_id'] = view_id
            df = grc.run_processing(df=df, customizer=customizer, processing_stages=PROCESSING_STAGES)
            grc.run_data_ingest_rolling_dates(df=df, customizer=customizer, date_col='report_date')
            grc.table_backfilter(customizer=customizer)

        else:
            logger.warning('No data returned for view id {} for dates {} - {}'.format(view_id, start_date, end_date))
    grc.run_update_join(
        customizer=customizer,
        target_table=grc.get_required_attribute(customizer, 'table'),
        lookup_table=grc.get_required_attribute(customizer, 'lookup_table'),
        on='page_path',
        exact_match=True,
        default=grc.get_required_attribute(customizer, 'entity_defaults')
    )
    grc.run_update_join(customizer=customizer, on='page_path', exact_match=True)

    return 0


if __name__ == '__main__':
    main()
