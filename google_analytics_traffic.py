"""
Google Analytics Traffic
"""
import logging
import datetime
import pandas as pd

from googleanalyticspy.reporting.client.reporting import GoogleAnalytics
from utils.custom import GoogleAnalyticsTrafficCustomizer
from utils import custom, grc
SCRIPT_NAME = grc.get_script_name(__file__)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def main() -> int:
    # Initiate instances of data source classes
    customizer = custom.get_customizer(SCRIPT_NAME)
    ga_customizer = GoogleAnalyticsTrafficCustomizer()

    run_configuration_check(customizer)

    if ga_customizer.google_analytics_traffic_historical:
        start_date = ga_customizer.google_analytics_traffic_historical_start_date
        end_date = ga_customizer.google_analytics_traffic_historical_end_date
    else:
        end_date = datetime.date.today()
        start_date = (end_date - datetime.timedelta(7))

    for view_id in ga_customizer.view_ids:
        df = GoogleAnalytics(client_name=ga_customizer.client, secrets_path=ga_customizer.google_analytics_traffic_secrets_path).query(
            view_id=view_id, raw_dimensions=ga_customizer.google_analytics_traffic_dimensions, raw_metrics=ga_customizer.google_analytics_traffic_metrics,
            start_date=start_date, end_date=end_date
        )
        if df.shape[0]:
            df = run_processing(df=df, customizer=customizer)
            grc.run_data_ingest_rolling_dates(df=df, customizer=customizer, date_col='report_date')
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


def run_configuration_check(customizer):
    """
    In order to run the script, the configured Customizer must be valid
    :param customizer:
    :return:
    """
    required_attributes = [
        'historical',
        'historical_start_date',
        'historical_end_date',
        'table',
    ]
    for attribute in required_attributes:
        result = grc.get_optional_attribute(cls=customizer, attribute=attribute)
        assert result, f"{SCRIPT_NAME}|Required attribute not configured, {attribute}"


def run_processing(df: pd.DataFrame, customizer: custom.Customizer) -> pd.DataFrame:
    """
    Iterate through pre-defined processing stages that may or may not be configured in Customizer
    :param df:
    :param customizer:
    :return:
    """
    processing_stages = [
        'rename',
        'type',
        'post_processing'  # TODO:(jake) build this out as a function that executes custom SQL to override/customize after update_join
    ]
    for stage in processing_stages:
        logger.info('Checking for custom stage {}'.format(stage))
        if grc.get_optional_attribute(cls=customizer, attribute=stage):
            logger.info('Now processing stage {}'.format(stage))
            df = grc.get_optional_attribute(cls=customizer, attribute=stage)(df=df)
    return df


if __name__ == '__main__':
    main()
