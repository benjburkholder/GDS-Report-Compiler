"""
Google Analytics Traffic
"""
import logging
import datetime
import pandas as pd

from utils import custom, platform
SCRIPT_NAME = platform.get_script_name(__file__)
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def main() -> int:
    customizer = custom.get_customizer(SCRIPT_NAME)
    run_configuration_check(customizer)
    view_id = platform.get_required_attribute(customizer, 'view_id')
    historical = platform.get_required_attribute(customizer, 'historical')
    if historical:
        start_date = platform.get_required_attribute(customizer, 'historical_start_date')
        end_date = platform.get_required_attribute(customizer, 'historical_end_date')
    else:
        end_date = datetime.date.today()
        start_date = (end_date - datetime.timedelta(7))
    # TODO(jschroeder) call GoogleAnalyticsReporting::query() and return to df
    df = pd.DataFrame()  # TEST
    if df.shape[0]:
        df = run_processing(df=df, customizer=customizer)
        platform.run_data_ingest_rolling_dates(df=df, customizer=customizer, date_col='report_date')
        return 0
    else:
        logger.warning('No data returned for view id {} for dates {} - {}'.format(view_id, start_date, end_date))
        return -1


def run_configuration_check(customizer):
    """
    In order to run the script, the configured Customizer must be valid
    :param customizer:
    :return:
    """
    required_attributes = [
        'view_id',
        'historical',
        'historical_start_date',
        'historical_end_date',
        'table',
    ]
    for attribute in required_attributes:
        result = platform.get_optional_attribute(cls=customizer, attribute=attribute)
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
        'parse'
    ]
    for stage in processing_stages:
        logger.info('Checking for custom stage {}'.format(stage))
        if platform.get_optional_attribute(cls=customizer, attribute=stage):
            logger.info('Now processing stage {}'.format(stage))
            platform.get_optional_attribute(cls=customizer, attribute=stage)(df=df)
    return df


if __name__ == '__main__':
    main()
