"""
Custom

This script is where all reporting configuration takes place

TODO(jschroeder) examples needed of each type of attribute and method
"""
import os
import sys
import inspect
import pandas as pd

# IMPORT CUSTOM CHILDREN HERE
from utils.custom_children.google_analytics_events import GoogleAnalyticsEventsCustomizer


def get_customizers() -> list:
    """
    Crawl this file and all files in custom_children and return all classes
    :return:
    """
    clsmembers = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    return clsmembers  # as list


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization

    TODO(jschroeder) could we / would we ever want to inherit from this?
    """

    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'client',
        'project',
        'version',
        'recipients'
    ]
    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    client = '<CLIENT HERE>'
    project = '<PROJECT HERE>'
    version = '<VERSION HERE>'
    recipients = [
        # EMAILS HERE for error notifications
    ]

    def __init__(self):
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
        return True

    # DATA SOURCE SPECIFIC - REQUIRED FOR INDIVIDUAL SCRIPTS TO BE RUN
    # GA - Traffic
    # attributes
    google_analytics_traffic_class = True  # flag that indicates this is the class to use for google_analytics_traffic
    google_analytics_traffic_debug = True
    google_analytics_traffic_metrics = [
        'sessions',
        'percentNewSessions',
        'pageviews',
        'uniquePageviews',
        'pageviewsPerSession',
        'entrances',
        'bounces',
        'sessionDuration',
        'users',
        'newUsers'
    ]
    google_analytics_traffic_dimensions = [
        'date',
        'channelGrouping',
        'sourceMedium',
        'device',
        'campaign',
        'page',
    ]

    # model
    google_analytics_traffic_schema = {
        'table': 'googleanalytics_traffic',
        'type': 'reporting',
        'columns': [
            {'name': 'report_date', 'type': 'date'},
            {'name': 'data_source', 'type': 'character varying', 'length': 100},
            {'name': 'property', 'type': 'character varying', 'length': 100},
            {'name': 'community', 'type': 'character varying', 'length': 100},
            {'name': 'service_line', 'type': 'character varying', 'length': 100},
            {'name': 'view_id', 'type': 'character varying', 'length': 25},
            {'name': 'source_medium', 'type': 'character varying', 'length': 100},
            {'name': 'device', 'type': 'character varying', 'length': 50},
            {'name': 'campaign', 'type': 'character varying', 'length': 100},
            {'name': 'page', 'type': 'character varying', 'length': 500},
            {'name': 'sessions', 'type': 'character varying', 'length': 500},
            {'name': 'percent_new_sessions', 'type': 'double precision'},
            {'name': 'pageviews', 'type': 'bigint'},
            {'name': 'unique_pageviews', 'type': 'bigint'},
            {'name': 'pageviews_per_session', 'type': 'double precision'},
            {'name': 'entrances', 'type': 'bigint'},
            {'name': 'bounces', 'type': 'bigint'},
            {'name': 'session_duration', 'type': 'double precision'},
            {'name': 'users', 'type': 'bigint'},
            {'name': 'new_users', 'type': 'bigint'},
        ],
        'indexes': [
            {
                'name': 'ix_google_analytics_traffic',
                'clustered': True,
                'method': 'btree',
                'columns': [
                    {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                    {'name': 'medium', 'sort': 'asc', 'nulls_last': True},
                    {'name': 'device', 'sort': 'asc', 'null_last': True}
                ]
            }
        ],
        'owner': 'postgres'
    }

    # backfilter procedure
    google_analytics_traffic_backfilter_procedure = {
        'name': 'googleanalytics_backfilter',
        'active': 1,
        'code': """
        UPDATE ...
        
        SELECT 1;
        """,
        'return': 'integer',
        'owner': 'postgres'
    }

    # audit procedure
    google_analytics_traffic_audit_procedure = {
        'name': 'googleanalytics_audit',
        'active': 1,
        'code': """
        
        """,
        'return': 'integer',
        'owner': 'postgres'
    }

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_getter(self) -> str:
        """
        Pass to GoogleAnalyticsReporting constructor as retrieval method for json credentials
        :return:
        """
        return '{"msg": "i am json credentials"}'

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns into pg/sql friendly aliases
        :param df:
        :return:
        """
        return df

    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in self.google_analytics_traffic_schema['columns']:
            assert column['name'] in df.columns
            if column['type'] == 'character varying':
                assert 'length' in column.keys()
                df[column['name']] = df[column['name']].apply(lambda x: str(x)[:column['length']] if x else None)
            elif column['type'] == 'bigint':
                df[column['name']] = df[column['name']].apply(lambda x: int(x) if x else None)
            elif column['type'] == 'double precision':
                df[column['name']] = df[column['name']].apply(lambda x: float(x) if x else None)
            elif column['type'] == 'date':
                df[column['name']] = pd.to_datetime(df[column['name']]).dt.date
            elif column['type'] == 'timestamp without time zone':
                df[column['name']] = pd.to_datetime(df[column['name']])
            elif column['type'] == 'datetime with time zone':
                # TODO(jschroeder) how better to interpret timezone data?
                df[column['name']] = pd.to_datetime(df[column['name']], utc=True)
        return df

    # methods
    # noinspection PyMethodMayBeStatic
    def google_analytics_traffic_parse(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse the data by pagePath to their respective entities
        :return:
        """
        return df


customizers = get_customizers()
print(customizers)
