from ..custom import Customizer


class GoogleAnalyticsEventsCustomizer(Customizer):
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