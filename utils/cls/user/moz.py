import os
import pathlib
import pandas as pd

from utils.cls.core import Customizer


class Moz(Customizer):
    prefix = 'moz'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)

        setattr(self, 'lookup_tables', {
            'status': {
                'table_type': 'moz',
                'active': True,
                'refresh_status': False,
                'lookup_source_sheet': 'Moz Listing to Property',
                'schema': 'lookup_moz_schema',
                'table_name': 'lookup_moz_listingtolocation'
            }}),

        # Schema for Moz lookup table
        setattr(self, f'{self.prefix}_lookup_moz_schema', {
            'table': 'lookup_moz_listingtolocation',
            'schema': 'public',
            'type': 'lookup',
            'columns': [
                {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 150},
                {'name': 'account', 'type': 'character varying', 'length': 150},
                {'name': 'label', 'type': 'character varying', 'length': 150},
                {'name': 'name', 'type': 'character varying', 'length': 150},
                {'name': 'address', 'type': 'character varying', 'length': 250},
                {'name': 'city', 'type': 'character varying', 'length': 50},
                {'name': 'state', 'type': 'character varying', 'length': 50},
                {'name': 'zip', 'type': 'bigint'},
                {'name': 'phone', 'type': 'character varying', 'length': 25},

            ],
            'owner': 'postgres'
        })

        # Schema for Moz source table
        setattr(self, f'{self.prefix}_source_moz_schema', {
            'table': 'source_moz_listingtoproperty',
            'schema': 'public',
            'type': 'source',
            'columns': [
                {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 150},
                {'name': 'account', 'type': 'character varying', 'length': 150},
                {'name': 'label', 'type': 'character varying', 'length': 150},
                {'name': 'name', 'type': 'character varying', 'length': 150},
                {'name': 'address', 'type': 'character varying', 'length': 250},
                {'name': 'city', 'type': 'character varying', 'length': 50},
                {'name': 'state', 'type': 'character varying', 'length': 50},
                {'name': 'zip', 'type': 'bigint'},
                {'name': 'phone', 'type': 'character varying', 'length': 25},

            ],
            'owner': 'postgres'
        })

        setattr(self, f'{self.prefix}_account_label_pairs', {
            'account_label_pairs': [
                {'account': 'Linkmedia360-Digital', 'label': 'zwirner'}
            ]
        })


class MozProRankingsCustomizer(Moz):
    prefix = 'moz_pro_rankings'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'mozpro_rankings')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Pro - Rankings',
            'property': None
        })

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'mozpro_rankings',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'campaign_id', 'type': 'character varying', 'length': 100},
                {'name': 'id', 'type': 'character varying', 'length': 100},
                {'name': 'search_id', 'type': 'character varying', 'length': 100},
                {'name': 'keyword', 'type': 'character varying', 'length': 100},
                {'name': 'search_engine', 'type': 'character varying', 'length': 100},
                {'name': 'device', 'type': 'character varying', 'length': 100},
                {'name': 'geo', 'type': 'character varying', 'length': 100},
                {'name': 'tags', 'type': 'character varying', 'length': 250},
                {'name': 'url', 'type': 'character varying', 'length': 1000},
                {'name': 'keyword_added_at', 'type': 'timestamp with time zone'},
                {'name': 'rank', 'type': 'bigint'},
                {'name': 'branded', 'type': 'bigint'},

            ],
            'indexes': [
                {
                    'name': 'ix_moz_pro_rankings',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'id', 'sort': 'asc', 'nulls_last': True},
                    ]
                }
            ],
            'owner': 'postgres'
        })

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozprorankings_backfilter',
            'active': 1,
            'code': """

                    UPDATE public.mozpro_rankings TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_urltolocation LOOKUP
                    WHERE TARGET.url LIKE CONCAT('%', LOOKUP.url, '%')
                    AND LOOKUP.exact = 0;

                    UPDATE public.mozpro_rankings TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_urltolocation LOOKUP
                    WHERE TARGET.url = LOOKUP.url
                    AND LOOKUP.exact = 1;

                    UPDATE public.mozpro_rankings
                    SET
                        property = 'Non-Location Pages'
                    WHERE property IS NULL;

                    CLUSTER public.mozpro_rankings;

                                        SELECT 1;
                    """,
            'return': 'integer',
            'owner': 'postgres'
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozprorankings_audit',
            'active': 1,
            'code': """

                    """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozProSERPCustomizer(Moz):
    prefix = 'moz_pro_serp'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'mozpro_serp')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Pro - SERP',
            'property': None
        })

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'mozpro_serp',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'campaign_id', 'type': 'character varying', 'length': 100},
                {'name': 'id', 'type': 'character varying', 'length': 100},
                {'name': 'search_id', 'type': 'character varying', 'length': 100},
                {'name': 'keyword', 'type': 'character varying', 'length': 100},
                {'name': 'search_engine', 'type': 'character varying', 'length': 100},
                {'name': 'device', 'type': 'character varying', 'length': 100},
                {'name': 'geo', 'type': 'character varying', 'length': 100},
                {'name': 'tags', 'type': 'character varying', 'length': 250},
                {'name': 'url', 'type': 'character varying', 'length': 1000},
                {'name': 'keyword_added_at', 'type': 'timestamp with time zone'},
                {'name': 'ads_bottom', 'type': 'bigint'},
                {'name': 'ads_top', 'type': 'bigint'},
                {'name': 'featured_snippet', 'type': 'bigint'},
                {'name': 'image_pack', 'type': 'bigint'},
                {'name': 'in_depth_articles', 'type': 'bigint'},
                {'name': 'knowledge_card', 'type': 'bigint'},
                {'name': 'knowledge_panel', 'type': 'bigint'},
                {'name': 'local_pack', 'type': 'bigint'},
                {'name': 'local_teaser', 'type': 'bigint'},
                {'name': 'news_pack', 'type': 'bigint'},
                {'name': 'related_questions', 'type': 'bigint'},
                {'name': 'review', 'type': 'bigint'},
                {'name': 'shopping_results', 'type': 'bigint'},
                {'name': 'sitelinks', 'type': 'bigint'},
                {'name': 'tweet', 'type': 'bigint'},
                {'name': 'video', 'type': 'bigint'},
                {'name': 'branded', 'type': 'bigint'},

            ],
            'indexes': [
                {
                    'name': 'ix_moz_pro_serp',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'id', 'sort': 'asc', 'nulls_last': True},
                    ]
                }
            ],
            'owner': 'postgres'
        })

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozproserp_backfilter',
            'active': 1,
            'code': """
                    UPDATE public.mozpro_serp TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_urltolocation LOOKUP
                    WHERE TARGET.url LIKE CONCAT('%', LOOKUP.url, '%')
                    AND LOOKUP.exact = 0;

                    UPDATE public.mozpro_serp TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_urltolocation LOOKUP
                    WHERE TARGET.url = LOOKUP.url
                    AND LOOKUP.exact = 1;

                    UPDATE public.mozpro_serp
                    SET
                        property = 'Non-Location Pages'
                    WHERE property IS NULL;

                    CLUSTER public.mozpro_serp;

                    SELECT 0;

                    """,
            'return': 'integer',
            'owner': 'postgres'
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozproserp_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozLocalVisibilityCustomizer(Moz):
    prefix = 'moz_local_visibility'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-01-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-01-02')
        setattr(self, f'{self.prefix}_table', 'mozlocal_directory_visibility_report_mdd')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Local - Visibility Report',
            'property': None
        })

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'mozlocal_directory_visibility_report_mdd',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'account_name', 'type': 'character varying', 'length': 100},
                {'name': 'listing_id', 'type': 'character varying', 'length': 25},
                {'name': 'directory', 'type': 'character varying', 'length': 100},
                {'name': 'points_reached', 'type': 'bigint'},
                {'name': 'max_points', 'type': 'bigint'},

            ],
            'indexes': [
                {
                    'name': 'ix_moz_local_directory_visibility_report',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'data_source', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'property', 'sort': 'asc', 'nulls_last': True}
                    ]
                }
            ],
            'owner': 'postgres'
        })

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozlocalvisibility_backfilter',
            'active': 1,
            'code': """
                    UPDATE public.mozlocal_directory_visibility_report TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_moz_listingtolocation LOOKUP
                    WHERE CAST(TARGET.listing_id AS character varying(25)) = CAST(LOOKUP.listing_id AS character varying(25));

                    UPDATE public.mozlocal_directory_visibility_report
                    SET
                        property = 'Non-Location Pages'
                    WHERE property IS NULL;

                    CLUSTER public.mozlocal_directory_visibility_report;

                    SELECT 0;
                    """,
            'return': 'integer',
            'owner': 'postgres'
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozlocalvisibility_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozLocalSyncCustomizer(Moz):
    prefix = 'moz_local_sync'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-03-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-03-10')
        setattr(self, f'{self.prefix}_table', 'mozlocal_directory_sync_report_mdd')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Local - Sync',
            'property': None
        })

        # model
        setattr(self, f'{self.prefix}_schema', {
            'table': 'mozlocal_directory_sync_report_mdd',
            'schema': 'public',
            'type': 'reporting',
            'columns': [
                {'name': 'report_date', 'type': 'date'},
                {'name': 'data_source', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'account_name', 'type': 'character varying', 'length': 100},
                {'name': 'listing_id', 'type': 'character varying', 'length': 25},
                {'name': 'directory', 'type': 'character varying', 'length': 100},
                {'name': 'field', 'type': 'character varying', 'length': 100},
                {'name': 'sync_status', 'type': 'bigint'},

            ],
            'indexes': [
                {
                    'name': 'ix_moz_local_directory_sync_report',
                    'tablespace': 'pg_default',
                    'clustered': True,
                    'method': 'btree',
                    'columns': [
                        {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'data_source', 'sort': 'asc', 'nulls_last': True},
                        {'name': 'property', 'sort': 'asc', 'nulls_last': True}
                    ]
                }
            ],
            'owner': 'postgres'
        })

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozlocalsync_backfilter',
            'active': 1,
            'code': """
                    UPDATE public.mozlocal_directory_sync_report TARGET
                    SET 
                        property = LOOKUP.property
                    FROM public.lookup_moz_listingtolocation LOOKUP
                    WHERE CAST(TARGET.listing_id AS character varying(25)) = CAST(LOOKUP.listing_id AS character varying(25));

                    UPDATE public.mozlocal_directory_sync_report
                    SET
                        property = 'Non-Location Pages'
                    WHERE property IS NULL;

                    CLUSTER public.mozlocal_directory_sync_report;

                    SELECT 0;
                            """,
            'return': 'integer',
            'owner': 'postgres'
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozlocalsync_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


