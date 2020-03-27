import os
import pathlib
import datetime
import sqlalchemy

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class Moz(Customizer):

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
        setattr(self, f'{self.prefix}_source_moz_listingtoproperty', {
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

        # Schema for URL lookup table
        setattr(self, f'{self.prefix}_lookup_url_schema', {
            'table': 'lookup_urltolocation',
            'schema': 'public',
            'type': 'lookup',
            'columns': [
                {'name': 'url', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 100},
                {'name': 'exact', 'type': 'bigint'},
            ],
            'owner': 'postgres'
        })

        setattr(self, f'{self.prefix}_source_moz_localacccountmaster', {
            'table': 'source_moz_localacccountmaster',
            'schema': 'public',
            'type': 'source',
            'columns': [
                {'name': 'account', 'type': 'character varying', 'length': 150},
                {'name': 'label', 'type': 'character varying', 'length': 150},

            ],
            'owner': 'postgres'
        })

        setattr(self, f'{self.prefix}_source_moz_procampaignmaster', {
            'table': 'source_moz_procampaignmaster',
            'schema': 'public',
            'type': 'source',
            'columns': [
                {'name': 'campaign_id', 'type': 'character varying', 'length': 100},

            ],
            'owner': 'postgres'
        })

        setattr(self, f'{self.prefix}_source_moz_directoryexclusions', {
            'table': 'source_moz_directoryexclusions',
            'schema': 'public',
            'type': 'source',
            'columns': [
                {'name': 'exclusions', 'type': 'character varying', 'length': 100},

            ],
            'owner': 'postgres'
        })

        setattr(self, f'{self.prefix}_exclude_moz_directories', self.exclude_moz_directories)

    def pull_moz_local_accounts(self, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_localaccountmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            accounts_cleaned = [{'account': account[0], 'label': account[1]} for account in accounts_raw if
                                accounts_raw]

            return accounts_cleaned

    def pull_moz_pro_accounts(self, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_procampaignmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            campaign_ids = [{'campaign_id': campaign_id} for campaign_id in accounts_raw[0]]

            return campaign_ids

    def exclude_moz_directories(self, customizer, df):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT *
                FROM public.source_moz_directoryexclusions;
                """
            )

            result = con.execute(sql)
            exclusions_raw = result.fetchall()

            exclusion_list = [exclusion[0] for exclusion in exclusions_raw]

            if exclusion_list:
                df_cleaned = df.loc[
                             ~(df['directory'].isin(exclusion_list))
                ,
                             :

                             ]

            return df_cleaned if True else df


class MozProRankingsCustomizer(Moz):
    prefix = 'moz_pro_rankings'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_report_date', datetime.date(2020, 1, 1))
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

        stmt = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.{getattr(self, f'{self.prefix}_lookup_url_schema')['table']} LOOKUP\n"
        stmt += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%');\n"

        stmt2 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt2 += "SET property = LOOKUP.property\n"
        stmt2 += f"FROM public.{getattr(self, f'{self.prefix}_lookup_url_schema')['table']} LOOKUP\n"
        stmt2 += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%')\n"
        stmt2 += "AND LOOKUP.exact = 1;"

        stmt3 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']}\n"
        stmt3 += "SET property = 'Non-Location Pages'\n"
        stmt3 += "WHERE property IS NULL;\n"

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozprorankings_backfilter',
            'active': 1,
            'code': [stmt, stmt2, stmt3]
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
        setattr(self, f'{self.prefix}_historical_report_date', datetime.date(2020, 1, 1))
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
                {'name': 'site_links', 'type': 'bigint'},
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

        stmt = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.{getattr(self, f'{self.prefix}_lookup_url_schema')['table']} LOOKUP\n"
        stmt += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%');\n"

        stmt2 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt2 += "SET property = LOOKUP.property\n"
        stmt2 += f"FROM public.{getattr(self, f'{self.prefix}_lookup_url_schema')['table']} LOOKUP\n"
        stmt2 += "WHERE TARGET.url ILIKE CONCAT('%', LOOKUP.url, '%')\n"
        stmt2 += "AND LOOKUP.exact = 1;"

        stmt3 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']}\n"
        stmt3 += "SET property = 'Non-Location Pages'\n"
        stmt3 += "WHERE property IS NULL;\n"

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozproserp_backfilter',
            'active': 1,
            'code': [stmt, stmt2, stmt3]
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
        setattr(self, f'{self.prefix}_historical', False)
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

        stmt = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.{getattr(self, f'{self.prefix}_lookup_moz_schema')['table']} LOOKUP\n"

        stmt += "WHERE CAST(TARGET.listing_id AS character varying(25)) = CAST(LOOKUP.listing_id AS character varying(25));\n"
        stmt2 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']}\n"
        stmt2 += "SET property = 'Non-Location Pages'\n"
        stmt2 += "WHERE property IS NULL;\n"

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozlocalvisibility_backfilter',
            'active': 1,
            'code': [stmt, stmt2]
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
        setattr(self, f'{self.prefix}_historical', False)
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

        stmt = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']} TARGET\n"
        stmt += "SET property = LOOKUP.property\n"
        stmt += f"FROM public.{getattr(self, f'{self.prefix}_lookup_moz_schema')['table']} LOOKUP\n"
        stmt += "WHERE CAST(TARGET.listing_id AS character varying(25)) = CAST(LOOKUP.listing_id AS character varying(25));\n"

        stmt2 = f"UPDATE public.{getattr(self, f'{self.prefix}_schema')['table']}\n"
        stmt2 += "SET property = 'Non-Location Pages'\n"
        stmt2 += "WHERE property IS NULL;\n"

        # backfilter procedure
        setattr(self, f'{self.prefix}_backfilter_procedure', {
            'name': 'mozlocalsync_backfilter',
            'active': 1,
            'code': [stmt, stmt2]
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


