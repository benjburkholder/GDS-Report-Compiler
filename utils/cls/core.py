"""
Custom

This script is where all reporting configuration takes place
"""
import re


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization
    """
    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'dbms',
        'client',
        'project',
        'version',
        'recipients'
    ]

    def get_lookup_table_by_tablespace(self, tablespace: list) -> dict:
        lookup_tables = []
        for space in tablespace:
            for sheet in self.CONFIGURATION_WORKBOOK['sheets']:
                if (sheet['table'].get('type') == 'lookup') and (space in sheet['table'].get('tablespace', [])):
                    lookup_tables.append(sheet['table'])
        assert len(lookup_tables) == 1
        return lookup_tables[0]

    def get_backfilter_columns_by_table(self, table: dict) -> list:
        return [
            col for col in table['columns'] if col.get('backfilter')
        ]

    def get_backfilter_entity_columns_by_table(self, table: dict) -> list:
        return [
            col for col in table['columns'] if col.get('entity_col')
        ]

    def generate_set_statement_by_entity_columns(self, entity_columns: list) -> str:
        statement = "SET\n"
        count = 1
        for col in entity_columns:
            if count == len(entity_columns):
                statement += f"{col['name']} = LOOKUP.{col['name']}\n"
            else:
                statement += f"{col['name']} = LOOKUP.{col['name']},\n"
        return statement

    def create_backfilter_statement(
            self, table: dict, lookup_table: dict, backfilter_column: dict, entity_columns: list, update_type: str):
        set_statement = self.generate_set_statement_by_entity_columns(entity_columns=entity_columns)
        if update_type == 'exact':
            return f"""
                UPDATE {table['schema']}.{table['name']} TARGET
                    {set_statement}
                FROM {lookup_table['schema']}.{lookup_table['name']} LOOKUP
                WHERE TARGET.{backfilter_column['name']} = LOOKUP.{backfilter_column['name']}
                AND LOOKUP.exact = 1;
            """
        elif update_type == 'fuzzy':
            return f"""
                UPDATE {table['schema']}.{table['name']} TARGET
                    {set_statement}
                FROM {lookup_table['schema']}.{lookup_table['name']} LOOKUP
                WHERE TARGET.{backfilter_column['name']} ILIKE CONCAT('%', LOOKUP.{backfilter_column['name']}, '%')
                AND LOOKUP.exact = 0;
            """
        else:
            raise AssertionError(
                f"GRC: Unsupported update_type, {update_type}"
            )

    def create_set_default_statements(self, table: dict) -> list:
        default_cols = [
            col for col in table['columns'] if 'default' in col.keys()
        ]
        statements = []
        for col in default_cols:
            statements.append(
                f"""
                UPDATE {table['schema']}.{table['name']}
                SET {col['name']} = '{col['default'] if col['default'] is not None else 'NULL'}'
                WHERE {col['name']} IS NULL;
                """
            )
        return statements

    def get_table_dictionary_by_name(self, table_name: str) -> dict:
        return [
            table for table in self.CONFIGURATION_WORKBOOK['sheets']
            if table['table']['name'] == table_name
        ][0]['table']

    def build_backfilter_statements(self) -> list:
        table = self.get_table_dictionary_by_name(self.get_attribute('table'))
        lookup_table = self.get_lookup_table_by_tablespace(
            tablespace=table['tablespace']
        )
        backfilter_columns = self.get_backfilter_columns_by_table(table=table)
        entity_columns = self.get_backfilter_entity_columns_by_table(table=table)

        statements = []
        for column in backfilter_columns:
            for update_type in lookup_table['update_types']:
                statements.append(
                    self.create_backfilter_statement(
                        table=table,
                        lookup_table=lookup_table,
                        backfilter_column=column,
                        entity_columns=entity_columns,
                        update_type=update_type
                    )
                )
        statements.extend(self.create_set_default_statements(table=table))
        return statements

    def create_ingest_statement(self, customizer, master_columns, target_sheets):
        target_columns = self.__isolate_target_columns(target_sheets=target_sheets)
        delete_statement = self.__create_delete_from_statement(customizer=customizer, target_columns=target_columns)
        insert_statement = self.__create_insert_statement(customizer, master_columns=master_columns, target_columns=target_columns)

        return delete_statement

    def __isolate_target_columns(self, target_sheets):
        assert len(target_sheets) == 1, "Only one client sheet should be present, " \
                                        "check config workbook sheet name matches only one table name"

        return target_sheets[0]['table']['columns']

    def __create_delete_from_statement(self, customizer, target_columns):
        ingest_indicator = [column['name'] for column in target_columns if 'ingest_indicator' in column][0]
        assert ingest_indicator, "'ingest_indicator' attribute not assigned to table column used in ingest procedure"
        return f"""
                DELETE FROM public.{customizer.marketing_data['table']['name']}
                WHERE {ingest_indicator} = '{customizer.custom_columns[0][ingest_indicator]}';

                """

    # TODO finish ingest statement
    def __create_insert_statement(self, customizer, master_columns, target_columns):

        for col in master_columns:
            if col not in target_columns:
                col['name'] = f"NULL AS {col['name']}"

        final_ingest_columns = [col['name'] for col in master_columns]

        insert_statement = ''
        for col in final_ingest_columns:
            insert_statement += '\t' + col + ',\n\t\t\t\t'

        return f"""
                INSERT INTO public.{customizer.marketing_data['table']['name']}
                SELECT
                {insert_statement}
                FROM public.{self.get_attribute('table')}


                """

    def __create_group_by_statement(self, target_sheet_columns):
        pass

    def __create_optional_exclusion_statement(self):
        pass

    CLIENT_NAME = 'ZwirnerEquipment'

    """
    Each table should have:
        - backfilter - column to join on for update... join statements
        - default - column to assign a default value to when NULL (usually an entity, below)
        - entity - column to map data to based off the lookup table (e.g. Property, Market, etc)
    """
    CONFIGURATION_WORKBOOK = {
        'config_sheet_name': 'Zwirner Equipment - Configuration',
        'source_refresh_dates': [1, 15],
        'lookup_refresh_status': False,
        'sheets': [
            {'sheet': 'URL to Property', 'table': {
                'name': 'lookup_urltolocation',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['moz_pro', 'google_analytics'],
                'update_types': ['exact', 'fuzzy'],
                'columns': [
                    {'name': 'url', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'exact', 'type': 'bigint'},
                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Pro Campaign Master', 'table': {
                'name': 'source_moz_procampaignmaster',
                'schema': 'public',
                'type': 'source',
                'tablespace': ['moz_pro'],
                'columns': [
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100},
                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Local Account Master', 'table': {
                'name': 'source_moz_localaccountmaster',
                'schema': 'public',
                'type': 'source',
                'tablespace': ['moz_local'],
                'columns': [
                    {'name': 'account', 'type': 'character varying', 'length': 150},
                    {'name': 'label', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Local Account Master', 'table': {
                'name': 'source_moz_localaccountmaster',
                'schema': 'public',
                'type': 'source',
                'tablespace': ['moz_local'],
                'columns': [
                    {'name': 'account', 'type': 'character varying', 'length': 150},
                    {'name': 'label', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GA Views', 'table': {
                'name': 'source_ga_views',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'view_id', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GMB Account Master', 'table': {
                'name': 'source_gmb_accountmaster',
                'schema': 'public',
                'type': 'source',
                'columns': [
                    {'name': 'account_name', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Listing to Property', 'table': {
                'name': 'lookup_moz_listingtolocation',
                'schema': 'public',
                'tablespace': ['moz_local'],
                'update_types': ['exact'],
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
                    {'name': 'exact', 'type': 'bigint'}
                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GMB Listing to Property', 'table': {
                'name': 'lookup_gmb_listingtolocation',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['google_my_business'],
                'update_types': ['exact'],
                'columns': [
                    {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                    {'name': 'property', 'type': 'character varying', 'length': 150},
                    {'name': 'address_line_1', 'type': 'character varying', 'length': 250},
                    {'name': 'city', 'type': 'character varying', 'length': 50},
                    {'name': 'state', 'type': 'character varying', 'length': 50},
                    {'name': 'zip', 'type': 'character varying', 'length': 50},
                    {'name': 'phone', 'type': 'character varying', 'length': 25},
                    {'name': 'exact', 'type': 'bigint'}
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_local_visibility',
                'schema': 'public',
                'tablespace': ['moz_local'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': True},
                    {'name': 'directory', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'points_reached', 'type': 'bigint', 'master_include': True},
                    {'name': 'max_points', 'type': 'bigint', 'master_include': True},
                ],
                'indexes': [
                    {
                        'name': 'ix_moz_local_visibility',
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
            }},
            {'sheet': None, 'table': {
                'name': 'moz_local_sync',
                'schema': 'public',
                'tablespace': ['moz_local'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': False},
                    {'name': 'directory', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'field', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'sync_status', 'type': 'bigint', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_moz_local_sync',
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
            }},
            {'sheet': None, 'table': {
                'name': 'moz_pro_rankings',
                'schema': 'public',
                'tablespace': ['moz_pro'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'id', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'search_id', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'keyword', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'search_engine', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'geo', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'tags', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': True},
                    {'name': 'keyword_added_at', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'rank', 'type': 'bigint', 'master_include': True},
                    {'name': 'branded', 'type': 'bigint', 'master_include': True},

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
            }},
            {'sheet': None, 'table': {
                'name': 'moz_pro_serp',
                'schema': 'public',
                'tablespace': ['moz_pro'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'id', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'search_id', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'keyword', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'search_engine', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'geo', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'tags', 'type': 'character varying', 'length': 250, 'master_include': False},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': False},
                    {'name': 'keyword_added_at', 'type': 'timestamp with time zone', 'master_include': False},
                    {'name': 'ads_bottom', 'type': 'bigint', 'master_include': True},
                    {'name': 'ads_top', 'type': 'bigint', 'master_include': True},
                    {'name': 'featured_snippet', 'type': 'bigint', 'master_include': True},
                    {'name': 'image_pack', 'type': 'bigint', 'master_include': True},
                    {'name': 'in_depth_articles', 'type': 'bigint', 'master_include': True},
                    {'name': 'knowledge_card', 'type': 'bigint', 'master_include': True},
                    {'name': 'knowledge_panel', 'type': 'bigint', 'master_include': True},
                    {'name': 'local_pack', 'type': 'bigint', 'master_include': True},
                    {'name': 'local_teaser', 'type': 'bigint', 'master_include': True},
                    {'name': 'news_pack', 'type': 'bigint', 'master_include': True},
                    {'name': 'related_questions', 'type': 'bigint', 'master_include': True},
                    {'name': 'review', 'type': 'bigint', 'master_include': True},
                    {'name': 'shopping_results', 'type': 'bigint', 'master_include': True},
                    {'name': 'site_links', 'type': 'bigint', 'master_include': True},
                    {'name': 'tweet', 'type': 'bigint', 'master_include': True},
                    {'name': 'video', 'type': 'bigint', 'master_include': True},
                    {'name': 'branded', 'type': 'bigint', 'master_include': False},

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
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_traffic',
                'schema': 'public',
                'tablespace': ['google_analytics'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': False},
                    {'name': 'sessions', 'type': 'bigint', 'master_include': True},
                    {'name': 'percent_new_sessions', 'type': 'double precision', 'master_include': True},
                    {'name': 'pageviews', 'type': 'bigint', 'master_include': True},
                    {'name': 'unique_pageviews', 'type': 'bigint', 'master_include': True},
                    {'name': 'pageviews_per_session', 'type': 'double precision', 'master_include': True},
                    {'name': 'entrances', 'type': 'bigint', 'master_include': True},
                    {'name': 'bounces', 'type': 'bigint', 'master_include': True},
                    {'name': 'session_duration', 'type': 'double precision', 'master_include': True},
                    {'name': 'users', 'type': 'bigint', 'master_include': True},
                    {'name': 'new_users', 'type': 'bigint', 'master_include': True},
                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_traffic',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_events',
                'schema': 'public',
                'tablespace': ['google_analytics'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 150, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': False},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': False},
                    {'name': 'event_label', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'event_action', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'total_events', 'type': 'bigint', 'master_include': True},
                    {'name': 'unique_events', 'type': 'bigint', 'master_include': True},
                    {'name': 'event_value', 'type': 'double precision', 'master_include': True},
                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_events',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_analytics_goals',
                'schema': 'public',
                'tablespace': ['google_analytics'],
                'type': 'reporting',
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 150, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': False},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': False},
                    {'name': 'request_a_quote', 'type': 'bigint', 'master_include': True},
                    {'name': 'sidebar_contact_us', 'type': 'bigint', 'master_include': True},
                    {'name': 'contact_us_form_submission', 'type': 'bigint', 'master_include': True},
                    {'name': 'newsletter_signups', 'type': 'bigint', 'master_include': True},
                    {'name': 'dialogtech_calls', 'type': 'bigint', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_google_analytics_goals',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'source_medium', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ]
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_my_business_insights',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['google_my_business'],
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'listing_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': False},
                    {'name': 'maps_views', 'type': 'bigint', 'master_include': True},
                    {'name': 'search_views', 'type': 'bigint', 'master_include': True},
                    {'name': 'website_click_actions', 'type': 'bigint', 'master_include': True},
                    {'name': 'phone_call_actions', 'type': 'bigint', 'master_include': True},
                    {'name': 'driving_direction_actions', 'type': 'bigint', 'master_include': True},
                    {'name': 'photo_views', 'type': 'bigint', 'master_include': True},
                    {'name': 'branded_searches', 'type': 'bigint', 'master_include': True},
                    {'name': 'direct_searches', 'type': 'bigint', 'master_include': True},
                    {'name': 'discovery_searches', 'type': 'bigint', 'master_include': True},
                    {'name': 'post_views_on_search', 'type': 'bigint', 'master_include': True},
                    {'name': 'post_cta_actions', 'type': 'bigint', 'master_include': True},
                ],
                'indexes': [
                    {
                        'name': 'ix_google_my_business_insights',
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
            }},
            {'sheet': None, 'table': {
                'name': 'google_my_business_reviews',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['google_my_business'],
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': False},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': False},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': False},
                    {'name': 'listing_name', 'type': 'character varying', 'length': 150, 'master_include': False},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': False},
                    {'name': 'average_rating', 'type': 'double precision', 'master_include': True},
                    {'name': 'rating', 'type': 'double precision', 'master_include': True},
                    {'name': 'review_count', 'type': 'double precision', 'master_include': True},
                    {'name': 'reviewer', 'type': 'character varying', 'length': 150, 'master_include': True},
                ],
                'indexes': [
                    {
                        'name': 'ix_google_my_business_reviews',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'listing_name', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'listing_id', 'sort': 'asc', 'nulls_last': True}
                        ],
                    }
                ],
                'owner': 'postgres'
            }},
        ]}

    # Schema for the marketing_data table creation
    marketing_data = {'sheet': None, 'table': {
        'name': 'marketing_data',
        'schema': 'public',
        'type': 'master',
        'columns': [

        ],
        'indexes': [
            {
                'name': 'ix_marketing_data',
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
    }}

    supported_dbms = [
        'postgresql'
    ]

    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    # ### START EDITING HERE ###
    dbms = 'postgresql'
    client = 'ZwirnerEquipment'
    project = '<PROJECT>'
    version = '<VERSION>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com',
        'bburkholder@linkmedia360.com'
    ]
    db = {
        'DATABASE': 'zwirnerequipment_omnilocal',
        'USERNAME': 'python-2',
        'PASSWORD': 'pythonpipelines',
        'SERVER': '35.222.11.147'
    }

    # ### END EDITING ###

    def __init__(self):
        self.prefix = self.get_class_prefix()
        self.set_function_prefixes()
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
            if attribute == 'dbms':
                if getattr(self, attribute) not in self.supported_dbms:
                    return False
        return True

    def get_class_prefix(self):
        cls_name = self.__class__.__name__.replace('Customizer', '')
        return re.sub(r'(?<!^)(?=[A-Z])', '_', cls_name).lower()

    def generate_attribute_prefix(self, attrib):
        return f"{self.prefix}_{attrib}"

    def set_attribute(self, attrib, value):
        setattr(self, self.generate_attribute_prefix(attrib=attrib), value)

    def get_attribute(self, attrib):
        return getattr(self, self.generate_attribute_prefix(attrib=attrib))

    def set_function_prefixes(self) -> None:
        # get function attributes from the Customizer child instance only
        funcs = [
            attrib for attrib in dir(self) if (
                (callable(getattr(self, attrib))) and not
                (re.match(r'_+.*', attrib)) and
                (attrib not in dir(Customizer))
            )
        ]
        # set the prefixed attribute and assert the attribute holds
        for func in funcs:
            prefix_func = self.generate_attribute_prefix(attrib=func)
            if not hasattr(self, prefix_func):
                setattr(self, prefix_func, getattr(self, func))
                assert hasattr(self, prefix_func)
        return
