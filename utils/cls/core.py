"""
Custom

This script is where all reporting configuration takes place
"""
from utils.dbms_helpers.postgres_helpers import build_postgresql_engine
from ..stdlib import module_from_file

import pandas as pd
import re
import os


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

        start_date = self.get_attribute(attrib='historical_start_date')
        end_date = self.get_attribute(attrib='historical_end_date')
        date_range = f"""
                      AND report_date BETWEEN '{start_date}' AND '{end_date}'
                      """

        if update_type == 'exact':
            exact_lookup = """AND LOOKUP.exact = 1;"""

            exact_stmt = f"""
                UPDATE {table['schema']}.{table['name']} TARGET
                    {set_statement}
                FROM {lookup_table['schema']}.{lookup_table['name']} LOOKUP
                WHERE TARGET.{backfilter_column['name']} = LOOKUP.{backfilter_column['name']}
                """

            if self.get_attribute(attrib='historical'):
                exact_stmt += date_range

            exact_stmt += exact_lookup

            return exact_stmt

        elif update_type == 'fuzzy':
            fuzzy_lookup = """AND LOOKUP.exact = 0;"""

            fuzzy_stmt = f"""
                UPDATE {table['schema']}.{table['name']} TARGET
                    {set_statement}
                FROM {lookup_table['schema']}.{lookup_table['name']} LOOKUP
                WHERE TARGET.{backfilter_column['name']} ILIKE CONCAT('%', LOOKUP.{backfilter_column['name']}, '%')
                """

            if self.get_attribute(attrib='historical'):
                fuzzy_stmt += date_range

            fuzzy_stmt += fuzzy_lookup

            return fuzzy_stmt

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

    def create_ingest_statement(self, customizer, master_columns, target_sheets) -> list:
        default_ingest_statements = self.__get_ingest_defaults(target_sheet=target_sheets)
        target_columns = self.__isolate_target_columns(target_sheets=target_sheets)
        delete_statement = self.__create_delete_from_statement(customizer=customizer, target_columns=target_columns)
        insert_statement = self.__create_insert_statement(customizer, master_columns=master_columns, target_columns=target_columns, ingest_defaults=default_ingest_statements)
        group_by_columns = self.__create_group_by_statement(target_sheet_columns=target_sheets)
        historical_date_range = self.__create_historical_range_statement(customizer=customizer)

        # Specifying date range for ingest statement
        if self.get_attribute(attrib='historical'):
            insert_statement += historical_date_range

        # Group_by statement is optional, checks if exists and appends to insert stmt if True
        if group_by_columns:
            insert_statement += group_by_columns

        return [delete_statement, insert_statement]

    def __isolate_target_columns(self, target_sheets):
        assert len(target_sheets) == 1, "Only one client sheet should be present, check config workbook sheet name matches only one table name"
        return target_sheets[0]['table']['columns']

    def __create_delete_from_statement(self, customizer, target_columns):
        assert len([col for col in target_columns if "ingest_indicator" in col]) == 1, "'ingest_indicator' attribute not assigned to table column used in ingest procedure"
        ingest_indicator = [column['name'] for column in target_columns if 'ingest_indicator' in column][0]

        if not self.get_attribute(attrib='historical'):
            delete_stmt = f"""
                          DELETE FROM public.{customizer.marketing_data['table']['name']}
                          WHERE {ingest_indicator} = f'{self.get_attribute(attrib=ingest_indicator)}';
                          """
        else:
            start_date = self.get_attribute(attrib='historical_start_date')
            end_date = self.get_attribute(attrib='historical_end_date')

            date_range = f"""AND report_date BETWEEN '{start_date}' AND '{end_date}';"""

            delete_stmt = f"""
                           DELETE FROM public.{customizer.marketing_data['table']['name']}
                           WHERE {ingest_indicator} = f'{self.get_attribute(attrib=ingest_indicator)}'
                           """

            delete_stmt += date_range

        return delete_stmt

    def __compile_target_keys(self, target_columns):

        target_keys = []
        for col in target_columns:
            if 'name' in col:
                target_keys.append('name')
            if 'aggregate_type' in col:
                target_keys.append('aggregate_type')
            if 'aggregate_cast' in col:
                target_keys.append('aggregate_cast')

        return list(set(target_keys))

    def __create_insert_statement(self, customizer, master_columns, target_columns, ingest_defaults):
        # Converts both to dataframes for easier comparison via column selection

        target_column_keys = self.__compile_target_keys(target_columns=target_columns)

        master_df = pd.DataFrame(master_columns)[target_column_keys].drop_duplicates(keep='first', subset='name').to_dict(orient='records')
        target_df = pd.DataFrame(target_columns)[target_column_keys]

        # Assign NULL to unused table columns in original order
        for col in master_df:
            if col['name'] not in target_df['name'].values:
                col['name'] = f"NULL AS {col['name']}"

                if ingest_defaults:
                    for item in ingest_defaults:
                        for key in item:
                            if col['name'] == f"NULL AS {key}":
                                col['name'] = f"'{item[key]}' AS {key}"

        # Assigns field aggregate type and cast if present (e.g. month aggregation, sum fields)
        for col in master_df:
            if col['name'] in target_df['name'].values:
                col.update(target_df.loc[target_df['name'] == col['name']].to_dict(orient='records')[0])
                if 'aggregate_type' in col:
                    if 'aggregate_cast' not in col:
                        if col['aggregate_type'] == 'month':
                            col['name'] = f"date_trunc('month', {col['name']})"

                        if col['aggregate_type'] == 'sum':
                            col['name'] = f"SUM({col['name']})"

                        if col['aggregate_type'] == 'avg':
                            col['name'] = f"AVG({col['name']})"

                        if col['aggregate_type'] == '1 month interval':
                            col['name'] = f"date_trunc('month', {col['name']} - interval '1 month')"

                    elif 'aggregate_cast' in col:
                        col['temp'] = f"CAST({col['name']} AS {col['aggregate_cast']})"
                        if 'aggregate_type' in col:
                            if col['aggregate_type'] == 'sum':
                                col['name'] = f"SUM({col['temp']})"

                            if col['aggregate_type'] == 'avg':
                                col['name'] = f"AVG({col['temp']})"

        # Extracts name from columns for final statement creation
        final_ingest_columns = [col['name'] for col in master_df]

        # Determines which item is last in list, then uses correct ending character
        insert_statement = ''
        col_count = len(final_ingest_columns) - 1
        for index, col in enumerate(final_ingest_columns):
            if index == col_count:
                insert_statement += f'{col}'
            else:
                insert_statement += f'{col},'

        return f"""
                INSERT INTO public.{customizer.marketing_data['table']['name']}
                SELECT
                {insert_statement}
                FROM public.{self.get_attribute('table')}
                """

    def __get_ingest_defaults(self, target_sheet):
        assert len(target_sheet) == 1, "Only one client sheet should be present, check config workbook sheet name matches only one table name"

        return target_sheet[0]['table']['ingest_defaults'] if 'ingest_defaults' in target_sheet[0]['table'] else None

    def __create_group_by_statement(self, target_sheet_columns):
        target_columns = self.__isolate_target_columns(target_sheets=target_sheet_columns)

        # Pulls columns (if any) to be used in group_by statement
        group_by_columns = [col['name'] for col in target_columns if 'group_by' in col]

        group_by_statement = ''

        # Check and add semicolon to last item in group by statement
        col_count = len(group_by_columns)-1
        for index, col in enumerate(group_by_columns):
            if index == col_count:
                group_by_statement += f'{col};'
            else:
                group_by_statement += f'{col},'

        return f"""
                GROUP BY
                {group_by_statement}
                """ if group_by_statement else None

    def __create_historical_range_statement(self, customizer):
        start_date = getattr(customizer, f'{self.prefix}_historical_start_date')
        end_date = getattr(customizer, f'{self.prefix}_historical_end_date')

        return f"""
                WHERE report_date BETWEEN '{start_date}' AND '{end_date}';
                """

    def audit_automation_procedure(self, index_column, cadence):
        engine = build_postgresql_engine(customizer=self)
        fail_list = []
        if cadence.lower() == 'daily':
            # if daily, there should be 7 days in week query
            days = self.__run_audit_query(how='week', quan=1, engine=engine, source=index_column)
            score = self.__score_days(days=days, min_days=7)
        elif cadence.lower() == 'weekly':
            # if weekly, there should be 1 day in week query
            days = self.__run_audit_query(how='week', quan=1, engine=engine, source=index_column)
            score = self.__score_days(days=days, min_days=1)
        elif cadence.lower() == 'monthly':
            # if monthly, there should be 1 day in 61 day query
            days = self.__run_audit_query(how='month', quan=2, engine=engine, source=index_column)
            score = self.__score_days(days=days, min_days=1)
        else:
            raise AssertionError(
                "Invalid cadence provided. Must be one of ('daily', 'weekly', 'monthly'). {}".format(cadence))
        # score the source and store for error reporting if needed
        if score:
            print('PASS: {}, {}'.format(index_column, cadence))
            print('SUCCESS: Audit Automation Complete.')

        else:
            fail_list.append({index_column: cadence})
        # if the fail list has entries, send the error notification
        if len(fail_list):
            raise AssertionError("""
                The following sources are not properly updating:
                {}
                """.format(fail_list))
        return 0

    def __score_days(self, days: list, min_days: int) -> bool:
        if days is None:
            return False
        elif len(days) < min_days:
            return False
        else:
            return True

    def __run_audit_query(self, how: str, quan: int, engine, source: str) -> list:
        sql = """
        SELECT DISTINCT report_date
        FROM public.{marketing_table}
        WHERE 
            report_date >= ((NOW() - INTERVAL '2 day') - INTERVAL '{quan} {how}')
            AND data_source = '{source}';
        """.format(quan=quan, how=how.lower(), source=source, marketing_table=self.marketing_data['table']['name'])
        print(sql)
        with engine.connect() as con:
            results = con.execute(sql).fetchall()
        return [item[0] for item in results] if results else None

    # ~~~~~~~~~~~~~~~~ EDITABLE CLIENT SPECIFIC DATA PAST THIS POINT ~~~~~~~~~~~~~~~~

    """
    Each table should have:
        - backfilter - column to join on for update... join statements
        - default - column to assign a default value to when NULL (usually an entity, below)
        - entity - column to map data to based off the lookup table (e.g. Property, Market, etc)
    """
    CONFIGURATION_WORKBOOK = {
        'config_sheet_name': '<CONFIG SHEET NAME>',
        'source_refresh_dates': [1, 15],
        'lookup_refresh_status': False,
        'sheets': [
            {'sheet': 'URL Mapping', 'table': {
                'name': 'lookup_urltolocation',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['moz_pro', 'google_analytics', 'google_search_console'],
                'update_types': ['exact', 'fuzzy'],
                'active': False,
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
                'active': False,
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
                'active': False,
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
                'active': False,
                'columns': [
                    {'name': 'view_id', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GMB Account Master', 'table': {
                'name': 'source_gmb_accountmaster',
                'schema': 'public',
                'type': 'source',
                'active': False,
                'columns': [
                    {'name': 'account_name', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GSC Property Master', 'table': {
                'name': 'source_gsc_propertymaster',
                'schema': 'public',
                'type': 'source',
                'active': False,
                'columns': [
                    {'name': 'property_url', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GAds Account Master', 'table': {
                'name': 'source_gads_accountmaster',
                'schema': 'public',
                'type': 'source',
                'active': False,
                'columns': [
                    {'name': 'account_id', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Account Cost', 'table': {
                'name': 'source_account_cost',
                'schema': 'public',
                'type': 'source',
                'active': False,
                'columns': [
                    {'name': 'date_start', 'type': 'date'},
                    {'name': 'date_end', 'type': 'date'},
                    {'name': 'property', 'type': 'character varying', 'length': 100},
                    {'name': 'tactic', 'type': 'character varying', 'length': 100},
                    {'name': 'cost', 'type': 'character varying', 'length': 25},
                    {'name': 'channel', 'type': 'character varying', 'length': 150},
                    {'name': 'notes', 'type': 'character varying', 'length': 100},

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Directory Exclusions', 'table': {
                'name': 'source_moz_directoryexclusions',
                'schema': 'public',
                'type': 'source',
                'active': False,
                'columns': [
                    {'name': 'exclusions', 'type': 'character varying', 'length': 100},
                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Moz Listing Mapping', 'table': {
                'name': 'lookup_moz_mapping',
                'schema': 'public',
                'tablespace': ['moz_local'],
                'update_types': ['exact'],
                'type': 'lookup',
                'active': False,
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
            {'sheet': 'GMB Listing Mapping', 'table': {
                'name': 'lookup_gmb_mapping',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['google_my_business'],
                'update_types': ['exact'],
                'active': False,
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
            {'sheet': 'DT Phone Label Mapping', 'table': {
                'name': 'lookup_dt_mapping',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['dialogtech'],
                'update_types': ['exact'],
                'active': False,
                'columns': [
                        {'name': 'phone_label', 'type': 'character varying', 'length': 150},
                        {'name': 'medium', 'type': 'character varying', 'length': 150},
                        {'name': 'property', 'type': 'character varying', 'length': 150},
                        {'name': 'exact', 'type': 'bigint'}

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'GAds Campaign Mapping', 'table': {
                'name': 'lookup_gads_mapping',
                'schema': 'public',
                'type': 'lookup',
                'tablespace': ['google_ads'],
                'update_types': ['exact'],
                'active': False,
                'columns': [
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 150},
                    {'name': 'campaign', 'type': 'character varying', 'length': 150},
                    {'name': 'property', 'type': 'character varying', 'length': 150},
                    {'name': 'exact', 'type': 'bigint'}

                ],
                'owner': 'postgres'
            }},
            {'sheet': 'Enquire - Market Source to Channel', 'table': {
                'name': 'lookup_enquire_market_source_to_channel_mapping',
                'schema': 'public',
                'type': 'lookup',
                'update_types': ['exact'],
                'active': False,
                'columns': [
                    {'name': 'market_source', 'type': 'character varying', 'length': 150},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 150},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'moz_local_visibility',
                'schema': 'public',
                'tablespace': ['moz_local'],
                'type': 'reporting',
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'ingest_defaults': [{'medium': 'Organic Search'}, {'device': 'Desktop'}],
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True, 'aggregate_type': 'month', 'group_by': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True, 'group_by': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True, 'group_by': True},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': True, 'group_by': True},
                    {'name': 'directory', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True},
                    {'name': 'points_reached', 'type': 'bigint', 'master_include': True, 'aggregate_type': 'sum'},
                    {'name': 'max_points', 'type': 'bigint', 'master_include': True, 'aggregate_type': 'sum'},
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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True, 'aggregate_type': 'month', 'group_by': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True, 'group_by': True},
                    {'name': 'account_name', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': True, 'group_by': True},
                    {'name': 'directory', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True},
                    {'name': 'field', 'type': 'character varying', 'length': 100, 'master_include': True, 'group_by': True},
                    {'name': 'sync_status', 'type': 'bigint', 'master_include': True, 'aggregate_type': 'avg', 'aggregate_cast': 'double precision'},

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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'ingest_defaults': [{'medium': 'Organic Search'}],
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True, 'aggregate_type': '1 month interval'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'ingest_defaults': [{'medium': 'Organic Search'}],
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True, 'aggregate_type': '1 month interval'},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
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
                    {'name': 'branded', 'type': 'bigint', 'master_include': True},

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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': True},
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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': True},
                    {'name': 'event_label', 'type': 'character varying', 'length': 300, 'master_include': True},
                    {'name': 'event_action', 'type': 'character varying', 'length': 300, 'master_include': True},
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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'view_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'medium', 'type': 'character varying', 'length': 100, 'master_include': True, },
                    {'name': 'source_medium', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'campaign', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'url', 'type': 'character varying', 'length': 1000, 'backfilter': True, 'master_include': True},
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
                'audit_cadence': 'monthly',
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'listing_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': True},
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
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True, 'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True, 'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'listing_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'listing_id', 'type': 'character varying', 'length': 150, 'backfilter': True, 'master_include': True},
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
            {'sheet': None, 'table': {
                'name': 'google_search_console_monthly',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['google_search_console'],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'property_url', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 150, 'backfilter': True,
                     'master_include': True},
                    {'name': 'page', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'query', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'impressions', 'type': 'bigint', 'master_include': True},
                    {'name': 'clicks', 'type': 'bigint', 'master_include': True},
                    {'name': 'ctr', 'type': 'double precision', 'master_include': True},
                    {'name': 'position', 'type': 'double precision', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_google_search_console_monthly',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True},
                        ],
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'dialogtech_call_detail',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['dialogtech'],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'campaign', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'medium', 'type': 'character varying', 'length': 150, 'backfilter': True,
                     'master_include': True},
                    {'name': 'number_dialed', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'caller_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'call_duration', 'type': 'double precision', 'master_include': True},
                    {'name': 'transfer_to_number', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'phone_label', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'call_transfer_status', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'client_id', 'type': 'character varying', 'length': 150,
                     'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_dialogtech_call_detail',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'number_dialed', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'caller_id', 'sort': 'asc', 'nulls_last': True},

                        ],
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'google_ads_campaign',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['google_ads'],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'account_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'advertising_channel_type', 'type': 'character varying', 'length': 100,
                     'master_include': True},
                    {'name': 'device', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'campaign', 'type': 'character varying', 'length': 250, 'backfilter': True,
                     'master_include': True},
                    {'name': 'campaign_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'cost', 'type': 'double precision', 'master_include': True},
                    {'name': 'clicks', 'type': 'bigint', 'master_include': True},
                    {'name': 'impressions', 'type': 'bigint', 'master_include': True},
                    {'name': 'search_available_impressions', 'type': 'double precision', 'master_include': True},
                    {'name': 'search_impression_share', 'type': 'double precision', 'master_include': True},
                    {'name': 'search_budget_lost_impression_share', 'type': 'double precision', 'master_include': True},
                    {'name': 'search_rank_lost_impression_share', 'type': 'double precision', 'master_include': True},
                    {'name': 'content_available_impressions', 'type': 'double precision', 'master_include': True},
                    {'name': 'content_impression_share', 'type': 'double precision', 'master_include': True},
                    {'name': 'content_budget_lost_impression_share', 'type': 'double precision', 'master_include': True},
                    {'name': 'content_rank_lost_impression_share', 'type': 'double precision', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_google_ads_campaign',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'campaign_id', 'sort': 'asc', 'nulls_last': True},
                            {'name': 'device', 'sort': 'asc', 'nulls_last': True}
                        ],
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_crm_activity_deposit',
                'schema': 'public',
                'type': 'reporting',
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'portal_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'community', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'market_source', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_status', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'activity_status_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_name', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'activity_type', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'activity_type_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_types_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'time_zone', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'reinquiry', 'type': 'bigint', 'master_include': True},
                    {'name': 'case_number', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'individual_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'individual_full_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'assigned_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'assigned_user_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'due_date_descriptive', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'due_date', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'due_date_end', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_updated', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_created', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'all_day', 'type': 'bigint', 'master_include': True},
                    {'name': 'sale_stage', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'completed', 'type': 'bigint', 'master_include': True},
                    {'name': 'cancelled', 'type': 'bigint', 'master_include': True},
                    {'name': 'no_show', 'type': 'bigint', 'master_include': True},
                    {'name': 'rescheduled', 'type': 'bigint', 'master_include': True},
                    {'name': 'is_activity_history', 'type': 'bigint', 'master_include': True},
                    {'name': 'overdue', 'type': 'bigint', 'master_include': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_crm_activity_inquiry',
                'schema': 'public',
                'type': 'reporting',
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'portal_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'community', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'market_source', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'community_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_status', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'activity_status_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_name', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'activity_type', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'activity_type_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_types_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'time_zone', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'reinquiry', 'type': 'bigint', 'master_include': True},
                    {'name': 'case_number', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'individual_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'individual_full_name', 'type': 'character varying', 'length': 150,
                     'master_include': True},
                    {'name': 'assigned_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'assigned_user_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'due_date_descriptive', 'type': 'character varying', 'length': 250,
                     'master_include': True},
                    {'name': 'due_date', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'due_date_end', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_updated', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_created', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'all_day', 'type': 'bigint', 'master_include': True},
                    {'name': 'sale_stage', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'completed', 'type': 'bigint', 'master_include': True},
                    {'name': 'cancelled', 'type': 'bigint', 'master_include': True},
                    {'name': 'no_show', 'type': 'bigint', 'master_include': True},
                    {'name': 'rescheduled', 'type': 'bigint', 'master_include': True},
                    {'name': 'is_activity_history', 'type': 'bigint', 'master_include': True},
                    {'name': 'overdue', 'type': 'bigint', 'master_include': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_crm_activity_movein',
                'schema': 'public',
                'type': 'reporting',
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'portal_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'community', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'market_source', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_status', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'activity_status_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_name', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'activity_type', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'activity_type_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_types_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'time_zone', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'reinquiry', 'type': 'bigint', 'master_include': True},
                    {'name': 'case_number', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'individual_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'individual_full_name', 'type': 'character varying', 'length': 150,
                     'master_include': True},
                    {'name': 'assigned_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'assigned_user_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'due_date_descriptive', 'type': 'character varying', 'length': 250,
                     'master_include': True},
                    {'name': 'due_date', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'due_date_end', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_updated', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_created', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'all_day', 'type': 'bigint', 'master_include': True},
                    {'name': 'sale_stage', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'completed', 'type': 'bigint', 'master_include': True},
                    {'name': 'cancelled', 'type': 'bigint', 'master_include': True},
                    {'name': 'no_show', 'type': 'bigint', 'master_include': True},
                    {'name': 'rescheduled', 'type': 'bigint', 'master_include': True},
                    {'name': 'is_activity_history', 'type': 'bigint', 'master_include': True},
                    {'name': 'overdue', 'type': 'bigint', 'master_include': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_crm_activity_tour',
                'schema': 'public',
                'type': 'reporting',
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'portal_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'community', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'service_line', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'channel_grouping', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'market_source', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_status', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'activity_status_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_name', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'activity_type', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'activity_type_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'activity_types_master_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'time_zone', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'reinquiry', 'type': 'bigint', 'master_include': True},
                    {'name': 'case_number', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'individual_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'individual_full_name', 'type': 'character varying', 'length': 150,
                     'master_include': True},
                    {'name': 'assigned_name', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'assigned_user_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'due_date_descriptive', 'type': 'character varying', 'length': 250,
                     'master_include': True},
                    {'name': 'due_date', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'due_date_end', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_updated', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'date_created', 'type': 'timestamp with time zone', 'master_include': True},
                    {'name': 'all_day', 'type': 'bigint', 'master_include': True},
                    {'name': 'sale_stage', 'type': 'character varying', 'length': 50, 'master_include': True},
                    {'name': 'completed', 'type': 'bigint', 'master_include': True},
                    {'name': 'cancelled', 'type': 'bigint', 'master_include': True},
                    {'name': 'no_show', 'type': 'bigint', 'master_include': True},
                    {'name': 'rescheduled', 'type': 'bigint', 'master_include': True},
                    {'name': 'is_activity_history', 'type': 'bigint', 'master_include': True},
                    {'name': 'overdue', 'type': 'bigint', 'master_include': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_map_histories',
                'schema': 'public',
                'type': 'reporting',
                'ingest_defaults': [],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'credential_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'created_at', 'type': 'timestamp without time zone', 'master_include': True},
                    {'name': 'id', 'type': 'bigint', 'master_include': True},
                    {'name': 'history_type', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'caller_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'direction', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'email_guid', 'type': 'character varying', 'length': 250, 'master_include': True},
                    {'name': 'email_sent_at', 'type': 'timestamp without time zone', 'master_include': True},
                    {'name': 'form_id', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'form_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': '"from"', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'from_city', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'from_country', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'from_state', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'from_zip', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'marketing_email', 'type': 'bigint', 'master_include': True},
                    {'name': 'open_tag_type', 'type': 'character varying', 'length': 25, 'master_include': True},
                    {'name': 'session_entry_page', 'type': 'character varying', 'length': 500, 'master_include': True},
                    {'name': 'session_ga_client_uid', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'session_ip_address', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'subject_line', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'submitted_from_url', 'type': 'character varying', 'length': 500, 'master_include': True},
                    {'name': '"to"', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'reply_to', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'transcript', 'type': 'character varying', 'length': 1000, 'master_include': True},
                    {'name': 'utm_campaign', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'utm_medium', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'utm_source', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'workflow_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'campaign_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'enquire_map_email_performance_summary',
                'schema': 'public',
                'type': 'reporting',
                'ingest_defaults': [],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': True,
                'columns': [
                    {'name': 'credential_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'email_log_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'contact_id', 'type': 'bigint', 'master_include': True},
                    {'name': 'campaign_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'workflow_name', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'email_log_date', 'type': 'date', 'master_include': True},
                    {'name': 'email', 'type': 'character varying', 'length': 255, 'master_include': True},
                    {'name': 'subject_line', 'type': 'character varying', 'length': 200, 'master_include': True},
                    {'name': 'email_log', 'type': 'integer', 'master_include': True},
                    {'name': 'email_open', 'type': 'integer', 'master_include': True},
                    {'name': 'email_link_click', 'type': 'integer', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},

                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'inquiry_goals',
                'schema': 'public',
                'type': 'reporting',
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'ingest_only': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'community', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'ownership_group', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'region', 'type': 'character varying', 'length': 100, 'master_include': True},
                    {'name': 'inquiry_goal', 'type': 'double precision', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix__inquiry_goals',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        ],
                    }
                ],
                'owner': 'postgres'
            }},
            {'sheet': None, 'table': {
                'name': 'account_cost',
                'schema': 'public',
                'type': 'reporting',
                'tablespace': ['account'],
                'audit_cadence': False,
                'backfilter_cadence': False,
                'active': False,
                'columns': [
                    {'name': 'report_date', 'type': 'date', 'master_include': True},
                    {'name': 'data_source', 'type': 'character varying', 'length': 100, 'master_include': True,
                     'ingest_indicator': True},
                    {'name': 'property', 'type': 'character varying', 'length': 100, 'entity_col': True,
                     'default': 'Non-Location Pages', 'master_include': True},
                    {'name': 'medium', 'type': 'character varying', 'length': 150, 'master_include': True},
                    {'name': 'daily_cost', 'type': 'double precision', 'master_include': True},

                ],
                'indexes': [
                    {
                        'name': 'ix_account_cost',
                        'tablespace': 'pg_default',
                        'clustered': True,
                        'method': 'btree',
                        'columns': [
                            {'name': 'report_date', 'sort': 'asc', 'nulls_last': True},
                        ],
                    }
                ],
                'owner': 'postgres'
            }},
        ]}

    # Custom columns (will be appended to end of marketing_data table)
    # Typically used for columns existing solely in marketing_data
    custom_marketing_data_columns = {'sheet': None, 'table': {
        'active': False,
        'columns': [

        ]
    }}

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

    # Indicates which columns to be dropped from source / lookup tables
    columns_to_drop = {
        'status': False,
        'columns': ['zip', 'phone']
    }

    supported_dbms = [
        'postgresql'
    ]

    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    # ### START EDITING HERE ###
    dbms = 'postgresql'
    client = '<TEST>'  # Enter in camel case
    project = '<PROJECT>'
    version = '<VERSION>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com',
        'bburkholder@linkmedia360.com'
    ]
    db = {
        'DATABASE': '',
        'USERNAME': 'python-2',
        'PASSWORD': 'pythonpipelines',
        'SERVER': '35.222.11.147'
    }

    # ### END EDITING ###

    def __init__(self):
        self.prefix = self.get_class_prefix()
        self.set_function_prefixes()
        self._load_test_configuration()
        assert self.valid_global_configuration(), self.global_configuration_message

    test_config_file_name = 'test_config.py'

    def _load_test_configuration(self):
        if not self.db['DATABASE']:
            test_config_path = os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                'user',
                'tests',
                'conf',
                self.test_config_file_name
            )
            try:
                test_config = module_from_file(self.test_config_file_name.replace('.py', ''), test_config_path)
                self.dbms = test_config.dbms
                self.client = test_config.client
                self.project = test_config.project
                self.version = test_config.version
                self.recipients = test_config.recipients
                self.db = test_config.db
            except FileNotFoundError:
                print('INFO: No testing configuration found, running script with production configuration.')
                return

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
