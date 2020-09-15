"""
Custom

This script is where all reporting configuration takes place
"""
import os
import re
import json
import pathlib

import pandas as pd
import sqlalchemy

from utils.dbms_helpers.postgres_helpers import build_postgresql_engine
from ..stdlib import module_from_file
from conf.static import ENTITY_COLS


def get_configured_item_by_key(key: str, lookup: dict):
    if key in lookup.keys():
        # support for customization by view id - aka "for view x use y"
        if type(lookup[key]) == list:
            return lookup[key]
        # support for self-referencing lookup - aka "same as... x"
        elif type(lookup[key]) == str:
            return lookup[lookup[key]]
        # checking to ensure the configuration was setup properly
        else:
            raise AssertionError("Invalid type for lookup entry on " + key)
    else:
        return lookup['global']


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

    # placeholder for oauth credentials and engine
    secrets = {}
    secrets_dat = {}
    application_engine = None
    application_database = 'applications'
    # columns used for entity mapping
    entity_cols = ENTITY_COLS

    __columns_key = 'columns'
    __sheets_key = 'sheets'
    __table_key = 'table'
    __table_type_key = 'type'
    __table_space_key = 'tablespace'

    def get_lookup_table_by_tablespace(self, tablespace: list) -> dict:
        lookup_tables = []
        ret_idx = 0  # always return the first result for this function
        for space in tablespace:
            for sheet in self.configuration_workbook[self.__sheets_key]:
                if (
                        (sheet[self.__table_key].get(self.__table_type_key) == 'lookup') and
                        (space in sheet[self.__table_key].get(self.__table_space_key, []))
                ):
                    lookup_tables.append(sheet[self.__table_key])
        assert len(lookup_tables) == 1
        return lookup_tables[ret_idx]

    def get_backfilter_entity_columns_by_table(self, table: dict) -> list:
        return [
            col for col in table[self.__columns_key] if col.get('entity_col')
        ]

    def get_backfilter_columns_by_table(self, table: dict) -> list:
        return [
            col for col in table[self.__columns_key] if col.get('backfilter')
        ]

    def generate_set_statement_by_entity_columns(self, entity_columns: list) -> str:
        statement = "SET\n"
        count = len(entity_columns)
        for col in enumerate(entity_columns, start=1):
            if col[0] == count:
                statement += f"{col[1]['name']} = LOOKUP.{col[1]['name']}\n"
            else:
                statement += f"{col[1]['name']} = LOOKUP.{col[1]['name']},\n"
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

        # we want to be able to use indexes to optimize fuzzy updates
        elif update_type == 'fuzzy':
            fuzzy_lookup = """AND LOOKUP.exact = 0;"""

            fuzzy_stmt = f"""
                UPDATE {table['schema']}.{table['name']} TARGET
                    {set_statement}
                FROM {lookup_table['schema']}.{lookup_table['name']} LOOKUP
                WHERE TARGET.{backfilter_column['name']} ILIKE CONCAT(LOOKUP.{backfilter_column['name']}, '%')
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
            table for table in self.configuration_workbook['sheets']
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

    def __construct_mv_select_statement(
            self,
            target_table: dict,
            lookup_table: dict,
            stmt: str = ''
    ) -> str:

        target_table_name = target_table['name']
        target_table_schema = target_table['schema']
        columns = target_table['columns']

        lookup_table_name = lookup_table['name']
        lookup_table_schema = lookup_table['schema']

        # get the columns we are setting
        entity_column_names = [
            x['name'] for x in columns
            if x.get('entity_col')
        ]

        backfilter = None

        if not stmt:
            stmt = '\tSELECT\n'
            for column in columns:
                column_name = column['name']
                if column.get('backfilter'):
                    backfilter = column['backfilter']
                if column_name in entity_column_names:
                    stmt += f'\t\tlookup.{column_name}\n'
                else:
                    stmt += f'\t\ttarget.{column_name}\n'
            stmt += f'\tFROM {target_table_schema}.{target_table_name} target\n'
            stmt += f'\tLEFT JOIN {lookup_table_schema}.{lookup_table_name} lookup\n'
            assert backfilter, "backfilter column not assigned"
            stmt += f'\t\tON target.{backfilter} = lookup.{backfilter}'
        else:
            outer = 'SELECT\n'
            for column in columns:
                column_name = column['name']
                column_default = column.get('default')
                if column.get('backfilter'):
                    backfilter = column['backfilter']
                if column_name in entity_column_names:
                    outer += f'\tCASE\n'
                    outer += f'WHEN t1.{column_name} IS NOT NULL THEN t1.{column_name}\n'
                    if column_default:
                        outer += f'ELSE COALESCE(lookup.{column_name}, {column_default})'
                else:
                    outer += f'\t\tt1.{column_name}\n'
            outer += 'FROM (\n'
            outer += stmt
            outer += '\n) t1\n'
            outer += f'LEFT JOIN {lookup_table_schema}.{lookup_table_name} lookup\n'
            assert backfilter, "backfilter column not assigned"
            stmt += f"\t\tON target.{backfilter} ILIKE CONCAT(lookup.{backfilter}, '%')"
            assert backfilter, "backfilter column not assigned"
        return stmt

    def _create_mv(self, name: str) -> None:
        """
        Creates an optimized materialized view using the available rules in the configuration workbook
        ====================================================================================================
        :param name:
        :return:
        """
        table = self.get_table_dictionary_by_name(self.get_attribute('table'))
        lookup_table = self.get_lookup_table_by_tablespace(
            tablespace=table['tablespace']
        )

        stmt = ''
        for _ in lookup_table['update_types']:
            stmt = self.__construct_mv_select_statement(
                stmt=stmt,
                target_table=table,
                lookup_table=lookup_table
            )

        with self.engine.connect() as con:
            con.execute(
                f"""
                CREATE MATERIALIZED VIEW public.{name}
                    TABLESPACE pg_default
                    AS   
                    {stmt}  
                WITH DATA;
                """
            )
            con.execute(
                f"ALTER TABLE public.{name} OWNER TO postgres;"
            )

        return

    def _check_mv_exists(self, name: str) -> bool:
        """
        Query the active database for the existence of a materialized view
        ====================================================================================================

        :param name:
        :return:
        """
        sql = sqlalchemy.text(
            """
            SELECT COUNT(*) AS view_count
            FROM pg_matviews
            WHERE matviewname = :name;  
            """
        )
        with self.engine.connect() as con:
            result = con.execute(
                sql,
                name=name
            ).first()
        return result['view_count'] if result else False

    def _refresh_mv(self, mv: str) -> None:
        """
        Refresh a materialized view by name

        :param mv:
        :return:
        """
        with self.engine.connect() as con:
            con.execute(
                f"REFRESH MATERIALIZED VIEW public.{mv};"
            )

    def compile(self, mv: str):
        """
        Generate or refresh the materialized view for the given Customizer instance

        :param mv:
        :return:
        """
        # check whether the materialized view exists or not
        if not self._check_mv_exists(name=mv):
            # if it does not, create the materialized view (under postgres)
            self._create_mv(name=mv)

        # if it does, execute a refreshment
        self._refresh_mv(mv=mv)





    def create_ingest_statement(self, master_columns, target_sheets) -> list:

        default_ingest_statements = self.__get_ingest_defaults(target_sheet=target_sheets)
        target_columns = self.__isolate_target_columns(target_sheets=target_sheets)
        delete_statement = self.__create_delete_from_statement(target_columns=target_columns)
        insert_statement = self.__create_insert_statement(master_columns=master_columns, target_columns=target_columns, ingest_defaults=default_ingest_statements)
        group_by_columns = self.__create_group_by_statement(target_sheet_columns=target_sheets)
        historical_date_range = self.__create_historical_range_statement()

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

    def __create_delete_from_statement(self, target_columns):
        assert len([col for col in target_columns if "ingest_indicator" in col]) == 1, "'ingest_indicator' attribute not assigned to table column used in ingest procedure"
        ingest_indicator = [column['name'] for column in target_columns if 'ingest_indicator' in column][0]

        if not self.get_attribute(attrib='historical'):
            delete_stmt = f"""
                          DELETE FROM public.{self.marketing_data['table']['name']}
                          WHERE {ingest_indicator} = '{self.get_attribute(attrib=ingest_indicator)}';
                          """
        else:
            start_date = self.get_attribute(attrib='historical_start_date')
            end_date = self.get_attribute(attrib='historical_end_date')

            date_range = f"""AND report_date BETWEEN '{start_date}' AND '{end_date}';"""

            delete_stmt = f"""
                           DELETE FROM public.{self.marketing_data['table']['name']}
                           WHERE {ingest_indicator} = '{self.get_attribute(attrib=ingest_indicator)}'
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

    def __check_for_custom_columns(self):
        base_path = self.get_base_path() / 'conf' / 'stored'
        if 'stored_custom_columns.json' in os.listdir(base_path):
            with open(os.path.join(base_path, 'stored_custom_columns.json')) as f:
                data = json.load(f)

        return data if data else None

    def __create_insert_statement(self, master_columns, target_columns, ingest_defaults):
        # Converts both to dataframes for easier comparison via column selection

        target_column_keys = self.__compile_target_keys(target_columns=target_columns)

        # custom_columns = self.__check_for_custom_columns()

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
                INSERT INTO public.{self.marketing_data['table']['name']}
                SELECT
                {insert_statement}
                FROM public.{self.get_attribute('table')}
                """

    def __add_custom_columns(self):
        pass

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

    def __create_historical_range_statement(self):
        start_date = self.get_attribute('historical_start_date')
        end_date = self.get_attribute('historical_end_date')

        return f"""
                WHERE report_date BETWEEN '{start_date}' AND '{end_date}';
                """

    def audit_automation_procedure(self, index_column, cadence):
        fail_list = []
        if cadence.lower() == 'daily':
            # if daily, there should be 7 days in week query
            days = self.__run_audit_query(how='week', quan=1, source=index_column)
            score = self.__score_days(days=days, min_days=7)
        elif cadence.lower() == 'weekly':
            # if weekly, there should be 1 day in week query
            days = self.__run_audit_query(how='week', quan=1, source=index_column)
            score = self.__score_days(days=days, min_days=1)
        elif cadence.lower() == 'monthly':
            # if monthly, there should be 1 day in 61 day query
            days = self.__run_audit_query(how='month', quan=2, source=index_column)
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

    def __run_audit_query(self, how: str, quan: int, source: str) -> list:
        sql = """
        SELECT DISTINCT report_date
        FROM public.{marketing_table}
        WHERE 
            report_date >= ((NOW() - INTERVAL '2 day') - INTERVAL '{quan} {how}')
            AND data_source = '{source}';
        """.format(quan=quan, how=how.lower(), source=source, marketing_table=self.marketing_data['table']['name'])
        print(sql)
        with self.engine.connect() as con:
            results = con.execute(sql).fetchall()
        return [item[0] for item in results] if results else None

    # Indicates which columns to be dropped from source / lookup tables
    columns_to_drop = {
        'status': False,
        'columns': ['zip', 'phone']
    }

    supported_dbms = [
        'postgresql'
    ]

    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    __configuration_app_file_name = 'app.json'

    __configuration_workbook_file_name = 'workbook.json'

    __configuration_marketing_data_columns_file_name = 'custom_marketing_data_columns.json'

    __configuration_slack_alerts_file_name = 'slack_alerts.json'

    __configuration_alert_recipients_file_name = 'alert_recipients.json'

    def __init__(self, database: str = None):
        # assign project core application configuration
        app = self.get_configuration_by_file_name(
            file_name=self.__configuration_app_file_name
        )
        self.dbms = app['dbms']
        self.client = app['client']
        self.vertical = app['vertical']
        self.project = app['project']
        self.version = app['version']
        self.db = app['db']

        if database:
            self.db['DATABASE'] = database
        # load configuration for customize-able base class attributes
        self.configuration_workbook = self.get_configuration_by_file_name(
            file_name=self.__configuration_workbook_file_name
        )
        self.marketing_data = self.get_configuration_by_file_name(
            file_name='marketing_data.json'
        )
        self.custom_marketing_data_columns = self.get_configuration_by_file_name(
            file_name=self.__configuration_marketing_data_columns_file_name
        )
        self.vertical_specific_slack_alerts = self.get_configuration_by_file_name(
            file_name=self.__configuration_slack_alerts_file_name
        )
        self.recipients = self.get_configuration_by_file_name(
            file_name=self.__configuration_alert_recipients_file_name
        )

        # run startup assertions, checks and assignments
        self.prefix = self.get_class_prefix()
        self.set_function_prefixes()
        self._load_test_configuration()
        assert self.valid_global_configuration(), self.global_configuration_message
        self.engine = build_postgresql_engine(customizer=self)
        self.build_application_engine()
        setattr(self, f"{os.path.basename(__file__).replace('.py', '')}_class", True)

    def build_application_engine(self):
        database = self.db['DATABASE']
        self.db['DATABASE'] = self.application_database
        self.application_engine = build_postgresql_engine(self)
        self.db['DATABASE'] = database

    def get_secrets(self, include_dat: bool = True) -> None:
        self.__get_secrets()
        if include_dat:
            self.__get_secrets_dat()
        return

    def __get_secrets(self) -> None:
        name_value = getattr(self, 'credential_name')
        assert name_value, f"Invalid name_value {name_value} provided"
        with self.application_engine.connect() as con:
            result = con.execute(
                sqlalchemy.text(
                    """
                    SELECT content_value
                    FROM public.gds_compiler_credentials
                    WHERE name_value = :name_value;
                    """
                ),
                name_value=name_value
            ).first()
        self.secrets = result['content_value'] if result else {}
        return

    def __get_secrets_dat(self) -> None:
        name_value = getattr(self, 'secrets_name')
        assert name_value, f"Invalid name_value {name_value} provided"
        with self.application_engine.connect() as con:
            result = con.execute(
                sqlalchemy.text(
                    """
                    SELECT
                        client_name,
                        name_value,
                        content_value
                    FROM public.gds_compiler_credentials_dat
                    WHERE client_name = :client_name
                    AND name_value = :name_value;
                    """
                ),
                client_name=self.client,  # camel-case client name
                name_value=name_value
            ).first()
        self.secrets_dat = json.dumps(result['content_value']) if result else {}
        return

    def set_customizer_secrets_dat(self) -> None:
        client_name = getattr(self, 'client')  # camel-case client name
        name_value = getattr(self, 'secrets_name', '')
        assert name_value, f"Invalid name_value {name_value} provided"
        content_value = getattr(self, 'secrets_dat', '')
        assert content_value, f"Invalid content_value {content_value} provided"
        content_value = json.dumps(content_value) if type(content_value) == dict else content_value
        with self.application_engine.connect() as con:
            count_result = con.execute(
                sqlalchemy.text(
                    """
                    SELECT COUNT(*) as count_value
                    FROM public.gds_compiler_credentials_dat
                    WHERE client_name = :client_name
                    AND name_value = :name_value;
                    """
                ),
                client_name=client_name,
                name_value=name_value
            ).first()
            if count_result['count_value'] == 0:
                con.execute(
                    sqlalchemy.text(
                        """
                        INSERT INTO public.gds_compiler_credentials_dat
                        (
                            client_name,
                            name_value,
                            content_value
                        )
                        VALUES
                        (
                            :client_name, 
                            :name_value,
                            :content_value
                        );
                        """
                    ),
                    client_name=client_name,
                    name_value=name_value,
                    content_value=content_value
                )
            else:
                con.execute(
                    sqlalchemy.text(
                        """
                        UPDATE public.gds_compiler_credentials_dat
                        SET content_value = :content_value
                        WHERE client_name = :client_name
                        AND name_value = :name_value;
                        """
                    ),
                    client_name=client_name,
                    name_value=name_value,
                    content_value=content_value
                )
        return

    def backfilter_statement(self):
        target_sheets = [
            sheet for sheet in self.configuration_workbook['sheets']
            if sheet['table']['name'] == self.get_attribute('table')
        ]
        assert len(target_sheets) == 1
        sheet = target_sheets[0]
        assert sheet['table']['type'] == 'reporting'
        statements = self.build_backfilter_statements()
        with self.engine.connect() as con:
            for statement in statements:
                con.execute(sqlalchemy.text(statement))

    def ingest_statement(self):
        master_columns = []
        for sheets in self.configuration_workbook['sheets']:
            if sheets['table']['type'] == 'reporting':
                if sheets['table']['active']:
                    for column in sheets['table']['columns']:
                        if column['master_include']:
                            master_columns.append(column)
        target_sheets = [
            sheet for sheet in self.configuration_workbook['sheets']
            if sheet['table']['name'] == self.get_attribute('table')]
        ingest_procedure = self.create_ingest_statement(master_columns, target_sheets)
        with self.engine.connect() as con:
            for statement in ingest_procedure:
                con.execute(statement)

    # TODO: since we're moving to unit testing, are these safe to remove?
    def audit(self):
        for sheets in self.configuration_workbook['sheets']:
            if sheets['table']['type'] == 'reporting':
                if sheets['table']['audit_cadence']:
                    if sheets['table']['name'] == self.get_attribute('table'):
                        audit_automation_indicator = [
                            column['name'] for column in sheets['table']['columns'] if 'ingest_indicator' in column
                        ][0]
                        self.audit_automation_procedure(
                            index_column=self.get_attribute(audit_automation_indicator),
                            cadence=sheets['table']['audit_cadence']
                        )

    def get_base_path(self):
        return pathlib.Path(__file__).parents[2]

    def get_configuration_path_by_file_name(self, file_name: str):
        return os.path.join(
            self.get_base_path(),
            'conf',
            'stored',
            file_name
        )

    def get_configuration_by_file_name(self, file_name: str) -> dict:
        with open(
            self.get_configuration_path_by_file_name(file_name=file_name),
            'r'
        ) as file:
            return json.load(file)

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
