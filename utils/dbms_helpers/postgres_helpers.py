"""
Platform Helpers File
"""
import sqlalchemy

from utils import stdlib


def build_postgresql_engine(customizer):
    connection_string = 'postgresql+pypostgresql://{username}:{password}@{server}/{database}'.format(
        username=customizer.db['USERNAME'],
        password=customizer.db['PASSWORD'],
        server=customizer.db['SERVER'],
        database=customizer.db['DATABASE']
    )
    return sqlalchemy.create_engine(connection_string)


def clear_postgresql_non_golden_data(customizer, date_col, min_date, max_date):
    engine = build_postgresql_engine(customizer=customizer)

    sql = sqlalchemy.text(
        f"""
        DELETE
        FROM {getattr(customizer, f'{customizer.prefix}_schema')['schema']}.{getattr(customizer, f'{customizer.prefix}_table')}

        WHERE {date_col} BETWEEN :min_date AND :max_date;
        """
    )
    with engine.connect() as con:
        con.execute(sql, min_date=min_date, max_date=max_date)


def clear_postgresql_lookup_table(customizer):
    engine = build_postgresql_engine(customizer=customizer)

    sql = sqlalchemy.text(
        f"""
            DELETE
            FROM public.{customizer.lookup_tables["status"]["table_name"]};

            """
    )
    with engine.connect() as con:
        con.execute(sql)


def insert_postgresql_data(customizer, df, if_exists='append', index=False, index_label=None):
    engine = build_postgresql_engine(customizer=customizer)
    with engine.connect() as con:
        df.to_sql(
            getattr(customizer, f'{customizer.prefix}_table'),
            con=con,
            if_exists=if_exists,
            index=index,
            index_label=index_label
        )


def insert_postgresql_lookup_data(customizer, df, if_exists='append', index=False, index_label=None):
    engine = build_postgresql_engine(customizer=customizer)

    with engine.connect() as con:
        df.to_sql(
            customizer.lookup_tables["status"]["table_name"],
            con=con,
            if_exists=if_exists,
            index=index,
            index_label=index_label
        )

    return 0


def check_postgresql_table_exists(customizer, table, schema) -> bool:
    engine = build_postgresql_engine(customizer=customizer)
    sql = sqlalchemy.text(
        """
        SELECT EXISTS (
           SELECT FROM pg_tables
           WHERE
                schemaname = :schema AND
                tablename  = :table
        );
        """
    )
    with engine.connect() as con:
        result = con.execute(sql, schema=schema, table=table).first()
    return bool(result['exists'])


def create_postgresql_table_from_schema(customizer, schema):
    engine = build_postgresql_engine(customizer=customizer)
    table_sql = _generate_postgresql_create_table_statement(schema=schema)
    with engine.connect() as con:
        con.execute(table_sql)

    if schema['type'] != 'lookup':
        for index in schema['indexes']:
            index_sql = _generate_postgresql_create_index_statement(schema=schema, index=index)
            with engine.connect() as con:
                con.execute(index_sql)
            if index['clustered']:
                cluster_sql = _generate_postgresql_cluster_statement(schema=schema, index=index)
                with engine.connect() as con:
                    con.execute(cluster_sql)

        return stdlib.EXIT_SUCCESS

    return stdlib.EXIT_FAILURE


def _generate_postgresql_create_table_statement(schema: dict) -> str:
    assert 'table' in schema.keys(), "Schema not formed properly {}".format(schema)
    assert 'columns' in schema.keys(), "Schema not formed properly {}".format(schema)
    assert len(schema['columns']), "Columns not specified in schema for table {}".format(schema['table'])
    stmt = f"CREATE TABLE {schema['schema']}.{schema['table']}"
    stmt += "\n"
    stmt += "(\n"
    col_len = len(schema['columns'])
    idx = 0
    for column in schema['columns']:
        line = f"\t{column['name']} {column['type']}(),\n"
        if 'length' in column.keys():
            line = line.replace('()', f"({column['length']})")
        else:
            line = line.replace('()', '')
        if 'null' in column.keys():
            if column['null']:
                line = line.replace(',', ' NULL,\n')
            else:
                line = line.replace(',', ' NOT NULL,\n')
        idx += 1
        if idx == col_len:
            line = line.replace(',', '')
        stmt += line
    stmt += ");"
    return stmt


def _generate_postgresql_create_index_statement(index: dict, schema: dict) -> str:
    assert 'columns' in index.keys(), "'columns' attribute missing from index. {}".format(index)
    assert 'tablespace' in index.keys(), "'tablespace' attribute missing from index. {}".format(index)
    stmt = f"CREATE INDEX {index['name']}\n"
    stmt += f"ON {schema['schema']}.{schema['table']} USING {index['method']}\n"
    stmt += "(\n"
    col_len = len(index['columns'])
    idx = 0
    for column in index['columns']:
        assert 'name' in column.keys(), "'name' attribute missing from column in index. {}".format(column)
        assert 'sort' in column.keys(), "'sort' attribute missing from column in index. {}".format(column)
        assert 'nulls_last' in column.keys(), "'nulls_last' attribute missing from column in index. {}".format(column)
        line = f"\t{column['name']}"
        if column['sort'].lower() == 'asc':
            line += " ASC"
        else:
            line += " DESC"
        if column['nulls_last']:
            line += " NULLS LAST"
        line += ",\n"
        idx += 1
        if idx == col_len:
            line = line.replace(',', '')
        stmt += line
    stmt += ")"
    stmt += f" TABLESPACE {index['tablespace']};"
    return stmt


def _generate_postgresql_cluster_statement(schema: dict, index: dict) -> str:
    stmt = f"ALTER TABLE {schema['schema']}.{schema['table']}\n"
    stmt += f"\tCLUSTER ON {index['name']};"
    return stmt
