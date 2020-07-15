"""
Platform Helpers File
"""
import sqlalchemy

from utils import stdlib


def build_postgresql_engine(customizer):
    connection_string = 'postgresql+psycopg2://{username}:{password}@{server}/{database}'.format(
        username=customizer.db['USERNAME'],
        password=customizer.db['PASSWORD'],
        server=customizer.db['SERVER'],
        database=customizer.db['DATABASE']
    )
    return sqlalchemy.create_engine(connection_string)


def clear_postgresql_non_golden_data(customizer, date_col, min_date, max_date, table):
    engine = build_postgresql_engine(customizer=customizer)

    sql = sqlalchemy.text(
        f"""
        DELETE
        FROM public.{table}

        WHERE {date_col} BETWEEN :min_date AND :max_date;
        """
    )
    with engine.connect() as con:
        con.execute(sql, min_date=min_date, max_date=max_date)


def clear_postgresql_other_table(customizer, sheet):
    engine = build_postgresql_engine(customizer=customizer)

    sql = sqlalchemy.text(
        f"""
            DELETE
            FROM public.{sheet['table']['name']};

            """
    )
    with engine.connect() as con:
        con.execute(sql)


def insert_postgresql_data(customizer, df, table, if_exists='append', index=False, index_label=None):
    engine = build_postgresql_engine(customizer=customizer)
    with engine.connect() as con:
        df.to_sql(
            table,
            con=con,
            if_exists=if_exists,
            index=index,
            index_label=index_label
        )


def insert_postgresql_other_data(customizer, df, sheet, if_exists='append', index=False, index_label=None):
    engine = build_postgresql_engine(customizer=customizer)

    with engine.connect() as con:
        df.to_sql(
            sheet['table']['name'],
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

    if schema['table']['type'] != 'lookup' and schema['table']['type'] != 'source':
        for index in schema['table']['indexes']:
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
    assert 'columns' in schema['table'].keys(), "Schema not formed properly {}".format(schema)
    assert len(schema['table']['columns']), "Columns not specified in schema for table {}".format(schema['table']['name'])
    stmt = f"CREATE TABLE {schema['table']['schema']}.{schema['table']['name']}"
    stmt += "\n"
    stmt += "(\n"
    col_len = len(schema['table']['columns'])
    idx = 0
    for column in schema['table']['columns']:
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
    stmt += f"ON {schema['table']['schema']}.{schema['table']['name']} USING {index['method']}\n"
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
    stmt = f"ALTER TABLE {schema['table']['schema']}.{schema['table']['name']}\n"
    stmt += f"\tCLUSTER ON {index['name']};"
    return stmt
