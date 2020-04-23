"""
Check Models

System script to check if tables need to be built, or are recommended to be re-built
"""
from utils import custom, grc


def main() -> None:
    # read all tables from global Customizer
    schemas = get_all_schemas()
    # for each table
    for customizer, schema in schemas:
        # check that it exists in the data base
        check = grc.check_table_exists(
            customizer=customizer,
            schema=schema
        )
        if check:
            # TODO(jschroeder)
            # if so, check that its data types align with the model
            # if not, raise error recommending the table be rebuilt
            pass
        else:
            # if not, create it using platform
            grc.create_table_from_schema(customizer=customizer, schema=schema)
    return


def get_all_schemas():
    """
    Returns a list of tuple (initialized class, class schema) for all custom.py Customizer implementations
    with a 'schema' defined
    :return:
    """
    schemas = []
    cls_members = custom.get_customizers()
    for cls in cls_members:
        ini_class = cls[1]()
        schema = grc.get_optional_attribute(ini_class, 'schema')
        if schema:
            schemas.append((ini_class, schema))
    return schemas


if __name__ == '__main__':
    main()
