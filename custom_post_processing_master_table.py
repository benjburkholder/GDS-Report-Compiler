"""
Master Table - Custom Post Processing
"""
import traceback
from utils.dbms_helpers import postgres_helpers
from utils.cls.core import Customizer
from utils import grc
from utils.cls.pltfm.gmail import send_error_email
SCRIPT_NAME = grc.get_script_name(__file__)


def custom_post_processing():

    # If there are custom columns added to master, execute custom queries
    if Customizer.custom_marketing_data_columns['table']['active']:
        engine = postgres_helpers.build_postgresql_engine(customizer=Customizer)

        # CUSTOM SQL QUERIES HERE, ADD AS MANY AS NEEDED
        sql = """ CUSTOM SQL HERE """

        custom_sql = [
            sql
        ]

        with engine.connect() as con:
            for query in custom_sql:
                print(f'Executing Query: {query}')
                con.execute(query)


if __name__ == '__main__':
    try:
        custom_post_processing()
    except Exception as error:
        send_error_email(
            client_name=Customizer.client,
            script_name=SCRIPT_NAME,
            to=Customizer.recipients,
            error=error,
            stack_trace=traceback.format_exc(),
            engine=grc.create_application_sql_engine()
        )
        raise
