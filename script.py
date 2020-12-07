"""
Data Processing Script
"""
import sys
import traceback
from utils import grc
from conf import static
from utils.cls.pltfm.gmail import send_error_email
from utils.cls.pltfm.marketing_data import execute_post_processing_scripts_for_process


def main(argv) -> int:
    # extract our supported command line arguments
    if len(argv) == 1:
        script_name = static.DEBUG_SCRIPT_NAME
        pull = 1
        ingest = 1
        backfilter = 1
        expedited = 1
        debug = 1
    else:
        script_name = argv[1]
        pull, ingest, backfilter, expedited, debug = grc.get_args(argv=argv)

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(
        script_name=script_name,
        expedited=expedited
    )

    if debug:
        print("WARN: Error reporting disabled and expedited runtime mode activated")

    try:
        if pull:
            customizer.pull()
        if backfilter:
            customizer.backfilter()
        if ingest:
            customizer.ingest()

        # find post processing SQL scripts with this file's name as a search key and execute
        post_processing_workflow(script_name=script_name)

    except Exception as error:
        if not debug:
            send_error_email(
                to=customizer.recipients,
                client_name=customizer.client,
                error=error,
                engine=grc.create_application_sql_engine(),
                script_name=script_name,
                stack_trace=traceback.format_exc(),
            )

        raise error

    # todo: wait until data is fully backfilled to do this
    customizer.audit()

    return 0


def post_processing_workflow(script_name: str) -> None:
    execute_post_processing_scripts_for_process(
        script_filter=script_name
    )


if __name__ == '__main__':
    main(argv=sys.argv)
