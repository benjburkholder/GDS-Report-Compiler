"""
Data Processing Script
"""
import traceback
import sys

from utils.cls.pltfm.gmail import send_error_email
from conf import static
from utils.cls.core import Customizer
from utils import grc
from utils.cls.pltfm.marketing_data import execute_post_processing_scripts_for_process
if static.DEBUG:
    print("WARN: Error reporting disabled and expedited runtime mode activated")


def main(argv) -> int:
    # extract our supported command line arguments
    if len(argv) == 1:
        script_name = static.DEBUG_SCRIPT_NAME
        pull = 1
        ingest = 1
        backfilter = 1
        expedited = 1
    else:
        script_name = argv[1]
        pull, ingest, backfilter, expedited = grc.get_args(argv=argv)

    # run startup data source checks and initialize data source specific customizer
    customizer = grc.setup(
        script_name=script_name,
        expedited=expedited
    )

    if pull:
        customizer.pull()
    if backfilter:
        customizer.backfilter()
    if ingest:
        # TODO: need to address the ingest workflow
        # customizer.ingest()
        pass

    # these stages happen at the end no matter what
    # todo: wait until data is fully backfilled to do this
    # customizer.audit()

    # find post processing SQL scripts with this file's name as a search key and execute
    post_processing_workflow(script_name=script_name)
    return 0


def audit_workflow(customizer: Customizer) -> None:
    grc.audit_automation(customizer=customizer)


def post_processing_workflow(script_name: str) -> None:
    execute_post_processing_scripts_for_process(
        script_filter=script_name
    )


if __name__ == '__main__':
    try:
        main(argv=sys.argv)
    except Exception as error:
        if not static.DEBUG:
            '''
            send_error_email(
                client_name=Customizer.client,
                script_name=SCRIPT_NAME,
                to=Customizer.recipients,
                error=error,
                stack_trace=traceback.format_exc(),
                engine=grc.create_application_sql_engine()
            )
            '''
            pass
        raise
