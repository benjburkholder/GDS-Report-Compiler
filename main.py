"""
Main Reporting Workflow Script
"""
import os
import sys
import subprocess
from utils import grc
from utils.queue_manager import QueueManager


def main(argv) -> None:
    workflow = QueueManager().get()

    root = os.path.abspath(os.path.dirname(__file__))
    venv_path = grc.build_path_by_os(root=root)
    script_path = os.path.join(root, 'script.py')

    if len(argv) > 1:
        # for each script in workflow.json
        for work_item in workflow:
            if work_item['active']:
                name = work_item['name']
                pull, ingest, backfilter, expedited = grc.get_args(argv=argv)

                # execute with the configured command line args
                call_args = [
                    venv_path,
                    script_path,
                    name,
                    f'--pull={pull}',
                    f'--ingest={ingest}',
                    f'--backfilter={backfilter}',
                    f'--expedited={expedited}'
                ]
                subprocess.call(
                    call_args
                )
        return

    else:
        # for each script in workflow.json
        for work_item in workflow:
            if work_item['active']:
                name = work_item['name']
                args = work_item['args']
                pull = args['pull']
                ingest = args['ingest']
                backfilter = args['backfilter']
                expedited = args['expedited']
                # execute with the configured command line args
                call_args = [
                    venv_path,
                    script_path,
                    name,
                    f'--pull={pull}',
                    f'--ingest={ingest}',
                    f'--backfilter={backfilter}',
                    f'--expedited={expedited}'
                ]
                print(call_args)
                subprocess.call(
                    call_args
                )
        return


if __name__ == '__main__':
    main(argv=sys.argv)