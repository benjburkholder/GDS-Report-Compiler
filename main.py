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

    # single logical branch - either get the args or use what is provided
    if len(argv) > 1:
        use_args = False
        pull, ingest, backfilter, expedited, debug = grc.get_args(argv=argv)
    else:
        use_args = True
        pull = None
        ingest = None
        backfilter = None
        expedited = None
        debug = None

    # iterate and pull each script in workflow.json
    for work_item in workflow:
        if work_item['active']:
            name = work_item['name']
            args = work_item['args']
            
            # only extract items from the workflow if args have not already been sent
            pull = args['pull'] if use_args else pull
            ingest = args['ingest'] if use_args else ingest
            backfilter = args['backfilter'] if use_args else backfilter
            expedited = args['expedited'] if use_args else expedited
            debug = args['debug'] if use_args else debug
            
            # execute with the configured command line args
            call_args = [
                venv_path,
                script_path,
                name,
                f'--pull={pull}',
                f'--ingest={ingest}',
                f'--backfilter={backfilter}',
                f'--expedited={expedited}',
                f'--debug={debug}'
            ]
            subprocess.call(
                call_args
            )
    return


if __name__ == '__main__':
    main(argv=sys.argv)
