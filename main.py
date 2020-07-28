"""
Main Reporting Workflow Script
"""
import os
import subprocess
from utils.queue_manager import QueueManager


def main() -> None:
    workflow = QueueManager().get()

    root = os.path.abspath(os.path.dirname(__file__))
    venv_path = os.path.join(root, 'venv', 'Scripts', 'python.exe')
    script_path = os.path.join(root, 'script.py')

    # for each script in workflow.json
    for work_item in workflow:
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


