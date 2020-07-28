"""
Main Reporting Workflow Script
"""
import sys_update
from utils.queue_manager import QueueManager


def main() -> None:
    workflow = QueueManager().get()

    # for each script in workflow.json
    for work_item in workflow:

    # execute with the configured command line args

    return


