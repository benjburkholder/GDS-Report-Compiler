"""
Systematic Procedure Execution
"""

import subprocess
import os

from utils.grc import systematic_procedure_execution


def main() -> int:

    # Pulls all file names from root directory
    active_files = [os.path.splitext(x)[0] for x in os.listdir()]

    # Pulls names of reporting tables currently active
    active_reporting_tables = systematic_procedure_execution()

    # Isolates active scripts based on files in directory and active reporting tables
    live_scripts = [x for x in active_files for y in active_reporting_tables if x == y]

    # Iterates through scripts, passing in arguments to only run procedures
    for file in live_scripts:
        subprocess.call(['python', f'{file}.py', 'backfill', 'ingest'])

    return 0


if __name__ == '__main__':
    main()



