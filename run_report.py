#! /Users/benburkholder/Documents/GitHub/GDS-Report-Compiler/venv
from utils.cls.core import Customizer
import subprocess
import os


def main():
    expedited = 'run'
    for sheets in Customizer.CONFIGURATION_WORKBOOK['sheets']:

        if sheets['table']['type'] == 'reporting':
            indicator = [file for file in os.listdir('./') if sheets['table']['name'] in file]

            if indicator:
                subprocess.call(['python', f"{sheets['table']['name']}.py", f"{expedited}"], shell=True)
                expedited = 'skip'


if __name__ == '__main__':
    main()
