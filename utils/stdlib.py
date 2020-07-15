"""
GRC Standard Library
"""
import os
from importlib import util

EXIT_SUCCESS = 0
EXIT_FAILURE = -1
EXIT_WARNING = 1


# https://www.journaldev.com/22576/python-import#python-import-class-from-another-file
def module_from_file(module_name, file_path):
    spec = util.spec_from_file_location(module_name, file_path)
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def get_base_path():
    return os.path.abspath(
        os.path.dirname(
            os.path.dirname(__file__)
        )
    )


def get_venv_path():
    if os.name == 'nt':
        return os.path.join(
            get_base_path(),
            'venv',
            'Scripts',
            'python.exe'
        )
    else:
        return  os.path.join(
            get_base_path(),
            'venv',
            'bin',
            'python'
        )
