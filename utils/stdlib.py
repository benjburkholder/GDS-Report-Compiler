"""
GRC Standard Library
"""
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
