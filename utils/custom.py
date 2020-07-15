"""
Custom

This script is where all reporting configuration takes place
"""
import os
import inspect
from .stdlib import module_from_file
from utils.cls.core import Customizer
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
USER_DEFINED_CLASS_PATH = os.path.join(BASE_DIR, 'cls', 'user')


# NO EDITING BEYOND THIS POINT
# ````````````````````````````````````````````````````````````````````````````````````````````````````
# GRC UTILITY FUNCTIONS
def get_customizers() -> list:
    """
    Return all custom.py Class objects in [(name, <class>)] format
    :return:
    """
    cls_members = []
    for script in os.listdir(USER_DEFINED_CLASS_PATH):
        if script != '__pycache__':
            mdle = module_from_file(script.replace('.py', ''), os.path.join(USER_DEFINED_CLASS_PATH, script))
            cls_members.extend(inspect.getmembers(mdle, inspect.isclass))
    return cls_members  # as list


def get_customizer(calling_file: str) -> Customizer:
    """
    Loop through and initialize each class in custom.py
    Return the proper Customizer instance that will have the necessary attributes, methods and schema
    for the calling file
    :param calling_file:
    :return:
    """
    cls_members = get_customizers()
    target_attribute = f'{calling_file}_class'
    for cls in cls_members:
        ini_cls = cls[1]()  # initialize the class
        if hasattr(ini_cls, target_attribute):
            if getattr(ini_cls, target_attribute):
                return ini_cls
    raise AssertionError("No configured classes for data source {}".format(calling_file))
