"""
Custom

This script is where all reporting configuration takes place
"""
import re


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization
    """
    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'dbms',
        'client',
        'project',
        'version',
        'recipients'
    ]

    CLIENT_NAME = '<NAME>'

    CONFIGURATION_WORKBOOK = {
        'config_sheet_name': '<CONFIG SHEET NAME>',
        'refresh_dates': [1, 15],
        'sheets': [
            {'sheet': '<SHEET NAME>', 'table': '<SOURCE TABLE NAME>'},
            {'sheet': '<SHEET NAME>', 'table': '<SOURCE TABLE NAME>'},
            {'sheet': '<SHEET NAME>', 'table': '<SOURCE TABLE NAME>'}
        ]}

    supported_dbms = [
        'postgresql'
    ]

    global_configuration_message = "Invalid global configuration. Please check your Customizer class and try again"

    # ### START EDITING HERE ###
    dbms = 'postgresql'
    client = '<CLIENT>'
    project = '<PROJECT>'
    version = '<VERSION>'
    recipients = [
        # EMAILS HERE for error notifications
        'jschroeder@linkmedia360.com',
        'bburkholder@linkmedia360.com'
    ]
    db = {
        'DATABASE': '<DATABASE>',
        'USERNAME': 'python-2',
        'PASSWORD': 'pythonpipelines',
        'SERVER': '35.222.11.147'
    }
    # ### END EDITING ###

    def __init__(self):
        self.prefix = self.get_class_prefix()
        self.set_function_prefixes()
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
            if attribute == 'dbms':
                if getattr(self, attribute) not in self.supported_dbms:
                    return False
        return True

    def get_class_prefix(self):
        cls_name = self.__class__.__name__.replace('Customizer', '')
        return re.sub(r'(?<!^)(?=[A-Z])', '_', cls_name).lower()

    def generate_attribute_prefix(self, attrib):
        return f"{self.prefix}_{attrib}"

    def set_attribute(self, attrib, value):
        setattr(self, self.generate_attribute_prefix(attrib=attrib), value)

    def set_function_prefixes(self) -> None:
        for func in dir(self):
            if (callable(getattr(self, func))) and not (re.match(r'_+.*', func)):
                if not hasattr(self, self.generate_attribute_prefix(attrib=func)):
                    setattr(self, self.generate_attribute_prefix(attrib=func), getattr(self, func))

