"""
Custom

This script is where all reporting configuration takes place
"""


class Customizer:
    """
    Required to run scripts
    Manages all report data transformation and customization
    """
    # attributes
    prefix = ''

    # GLOBALS - REQUIRED TO BE REFERENCED FOR ALL PROJECTS
    required_attributes = [
        'dbms',
        'client',
        'project',
        'version',
        'recipients'
    ]

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
        assert self.valid_global_configuration(), self.global_configuration_message

    def valid_global_configuration(self) -> bool:
        for attribute in self.required_attributes:
            if not getattr(self, attribute):
                return False
            if attribute == 'dbms':
                if getattr(self, attribute) not in self.supported_dbms:
                    return False
        return True
