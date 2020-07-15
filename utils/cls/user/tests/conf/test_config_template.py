"""
Test Configuration Template
"""
dbms = 'postgresql'
client = 'TestSuite'  # Enter in camel case
project = 'GRC'
version = '2020.2'
recipients = [
    # EMAILS HERE for error notifications
    'jschroeder@linkmedia360.com',
    'bburkholder@linkmedia360.com'
]
db = {
    'DATABASE': '<DB_NAME>',
    'USERNAME': 'python-2',
    'PASSWORD': 'pythonpipelines',
    'SERVER': '35.222.11.147'
}
