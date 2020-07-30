"""
Static GRC Configuration
"""
DEBUG = True
DEBUG_SCRIPT_NAME = 'google_analytics_traffic'

UPDATE_KEY = '32e58f63114435f643f2c88617a02a5ba03e1e91'
UPDATE_USERNAME = 'jwschroeder330'
UPDATE_REPOSITORY = 'GDS-Report-Compiler'

SHEETS = {
    # which emails to share new workbook with?
    'WHITELIST_EMAILS': [
        'linkmedia360mcc@gmail.com'
    ]
}

# specify the repository name and version
REQUIRED_PACKAGES = [
    {
        'name': 'google-analytics-py',
        'pip_alias': 'googleanalyticspy',
        'version': 'latest'
    },
    {
        'name': 'google-my-business-py',
        'pip_alias': 'googlemybusiness',
        'version': 'latest'
    }
]

ENTITY_COLS = {
    1: {
        'name': 'property',
        'default_db_value': 'NULL'
    },
    2: {
        'name': 'community',
        'default_db_value': 'NULL'
    },
    3: {
        'name': 'service_line',
        'default_db_value': 'NULL'
    }
}
