"""
Static GRC Configuration
"""

UPDATE_KEY = '32e58f63114435f643f2c88617a02a5ba03e1e91'
UPDATE_USERNAME = 'jwschroeder330'
UPDATE_REPOSITORY = 'GDS-Report-Compiler'

# specify the repository name and version
REQUIRED_PACKAGES = [
    {
        'name': 'google-analytics-py',
        'pip_alias': 'googleanalyticspy',
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
