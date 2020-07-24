"""
Static GRC Configuration
"""

GIT_HUB_KEY = '<API_KEY>'

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
