import os
import pathlib
import pandas as pd

from utils.cls.core import Customizer


class GoogleMyBusiness(Customizer):
    prefix = 'google_my_business'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)

        setattr(self, 'lookup_tables', {
            'status': {
                'table_type': 'gmb',
                'active': True,
                'refresh_status': False,
                'lookup_source_sheet': 'GMB Listing to Property',
                'schema': 'lookup_gmb_schema',
                'table_name': 'lookup_gmb_listingtolocation'
            }}),

        # Schema for GMB lookup table
        setattr(self, f'{self.prefix}_lookup_gmb_schema', {
            'table': 'lookup_gmb_listingtolocation',
            'schema': 'public',
            'type': 'lookup',
            'columns': [
                {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 150},
                {'name': 'address_line_1', 'type': 'character varying', 'length': 250},
                {'name': 'city', 'type': 'character varying', 'length': 50},
                {'name': 'state', 'type': 'character varying', 'length': 50},
                {'name': 'zip', 'type': 'character varying', 'length': 50},
                {'name': 'phone', 'type': 'character varying', 'length': 25},

            ],
            'owner': 'postgres'
        })

        # Schema for GMB source table
        setattr(self, f'{self.prefix}_source_gmb_schema', {
            'table': 'source_gmb_listingtolocation',
            'schema': 'public',
            'type': 'source',
            'columns': [
                {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 150},
                {'name': 'address_line_1', 'type': 'character varying', 'length': 250},
                {'name': 'city', 'type': 'character varying', 'length': 50},
                {'name': 'state', 'type': 'character varying', 'length': 50},
                {'name': 'zip', 'type': 'character varying', 'length': 50},
                {'name': 'phone', 'type': 'character varying', 'length': 25},

            ],
            'owner': 'postgres'
        })


