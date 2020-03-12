import os
import pathlib
import pandas as pd

from utils.cls.core import Customizer


class Moz(Customizer):
    prefix = 'moz'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)

        setattr(self, 'lookup_tables', {
            'status': {
                'table_type': 'moz',
                'active': True,
                'refresh_status': False,
                'lookup_source_sheet': 'Moz Listing to Property',
                'schema': 'lookup_moz_schema',
                'table_name': 'lookup_moz_listingtolocaton'
            }}),

        # Schema for Moz lookup table
        setattr(self, f'{self.prefix}_lookup_moz_schema', {
            'table': 'lookup_moz_listingtolocation',
            'schema': 'public',
            'type': 'lookup',
            'columns': [
                {'name': 'listing_id', 'type': 'character varying', 'length': 100},
                {'name': 'property', 'type': 'character varying', 'length': 150},
                {'name': 'account', 'type': 'character varying', 'length': 150},
                {'name': 'label', 'type': 'character varying', 'length': 150},
                {'name': 'name', 'type': 'character varying', 'length': 150},
                {'name': 'address', 'type': 'character varying', 'length': 250},
                {'name': 'city', 'type': 'character varying', 'length': 50},
                {'name': 'state', 'type': 'character varying', 'length': 50},
                {'name': 'zip', 'type': 'bigint'},
                {'name': 'phone', 'type': 'character varying', 'length': 25},

            ],
            'owner': 'postgres'
        })


class MozProCustomizer(Moz):
    prefix = 'moz_pro'


class MozLocalCustomizer(Moz):
    prefix = 'moz_local'
    pass


