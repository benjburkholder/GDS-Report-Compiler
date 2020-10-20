"""
Dialogtech Call Detail Customizer Module
"""

import datetime

# PLATFORM IMPORTS
from utils.cls.user.dt import Dialogtech
from dialogtech.reporting.client.call_detail import CallDetailReporting

# CUSTOM IMPORTS
IS_CLASS = True
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
DATA_SOURCE = 'DialogTech - Call Detail'


class DialogtechCallDetailCustomizer(Dialogtech):
    """
    Handles DT pulling, parsing and processing
    """

    rename_map = {
        'global': {

            'call_date': 'report_date',
        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', IS_CLASS)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})

    def pull(self):

        start_date = self.calculate_date(start_date=True)
        end_date = self.calculate_date(start_date=False)

        dialog_tech = CallDetailReporting(vertical=self.vertical)

        phone_labels = self.pull_dialogtech_labels()

        for phone_label in phone_labels:
            df = dialog_tech.get_call_detail_report(
                start_date=start_date,
                end_date=end_date,
                phone_label=phone_label['phone_label']
            )

            if df.shape[0]:
                df['data_source'] = DATA_SOURCE
                df['property'] = phone_label['property']
                df['community'] = None
                df['medium'] = phone_label['medium']
                df = self.type(df=df)
                rename_map = self.get_rename_map(phone_label=phone_label['phone_label'])
                df.rename(columns=rename_map, inplace=True)

                df = df[[
                    'report_date',
                    'data_source',
                    'property',
                    'campaign',
                    'medium',
                    'number_dialed',  # call tracking number
                    'caller_id',
                    'call_duration',
                    'transfer_to_number',  # terminating number
                    'phone_label',
                    'call_transfer_status',
                    'client_id'
                ]]

                self.ingest_by_phone_label(
                    df=df,
                    phone_label=phone_label['phone_label'],
                    start_date=start_date,
                    end_date=end_date
                )

            else:
                print('INFO: No data returned for ' + str(phone_label['phone_label']))

