""""
Inquiry Web Goals Customizer Module
"""

# PLATFORM IMPORTS
from utils.cls.user.inquiry import Inquiry

# CUSTOM IMPORTS
HISTORICAL = False
HISTORICAL_START_DATE = '2020-01-01'
HISTORICAL_END_DATE = '2020-07-01'
TABLE_SCHEMA = 'public'
DATE_COL = 'report_date'
DATA_SOURCE = 'Goals - Web Inquiries'


class InquiryWebGoals(Inquiry):

    rename_map = {
        'global': {

            'Date': 'report_date',
            'Property': 'property',
            'Community': 'community',
            'Ownership_Group': 'ownership_group',
            'Region': 'region',
            'Daily_Cost': 'daily_cost'

        }
    }

    post_processing_sql_list = []

    def __get_post_processing_sql_list(self) -> list:
        """
        If you wish to execute post-processing on the SOURCE table, enter sql commands in the list
        provided below
        ====================================================================================================
        :return:
        """
        # put this in a function to leave room for customization
        return self.post_processing_sql_list

    def __init__(self):
        super().__init__()
        self.set_attribute('table_schema', TABLE_SCHEMA)
        self.set_attribute('historical', HISTORICAL)
        self.set_attribute('historical_start_date', HISTORICAL_START_DATE)
        self.set_attribute('historical_end_date', HISTORICAL_END_DATE)
        self.set_attribute('date_col', DATE_COL)
        self.set_attribute('table', self.prefix)
        self.set_attribute('class', True)
        self.set_attribute('data_source', DATA_SOURCE)
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

    def pull(self):
        gs = self.create_gs_object()
        df = gs.get_spreadsheet_by_name(
            worksheet_name='Inquiry Goals',
            workbook_name='National Church Residences | Configuration Workbook (v2020.2.1)'
        )

        if df.shape[0]:
            df = self.calculate_inquiry_web_goals(raw_web_goals=df)
            df.rename(columns=self.rename_map['global'], inplace=True)

            # float dtypes
            df['daily_cost'] = df['daily_cost'].astype(float)
            df['data_source'] = DATA_SOURCE

            self.ingest_all(df=df)






