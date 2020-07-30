"""
GMB Reviews Customizer Module
"""
import pandas as pd

from utils.cls.user.gmb import GoogleMyBusiness
from googlemybusiness.reporting.client.listing_report import GoogleMyBusinessReporting


class GoogleMyBusinessReviews(GoogleMyBusiness):

    rename_map = {
        'global': {
            'Date': 'report_date',
            'Reviewer': 'reviewer',
            'Rating': 'rating',
            'Review_Count': 'review_count',
            'Average_Rating': 'average_rating',
            'Listing_ID': 'listing_id',
            'Listing_Name': 'listing_name'
        }
    }

    def __init__(self):
        super().__init__()
        self.set_attribute('class', True)
        self.set_attribute('debug', True)
        self.set_attribute('historical', False)
        self.set_attribute('historical_start_date', '2020-01-01')
        self.set_attribute('historical_end_date', '2020-01-02')
        self.set_attribute('table', self.prefix)
        self.set_attribute('data_source', 'Google My Business - Reviews')
        self.set_attribute('schema', {'columns': []})

        # set whether this data source is being actively used or not
        self.set_attribute('active', True)

        # noinspection PyMethodMayBeStatic

    def type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Type columns for safe storage (respecting data type and if needed, length)
        :param df:
        :return:
        """
        for column in self.get_attribute('schema')['columns']:
            if column['name'] in df.columns:
                if column['type'] == 'character varying':
                    assert 'length' in column.keys()
                    df[column['name']] = df[column['name']].apply(lambda x: str(x)[:column['length']] if x else None)
                elif column['type'] == 'bigint':
                    df[column['name']] = df[column['name']].apply(lambda x: int(x) if x else None)
                elif column['type'] == 'double precision':
                    df[column['name']] = df[column['name']].apply(lambda x: float(x) if x else None)
                elif column['type'] == 'date':
                    df[column['name']] = pd.to_datetime(df[column['name']])
                elif column['type'] == 'timestamp without time zone':
                    df[column['name']] = pd.to_datetime(df[column['name']])
                elif column['type'] == 'datetime with time zone':
                    # TODO(jschroeder) how better to interpret timezone data?
                    df[column['name']] = pd.to_datetime(df[column['name']], utc=True)
        return df

    def parse(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO: UPDATE AND DOCUMENT
        del df['Data_Source']

        return df

    def post_processing(self) -> None:
        """
        Handles custom sql UPDATE / JOIN post-processing needs for reporting tables,
        :return:
        """

        # TODO: UPDATE THIS TO BE LIKE GA.py
        # CUSTOM SQL QUERIES HERE, ADD AS MANY AS NEEDED
        sql = """ CUSTOM SQL HERE """

        sql2 = """ CUSTOM SQL HERE """

        custom_sql = [
            sql,
            sql2
        ]
        with self.engine.connect() as con:
            for query in custom_sql:
                con.execute(query)
        return

    def pull(self):
        # todo: update gmb library to pass a customizer with credentials

        gmb_client = GoogleMyBusinessReporting(
            customizer=self
        )
        accounts = self.get_filtered_accounts(gmb_client=gmb_client)
        for account in accounts:
            # get account name using first key (account human name) to access API Name
            account_name = account[list(account.keys())[0]]['API_Name']

            # get all listings
            listings = gmb_client.get_listings(account=account_name)

            # for each listing, get insight data
            for listing in listings:
                listing_id = listing['storeCode']
                report = gmb_client.get_reviews(
                    start_date=None,
                    end_date=None,
                    historical=True,
                    listing_name=listing['name'])
                if report.shape[0]:
                    df = pd.DataFrame(report)
                    df = self.assign_average_rating(df=df)
                    df['Listing_ID'] = listing_id
                    df['Listing_Name'] = listing['locationName']
                    df['account_name'] = account_name
                    df['data_source'] = self.get_attribute('data_source')
                    df['property'] = None
                    df.rename(columns=self.__get_rename_map(account_name='global'), inplace=True)
                    df = self.type(df=df)
                    # self.ingest_by_listing_id(listing_id=listing_id, df=df, start_date=start, end_date=end)
                else:
                    print('INFO: No data returned for ' + str(listing))
