import os
import pathlib
import datetime
import sqlalchemy

from utils.cls.core import Customizer
from utils.dbms_helpers import postgres_helpers


class Moz(Customizer):

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_secrets_path',
                str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parents[2]))
        setattr(self, f'{self.prefix}_client_name', self.client)

    def pull_moz_local_accounts(self, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_localaccountmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            accounts_cleaned = [{'account': account[0], 'label': account[1]} for account in accounts_raw if
                                accounts_raw]

            return accounts_cleaned

    def pull_moz_pro_accounts(self, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                f"""
                SELECT *
                FROM public.source_moz_procampaignmaster;
                """
            )

            result = con.execute(sql)
            accounts_raw = result.fetchall()

            campaign_ids = [{'campaign_id': campaign_id} for campaign_id in accounts_raw[0]]

            return campaign_ids

    def exclude_moz_directories(self, customizer, df):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            sql = sqlalchemy.text(
                """
                SELECT *
                FROM public.source_moz_directoryexclusions;
                """
            )

            result = con.execute(sql)
            exclusions_raw = result.fetchall()

            exclusion_list = [exclusion[0] for exclusion in exclusions_raw]

            if exclusion_list:
                df_cleaned = df.loc[
                             ~(df['directory'].isin(exclusion_list))
                ,
                             :

                             ]

            return df_cleaned if True else df


class MozProRankingsCustomizer(Moz):
    prefix = 'moz_pro_rankings'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_report_date', datetime.date(2020, 1, 1))
        setattr(self, f'{self.prefix}_table', 'mozpro_rankings')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Pro - Rankings',
            'property': None
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozprorankings_audit',
            'active': 1,
            'code': """

                    """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozProSERPCustomizer(Moz):
    prefix = 'moz_pro_serp'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_report_date', datetime.date(2020, 1, 1))
        setattr(self, f'{self.prefix}_table', 'mozpro_serp')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Pro - SERP',
            'property': None
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozproserp_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozLocalVisibilityCustomizer(Moz):
    prefix = 'moz_local_visibility'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-02-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-02-15')
        setattr(self, f'{self.prefix}_table', 'mozlocal_directory_visibility_report_mdd')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Local - Visibility Report',
            'property': None
        })


        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozlocalvisibility_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


class MozLocalSyncCustomizer(Moz):
    prefix = 'moz_local_sync'

    def __init__(self):
        super().__init__()
        setattr(self, f'{self.prefix}_class', True)
        setattr(self, f'{self.prefix}_debug', True)
        setattr(self, f'{self.prefix}_historical', True)
        setattr(self, f'{self.prefix}_historical_start_date', '2020-02-01')
        setattr(self, f'{self.prefix}_historical_end_date', '2020-02-10')
        setattr(self, f'{self.prefix}_table', 'mozlocal_directory_sync_report_mdd')

        # Used to set columns which vary from data source and client vertical
        setattr(self, f'{self.prefix}_custom_columns', {
            'data_source': 'Moz Local - Sync',
            'property': None
        })

        # audit procedure
        setattr(self, f'{self.prefix}_audit_procedure', {
            'name': 'mozlocalsync_audit',
            'active': 1,
            'code': """

                            """,
            'return': 'integer',
            'owner': 'postgres'
        })


