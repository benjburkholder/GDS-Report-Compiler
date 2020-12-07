"""
Test Data Quality

Check daily, weekly and monthly pull ranges to ensure .
"""

import calendar
import datetime
import unittest
import sqlalchemy
import pandas as pd
from datetime import date, timedelta
from utils.dbms_helpers import postgres_helpers


class TestDataQuality(unittest.TestCase):

    def __init__(self):
        super().__init__()
        self.audit_message = ''

    # audit daily data
    def test_daily_data(self, start_date, end_date, data_source, marketing_table, customizer):
        daily = self.__pull_daily_data(start_date=start_date,
                                       end_date=end_date,
                                       data_source=data_source,
                                       marketing_table=marketing_table,
                                       customizer=customizer
                                       )
        if daily:
            self.__test_daily(start_date=start_date, end_date=end_date, daily=daily, data_source=data_source)

    # audit weekly data
    def test_weekly_data(self, start_date, data_source, end_date, marketing_table, customizer):
        weekly = self.__pull_weekly_data(start_date=start_date,
                                         end_date=end_date,
                                         data_source=data_source,
                                         marketing_table=marketing_table,
                                         customizer=customizer
                                         )

        if weekly:
            self.__test_weekly(start_date=start_date, end_date=end_date, weekly=weekly, data_source=data_source)

    # audit monthly data
    def test_monthly_data(self, start_date, data_source, marketing_table, customizer):
        monthly = self.__pull_monthly_data(start_date=start_date,
                                           data_source=data_source,
                                           marketing_table=marketing_table,
                                           customizer=customizer
                                           )
        if monthly:
            self.__test_monthly(start_date=start_date, data_source=data_source, monthly=monthly)

    @staticmethod
    def __pull_daily_data(start_date, end_date, data_source, marketing_table, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            results = con.execute(
                sqlalchemy.text(
                    f"""
                        SELECT DISTINCT report_date
                        FROM public.{marketing_table}
                        WHERE report_date BETWEEN :start_date AND :end_date
                        AND data_source = :data_source;
                        """
                ),
                table=marketing_table,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source
            ).fetchall()

        return [item[0] for item in results] if results else None

    @staticmethod
    def __pull_weekly_data(start_date, end_date, data_source, marketing_table, customizer):
        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            results = con.execute(
                sqlalchemy.text(
                    f"""
                        SELECT DISTINCT report_date
                        FROM public.{marketing_table}
                        WHERE report_date BETWEEN :start_date AND :end_date
                        AND data_source = :data_source;
                        """
                ),
                table=marketing_table,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source
            ).fetchall()

        return [item[0] for item in results] if results else None

    @staticmethod
    def __pull_monthly_data(start_date, data_source, marketing_table, customizer):

        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        month_range = calendar.monthrange(start_date.year, start_date.month -1)

        first_date = f'{start_date.year}-{start_date.month - 1}-01'
        last_date = f'{start_date.year}-{start_date.month - 1}-{month_range[1]}'

        engine = postgres_helpers.build_postgresql_engine(customizer=customizer)
        with engine.connect() as con:
            results = con.execute(
                sqlalchemy.text(
                    f"""
                        SELECT DISTINCT report_date
                        FROM public.{marketing_table}
                        WHERE report_date BETWEEN :first_date AND :last_date
                        AND data_source = :data_source;
                        """
                ),
                table=marketing_table,
                first_date=first_date,
                last_date=last_date,
                data_source=data_source
            ).fetchall()

        return [item[0] for item in results] if results else None

    def __test_daily(self, start_date, end_date, daily: list, data_source):
        daily_dates = pd.date_range(start_date, end_date, freq='d')
        new_daily_dates = [daily_date.strftime('%Y-%m-%d') for daily_date in daily_dates]
        new_dates = [daily_date.strftime('%Y-%m-%d') for daily_date in daily]

        try:
            self.assertCountEqual(new_daily_dates, new_dates)

        except AssertionError as error:
            missing_dates = [date for date in new_dates if date not in new_daily_dates]
            self.audit_message += f'{data_source}: FAILED\nMESSAGE: {error}\nMISSING DATES: {missing_dates}'
            print(f'{data_source}: FAILED\nMESSAGE: {error}\nMISSING DATES: {missing_dates}')

        else:
            self.audit_message += f'{data_source}: PASSED\n'
            print(f'{data_source}: PASSED')

    def __test_weekly(self, start_date, end_date, weekly, data_source):
        weekly_dates = pd.date_range(start_date, end_date, freq='d')
        new_weekly_dates = [weekly_date.strftime('%Y-%m-%d') for weekly_date in weekly_dates]
        new_week_dates = [daily_date.strftime('%Y-%m-%d') for daily_date in weekly]

        mutual_dates = [date for date in new_week_dates if date in new_weekly_dates]

        try:
            assert len(mutual_dates) > 0

        except AssertionError as error:
            missing_dates = [date for date in new_week_dates if date not in new_weekly_dates]
            self.audit_message += f'{data_source}: FAILED\nMESSAGE: {error}\nMISSING DATES: f{missing_dates}'

        else:
            self.audit_message += f'{data_source}: PASSED\n'
            print(f'{data_source}: PASSED')

    def __test_monthly(self, start_date, data_source, monthly):

        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        report_month = f'{start_date.year}-{start_date.month - 1}'

        try:
            assert len(monthly) > 0

        except AssertionError as error:
            self.audit_message += f'{data_source}: FAILED\nMESSAGE: {error}\nNO DATA FOR MONTH: f{report_month}'

        else:
            self.audit_message += f'{data_source}: PASSED\n'
            print(f'{data_source}: PASSED')

