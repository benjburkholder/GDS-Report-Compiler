"""
Test Data Quality

Check daily, weekly and monthly pull ranges to ensure .
"""

import unittest
import sqlalchemy
import pandas as pd
from datetime import date, timedelta
from utils.dbms_helpers import postgres_helpers


class TestDataQuality(unittest.TestCase):

    # audit daily data
    def test_daily_data(self, start_date, end_date, data_source, marketing_table, customizer):
        daily = self.__pull_daily_data(start_date=start_date,
                                       end_date=end_date,
                                       data_source=data_source,
                                       marketing_table=marketing_table,
                                       customizer=customizer
                                       )
        if daily:
            self.__test_daily(start_date=start_date, end_date=end_date, daily=daily)

    # audit weekly data
    def test_weekly_data(self, start_date, data_source, end_date, marketing_table):
        weekly = self.__pull_weekly_data(start_date=start_date,
                                         end_date=end_date,
                                         data_source=data_source,
                                         marketing_table=marketing_table
                                         )

        if weekly:
            self.__test_weekly(start_date=start_date, end_date=end_date, weekly=weekly)

    # audit monthly data
    def test_monthly_data(self, month, data_source, marketing_table):
        monthly = self.__pull_monthly_data(month=month,
                                           data_source=data_source,
                                           marketing_table=marketing_table
                                           )
        if monthly:
            self.__test_monthly(month=month, monthly=monthly)

    def __pull_daily_data(self, start_date, end_date, data_source, marketing_table, customizer):
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

    def __pull_weekly_data(self, start_date, end_date, data_source, marketing_table):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
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

    def __pull_monthly_data(self, month, data_source, marketing_table):
        engine = postgres_helpers.build_postgresql_engine(customizer=self)
        with engine.connect() as con:
            results = con.execute(
                sqlalchemy.text(
                    f"""
                        SELECT DISTINCT report_date
                        FROM public.{marketing_table}
                        WHERE report_date = :month
                        AND data_source = :data_source;
                        """
                ),
                table=marketing_table,
                month=month,
                data_source=data_source
            ).fetchall()

        return [item[0] for item in results] if results else None

    def __test_daily(self, start_date, end_date, daily: list):
        daily_dates = pd.date_range(start_date, end_date, freq='d')
        new_daily_dates = [daily_date.strftime('%Y-%m-%d') for daily_date in daily_dates]
        new_dates = [daily_date.strftime('%Y-%m-%d') for daily_date in daily]

        self.assertCountEqual(new_daily_dates, new_dates)

    def __test_weekly(self, start_date, end_date, weekly):
        weekly_dates = pd.date_range(start_date, end_date - timedelta(days=1), freq='d')

    def __test_monthly(self, month, monthly):
        pass
