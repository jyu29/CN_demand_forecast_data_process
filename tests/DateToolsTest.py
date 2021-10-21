from tests.PySparkTestCase import PySparkTestCase
from src.tools import date_tools
import unittest
import datetime as dt


class DateToolsTest(PySparkTestCase):
    def test_get_week_id(self):
        self.assertEqual(date_tools.get_week_id(dt.date(2021, 5, 31)), 202122)
        self.assertEqual(date_tools.get_week_id(dt.date(2021, 5, 30)), 202122) #sunday
        self.assertEqual(date_tools.get_week_id(dt.date(2021, 1, 1)), 202053) #first day of year
        self.assertEqual(date_tools.get_week_id(dt.date(2021, 1, 3)), 202101)

    def test_get_first_day_month(self):
        self.assertEqual(date_tools.get_first_day_month(202122).strftime("%Y-%m-%d"), '2021-05-01')
        self.assertEqual(date_tools.get_first_day_month(202053).strftime("%Y-%m-%d"), '2020-12-01')
        self.assertEqual(date_tools.get_first_day_month(202101).strftime("%Y-%m-%d"), '2021-01-01')

    def test_get_last_day_week(self):
        self.assertEqual(date_tools.get_last_day_week(202122).strftime("%Y-%m-%d"), '2021-06-05')
        self.assertEqual(date_tools.get_last_day_week(202053).strftime("%Y-%m-%d"), '2021-01-02')
        self.assertEqual(date_tools.get_last_day_week(202101).strftime("%Y-%m-%d"), '2021-01-09')

    def test_get_previous_n_week(self):
        self.assertEqual(date_tools.get_previous_n_week(202122, 2), 202120)
        self.assertEqual(date_tools.get_previous_n_week(202122, 32), 202043)

    def test_get_next_n_week(self):
        self.assertEqual(date_tools.get_next_n_week(202122, 2), 202124)
        self.assertEqual(date_tools.get_next_n_week(202122, 31), 202201)


if __name__ == "__main__":
    unittest.main()
