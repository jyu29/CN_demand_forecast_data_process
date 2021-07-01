from pyspark.sql import functions as F
from spark.test.PySparkTestCase import PySparkTestCase
from spark.src import mag_choices as mc
import unittest


class StocksRetailTest(PySparkTestCase):
    def test_recurse_overlap(self):
        self.assertEqual(mc.recurse_overlap([['2020-01-01', '2020-01-31']]), [['2020-01-01', '2020-01-31']])
        self.assertEqual(
            mc.recurse_overlap([['2020-01-01', '2020-01-31'], ['2020-01-31', '2020-02-28']]),
            [('2020-01-01', '2020-02-28')])
        self.assertEqual(
            mc.recurse_overlap([['2020-01-01', '2020-01-31'], ['2020-01-31', '2020-02-28'], ['2021-01-01', '2021-01-11']]),
            [('2020-01-01', '2020-02-28'), ['2021-01-01', '2021-01-11']])

    def test_get_clean_data(self):
        choices = self.spark.createDataFrame([
            (100, 'Z1', 'Z0', '000123', '000100', '2020-01-01', '2020-09-11', '2020-01-01'),
            (100, 'Z1', 'Z0', '000123', '000100', '2020-01-01', '2020-12-31', '2020-01-02'),
            (100, 'Z1', 'Z0', '000123', '000100', '2020-06-01', '2022-01-01', '2020-01-01'),
            (100, 'Z1', 'Z0', '000123', '000100', '2019-01-02', '2019-02-15', '2019-01-01'),
            (100, 'Z1', 'Z0', '000123', '000100', '2019-01-01', '2019-03-05', '2019-01-01')],
            ["plant_id", "purch_org", "sales_org", "material_id", "model_id", "date_valid_from", "date_valid_to", "date_last_change"])

        results = mc.get_clean_data(choices).orderBy(F.col('date_from')).collect()
        expected_results = [
            (100, 'Z1', 'Z0', 123, 100, '2019-01-01', '2019-03-05'),
            (100, 'Z1', 'Z0', 123, 100, '2020-01-01', '2022-01-01'),
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_get_choices_per_week(self):
        choices = self.spark.createDataFrame([
            (100, '000123', '000100', '2020-01-03', '2020-01-05', '2020-01-01'),
            (100, '000210', '000200', '2020-01-03', '2020-01-05', '2020-01-01'),
            (100, '000310', '000300', '2021-06-10', '2021-06-17', '2020-01-02')],
            ["plant_id", "material_id", "model_id", "date_from", "date_to", "date_last_change"])
        weeks = self.spark.createDataFrame([
                (202001, ''), (202002, ''), (202123, ''), (202124, ''), (202125, '')],
            "week_id int, nan string").drop('nan')
        results = mc.get_choices_per_week(choices, weeks).orderBy(F.col('date_from')).collect()
        expected_results = [
            (202001, 100, '000123', '000100', '2020-01-03', '2020-01-05', '2020-01-01', 202001, 202001),
            (202001, 100, '000210', '000200', '2020-01-03', '2020-01-05', '2020-01-01', 202001, 202001),
            (202123, 100, '000310', '000300', '2021-06-10', '2021-06-17', '2020-01-02', 202123, 202124),
            (202124, 100, '000310', '000300', '2021-06-10', '2021-06-17', '2020-01-02', 202123, 202124),
        ]

        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
