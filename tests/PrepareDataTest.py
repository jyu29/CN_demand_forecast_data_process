from datetime import datetime
from pyspark.sql import functions as F
from tests.PySparkTestCase import PySparkTestCase
from src.refining_global import prepare_data as prep
import unittest


class PrepareDataTest(PySparkTestCase):
    def test_get_current_exchange(self):
        current_timestamp = int(datetime.now().timestamp())
        start = datetime.fromtimestamp(current_timestamp - 3600)
        end = datetime.fromtimestamp(current_timestamp + 3600)

        cex = self.spark.createDataFrame(
            [
                (6, 17, 32, start, end, 1.0),
                (6, 17, 32, start, end, 1.0),
                (6, 17, 32, end, end, 1.2),
                (6, 20, 32, start, end, 0.6),
            ],
            ["cpt_idr_cur_price", "cur_idr_currency_base", "cur_idr_currency_restit", "hde_effect_date", "hde_end_date",
             "hde_share_price"]
        )

        results = prep.get_current_exchange(cex).orderBy(F.col("cur_idr_currency_base")).collect()
        expected_results = [(17, 32, 1.0), (20, 32, 0.6)]
        self.assertEqual(set(results), set(expected_results))

    def test_filter_sku(self):
        sku = self.spark.createDataFrame(
            [
                (0,  1),
                (14, 2),
                (89, 3),
                (90, 4),
                (23, None),
                (13, 5),
            ],
            ["unv_num_univers", "mdl_num_model_r3"]
        )
        results = prep.filter_sku(sku).collect()
        expected_results = [(13, 5)]
        self.assertEqual(set(results), set(expected_results))

    def test_filter_sap(self):
        current_timestamp = int(datetime.now().timestamp())
        start = datetime.fromtimestamp(current_timestamp - 3600)
        end = datetime.fromtimestamp(current_timestamp + 3600)
        sap = self.spark.createDataFrame(
            [
                ('PRT', 'Z01', start, end),
                ('PRT', 'Z02', start, end),
                ('PRT', 'Z03', end, end),
                ('NON', 'Z04', start, end),
            ],
            ["sapsrc", "purch_org", "date_begin", "date_end"]
        )
        results = prep.filter_sap(sap, ['Z01', 'Z03', 'Z04']).select("sapsrc", "purch_org").collect()
        expected_results = [('PRT', 'Z01')]
        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
