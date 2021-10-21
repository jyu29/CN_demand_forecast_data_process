from datetime import datetime
from pyspark.sql import functions as F
from tests.PySparkTestCase import PySparkTestCase
from src.global import model_week_mrp as mrp
import unittest


class ModelWeekMrpTest(PySparkTestCase):
    def test_get_sku_mrp_update(self):
        current_timestamp = int(datetime.now().timestamp())
        start = datetime.fromtimestamp(current_timestamp - 3600)
        end = datetime.fromtimestamp(current_timestamp + 3600)
        sap = self.spark.createDataFrame([('100',)], ["plant_id"])
        sku = self.spark.createDataFrame(
            [
                (23, '23', start, end),
                (35, '35', start, end),
                (48, '48', start, start),
            ],
            ["mdl_num_model_r3", "sku_num_sku_r3", "sku_date_begin", "sku_date_end"]
        )
        gdw = self.spark.createDataFrame(
            [
                ('PRT', '8', '100', '23', '01-01', '15-02'),
                ('PRT', '9', '100', '35', '15-06', '31-08'),
                ('PRT', '7', '100', '48', '15-09', '15-10'),
            ],
            ["sdw_sap_source", "sdw_material_mrp", "sdw_plant_id", "sdw_material_id", "date_begin", "date_end"]
        )
        results = mrp.get_sku_mrp_update(gdw, sap, sku).orderBy(F.col("sku_id").asc()).collect()
        expected_results = [
            ('01-01', '15-02', '23', 23, 8),
            ('15-06', '31-08', '35', 35, 9),
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_get_model_week_mrp(self):
        day = self.spark.createDataFrame(
            [
                ('2021-01-10', '202102'),
                ('2021-02-10', '202107'),
                ('2021-03-10', '202111'),
                ('2019-01-10', '201902'),
            ],
            ["day_id_day", "wee_id_week"]
        )
        smu = self.spark.createDataFrame(
            [
                ('2021-01-01', '2021-01-15', '23', 2),
                ('2021-02-01', '2021-02-15', '23', 6),
                ('2021-03-01', '2021-03-15', '23', 5),
                ('2019-01-01', '2019-01-15', '23', 5),
            ],
            ["date_begin", "date_end", "model_id", "mrp"]
        )
        results = mrp.get_model_week_mrp(smu, day).collect()
        expected_results = [
            (202102, '23', True),
            (202107, '23', False),
            (202111, '23', True),
        ]
        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
