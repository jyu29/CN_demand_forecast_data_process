from pyspark.sql import functions as F
from spark.test.PySparkTestCase import PySparkTestCase
from spark.src import sales
import unittest


class SalesTest(PySparkTestCase):
    def test_get_offline_sales(self):
        day = self.spark.createDataFrame([(202107, '2021-02-15'), (202107, '2021-02-16')],
                                         ["wee_id_week", "day_id_day"])
        week = self.spark.createDataFrame([(202107, '2021-02-14'), (202107, '2021-02-14')],
                                          ["wee_id_week", "day_first_day_week"])
        cex = self.spark.createDataFrame([(33, 1.0), (17, 0.6)], ["cur_idr_currency_base", "exchange_rate"])
        but = self.spark.createDataFrame([(7, 100, 100), (8, 200, 200)],
                                         ["but_num_typ_but", "but_idr_business_unit", "but_num_business_unit"])
        sap = self.spark.createDataFrame([('100',)], ["plant_id"])
        sku = self.spark.createDataFrame([(23, 23), (35, 35)], ["mdl_num_model_r3", "sku_idr_sku"])
        tdt = self.spark.createDataFrame(
            [
                ('offline', '2021-02-15', 23, 100, 33, 2, 1, 3),
                ('offline', '2021-02-16', 23, 100, 17, 3, 4, 3),
                ('offline', '2021-02-16', 35, 200, 33, 1, 1, 1),
                ('online', '2021-02-16', 35, 100, 33, 1, 1, 1),
            ],
            ["the_to_type", "tdt_date_to_ordered", "sku_idr_sku", "but_idr_business_unit", "cur_idr_currency",
             "f_qty_item", "f_pri_regular_sales_unit", "f_to_tax_in"]
        )
        results = sales.get_offline_sales(tdt, day, week, sku, but, cex, sap).orderBy(F.col("f_qty_item")).collect()
        expected_results = [
            (23, 202107, '2021-02-14', 2, 1, 3, 1.0),
            (23, 202107, '2021-02-14', 3, 4, 3, 0.6)
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_get_online_sales(self):
        day = self.spark.createDataFrame([(202107, '2021-02-15'), (202107, '2021-02-16')],
                                         ["wee_id_week", "day_id_day"])
        week = self.spark.createDataFrame([(202107, '2021-02-14')], ["wee_id_week", "day_first_day_week"])
        cex = self.spark.createDataFrame([(33, 1.0), (17, 0.6)], ["cur_idr_currency_base", "exchange_rate"])
        but = self.spark.createDataFrame([(7, 100, 100, '_1_2_3'), (8, 200, 200, '_4_5_6')],
                                         ["but_num_typ_but", "but_idr_business_unit", "but_num_business_unit", "but_code_international"])
        sap = self.spark.createDataFrame([('100',)], ["plant_id"])
        gdc = self.spark.createDataFrame([('100', '_1', '_2', '_3')], ["plant_id", "ean_1", "ean_2", "ean_3"])
        sku = self.spark.createDataFrame([(23, 23), (35, 35)], ["mdl_num_model_r3", "sku_idr_sku"])
        dyd = self.spark.createDataFrame(
            [
                ('online',  'sale', '2021-02-15', 23, 100, 33, 2, 1, 3),
                ('online',  'sale', '2021-02-16', 23, 100, 17, 3, 4, 3),
                ('online',  'sale', '2021-02-16', 35, 200, 33, 1, 1, 1),
                ('offline', 'sale', '2021-02-16', 35, 100, 33, 1, 1, 1),
            ],
            ["the_to_type", "tdt_type_detail", "tdt_date_to_ordered", "sku_idr_sku", "but_idr_business_unit_sender",
             "cur_idr_currency", "f_qty_item", "f_tdt_pri_regular_sales_unit", "f_to_tax_in"]
        )
        results = sales.get_online_sales(dyd, day, week, sku, but, gdc, cex, sap).orderBy(F.col("f_qty_item")).collect()
        expected_results = [
            (23, 202107, '2021-02-14', 2, 1, 3, 1.0),
            (23, 202107, '2021-02-14', 3, 4, 3, 0.6)
        ]
        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
