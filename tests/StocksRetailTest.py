from pyspark.sql import functions as F
from tests.PySparkTestCase import PySparkTestCase
from src.global import stocks_retail
import unittest
import datetime as dt


class StocksRetailTest(PySparkTestCase):
    def test_get_retail_stock(self):
        but = self.spark.createDataFrame([(100, 100), (200, 200)],
                                         ["but_idr_business_unit", "but_num_business_unit"])
        sap = self.spark.createDataFrame([('100', 'Z001', 'Z002'), ('200', 'Z001', 'Z003')],
                                         ["plant_id", "purch_org", "sales_org"])
        sku = self.spark.createDataFrame([(2, 23, 23, 230), (3, 35, 35, 350)],
                                         ["mdl_num_model_r3", "sku_num_sku_r3", "sku_idr_sku", "fam_idr_family"])
        stocks = self.spark.createDataFrame(
            [
                ('67', 23, 100, '2021-02-15', 21, '2021-02'),
                ('67', 23, 100, '2021-02-16', 15, '2021-02'),
                ('67', 35, 200, '2021-02-16', 10, '2021-02'),
                ('99', 35, 100, '2021-02-16', 11, '2021-02'),
            ],
            ["stt_idr_stock_type", "sku_idr_sku", "but_idr_business_unit", "spr_date_stock", "f_quantity", "month"]
        )
        results = stocks_retail.get_retail_stock(stocks, but, sku, sap).orderBy(F.col("sku_num_sku_r3"), F.col("day")).collect()
        expected_results = [
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 16), 15, '2021-02'),
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 15), 21, '2021-02'),
            (200, 200, 'Z001', 'Z003', 35, 3, 350, dt.date(2021, 2, 16), 10, '2021-02')
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_add_lifestage_data(self):
        stocks = self.spark.createDataFrame([
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 16), 15, '2021-02'),
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 15), 21, '2021-02'),
            (200, 200, 'Z001', 'Z003', 35, 3, 350, dt.date(2021, 2, 16), 10, '2021-02')],
            ["but_num_business_unit", "but_idr_business_unit", "purch_org", "sales_org",
             "sku_num_sku_r3", "mdl_num_model_r3", "fam_idr_family", "day", "f_quantity", "month"])
        dtm = self.spark.createDataFrame(
            [
                (23, 'Z002', dt.date(2021, 2, 1), dt.date(2021, 3, 1), 2),
                (35, 'Z003', dt.date(2021, 2, 1), dt.date(2021, 3, 1), 3),
            ],
            ["material_id", "dtm_sales_org", "date_begin", "date_end", "assortment_grade"]
        )
        results = stocks_retail.add_lifestage_data(stocks, dtm, 202106, 201910).orderBy(F.col("sku_num_sku_r3"),
                                                                                        F.col("day")).collect()
        expected_results = [
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 15), 21, 2, '2021-02'),
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 16), 15, 2, '2021-02'),
            (200, 200, 'Z001', 'Z003', 35, 3, 350, dt.date(2021, 2, 16), 10, 3, '2021-02')
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_get_all_days_bu_df(self):
        stocks = self.spark.createDataFrame([
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 16), 15, '2021-02'),
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 15), 21, '2021-02'),
            (200, 200, 'Z001', 'Z003', 35, 3, 350, dt.date(2021, 2, 16), 10, '2021-02')],
            ["but_num_business_unit", "but_idr_business_unit", "purch_org", "sales_org",
             "sku_num_sku_r3", "mdl_num_model_r3", "fam_idr_family", "day", "f_quantity", "month"])

        results = stocks_retail.get_all_days_bu_df(self.spark, stocks, dt.date(2021, 2, 14), dt.date(2021, 2, 17))\
            .orderBy(F.col("day").asc(), F.col("sku_num_sku_r3").asc()).collect()
        expected_results = [
            (dt.date(2021, 2, 14), 100, 23, 2, 230, 'Z001', 'Z002', 100),
            (dt.date(2021, 2, 14), 200, 35, 3, 350, 'Z001', 'Z003', 200),
            (dt.date(2021, 2, 15), 100, 23, 2, 230, 'Z001', 'Z002', 100),
            (dt.date(2021, 2, 15), 200, 35, 3, 350, 'Z001', 'Z003', 200),
            (dt.date(2021, 2, 16), 100, 23, 2, 230, 'Z001', 'Z002', 100),
            (dt.date(2021, 2, 16), 200, 35, 3, 350, 'Z001', 'Z003', 200),
            (dt.date(2021, 2, 17), 100, 23, 2, 230, 'Z001', 'Z002', 100),
            (dt.date(2021, 2, 17), 200, 35, 3, 350, 'Z001', 'Z003', 200),
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_fill_empty_days(self):
        stocks = self.spark.createDataFrame([
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 16), 15, 2, '2021-02'),
            (100, 100, 'Z001', 'Z002', 23, 2, 230, dt.date(2021, 2, 15), 21, 2, '2021-02'),
            (200, 200, 'Z001', 'Z003', 35, 3, 350, dt.date(2021, 2, 16), 10, 3, '2021-02')],
            ["but_num_business_unit", "but_idr_business_unit", "purch_org", "sales_org", "sku_num_sku_r3",
             "mdl_num_model_r3", "fam_idr_family", "day", "f_quantity", "assortment_grade", "month"])

        df_bu_sku_day = self.spark.createDataFrame([
                (dt.date(2021, 2, 15), 100, 23, 2, 230, 'Z001', 'Z002', 100),
                (dt.date(2021, 2, 15), 200, 35, 3, 350, 'Z001', 'Z003', 200),
                (dt.date(2021, 2, 16), 100, 23, 2, 230, 'Z001', 'Z002', 100),
                (dt.date(2021, 2, 16), 200, 35, 3, 350, 'Z001', 'Z003', 200),
                (dt.date(2021, 2, 17), 100, 23, 2, 230, 'Z001', 'Z002', 100),
                (dt.date(2021, 2, 17), 200, 35, 3, 350, 'Z001', 'Z003', 200),
            ],
            ["day", "but_num_business_unit", "sku_num_sku_r3", "mdl_num_model_r3", "fam_idr_family",
             "purch_org", "sales_org", "but_idr_business_unit"]
        )
        results = stocks_retail.fill_empty_days(stocks, df_bu_sku_day).drop("month")\
            .orderBy(F.col("sku_num_sku_r3").asc(), F.col("day").asc()).collect()
        expected_results = [
            (100, 23, 2, 230, 'Z001', 'Z002', dt.date(2021, 2, 15), 100, 21, 2),
            (100, 23, 2, 230, 'Z001', 'Z002', dt.date(2021, 2, 16), 100, 15, 2),
            (100, 23, 2, 230, 'Z001', 'Z002', dt.date(2021, 2, 17), 100, 15, 2),
            (200, 35, 3, 350, 'Z001', 'Z003', dt.date(2021, 2, 15), 200, 0, 0),
            (200, 35, 3, 350, 'Z001', 'Z003', dt.date(2021, 2, 16), 200, 10, 3),
            (200, 35, 3, 350, 'Z001', 'Z003', dt.date(2021, 2, 17), 200, 10, 3)
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_enrich_with_data(self):
        stocks = self.spark.createDataFrame([
            (100, 100, 'Z001', 'Z002', 23, 230, dt.date(2021, 2, 16), 15, 2),
            (100, 100, 'Z001', 'Z002', 23, 230, dt.date(2021, 2, 15), 21, 2),
            (200, 200, 'Z001', 'Z003', 35, 350, dt.date(2021, 2, 16), 10, 3)],
            ["but_num_business_unit", "but_idr_business_unit", "purch_org", "sales_org",
             "mdl_num_model_r3", "fam_idr_family", "day", "f_quantity", "assortment_grade"])
        week = self.spark.createDataFrame(
            [(202107, '2021-02-15'), (202107, '2021-02-16')],
            ["week_id", "day"])
        rc_df = self.spark.createDataFrame(
            [(100, 230, 1, 202107), (200, 350, 4, 202107)],
            ["but_idr_business_unit", "fam_idr_family", "range_level", "wee_id_week"])

        results = stocks_retail.enrich_with_data(stocks, week, rc_df, 202101, 202109, 201901)\
            .orderBy(F.col("mdl_num_model_r3").asc(), F.col("day").asc()).collect()
        expected_results = [
            (200, 35, dt.date(2021, 2, 16), 202107, 10, 'Z001', 'Z003')
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_refine_stock(self):
        stocks = self.spark.createDataFrame([
            (100, 'Z001', 'Z002', 23, dt.date(2021, 2, 16), 202107, 15),
            (100, 'Z001', 'Z002', 23, dt.date(2021, 2, 15), 202107, 21),
            (200, 'Z001', 'Z003', 35, dt.date(2021, 2, 16), 202107, 10)],
            ["but_num_business_unit", "purch_org", "sales_org", "mdl_num_model_r3", "day", "week_id", "f_quantity"])

        results = stocks_retail.refine_stock(stocks)\
            .orderBy(F.col("mdl_num_model_r3")).collect()
        expected_results = [
            (200, 35, 202107, 'Z001', 'Z003', 0, 10.0, 10, 10, 0),
            (100, 23, 202107, 'Z001', 'Z002', 0, 18.0, 21, 15, 0)
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_keep_only_assigned_stock_for_old_stocks(self):
        stocks = self.spark.createDataFrame([
            (200, 23, 201701, 'Z001', 'Z003', 1),
            (200, 23, 201702, 'Z001', 'Z003', 1),
            (200, 23, 201703, 'Z001', 'Z003', 1),
            (200, 23, 201704, 'Z001', 'Z003', 1),
            (200, 23, 201705, 'Z001', 'Z003', 1),
            (200, 23, 201706, 'Z001', 'Z003', 1),
            (200, 35, 201701, 'Z001', 'Z003', 1),
            (200, 35, 201702, 'Z001', 'Z003', 0),
            (200, 35, 201703, 'Z001', 'Z003', 1),
            (200, 35, 201704, 'Z001', 'Z003', 1),
            (200, 35, 201705, 'Z001', 'Z003', 1),
            (200, 35, 201706, 'Z001', 'Z003', 1),
            (200, 35, 201707, 'Z001', 'Z003', 0),
        ],
            ["but_num_business_unit", "mdl_num_model_r3", "week_id", "purch_org", "sales_org", "is_sold_out"])

        results = stocks_retail.keep_only_assigned_stock_for_old_stocks(stocks, 201707, 201901, 4)\
            .orderBy(F.col("mdl_num_model_r3"), F.col("week_id"))
        expected_results = [
            (200, 23, 201701, 'Z001', 'Z003', 1, 1, 1, 1),
            (200, 23, 201702, 'Z001', 'Z003', 1, 0, 1, 2),
            (200, 23, 201703, 'Z001', 'Z003', 1, 0, 1, 3),
            (200, 23, 201704, 'Z001', 'Z003', 1, 0, 1, 4),
            (200, 35, 201701, 'Z001', 'Z003', 1, 1, 1, 1),
            (200, 35, 201702, 'Z001', 'Z003', 0, 1, 2, 0),
            (200, 35, 201703, 'Z001', 'Z003', 1, 1, 3, 1),
            (200, 35, 201704, 'Z001', 'Z003', 1, 0, 3, 2),
            (200, 35, 201705, 'Z001', 'Z003', 1, 0, 3, 3),
            (200, 35, 201706, 'Z001', 'Z003', 1, 0, 3, 4),
            (200, 35, 201707, 'Z001', 'Z003', 0, 1, 4, 0)
        ]#but_num_business_unit, mdl_num_model_r3, week_id, purch_org, sales_org, is_sold_out, is_new_session, session, nb_weeks_soldout
        self.assertEqual(set(results.collect()), set(expected_results))

    def test_get_stock_avail_by_country(self):
        stocks = self.spark.createDataFrame([
            (200, 35, 202107, 'Z001', 'Z003', 0, 10.0, 10, 10, 0),
            (100, 23, 202107, 'Z001', 'Z002', 2, 18.0, 21, 15, 1),
            (150, 23, 202107, 'Z001', 'Z002', 0, 10.0, 10, 10, 0)],
            ["but_num_business_unit", "mdl_num_model_r3", "week_id", "purch_org", "sales_org", "nb_day_stock_null",
             "f_quantity_mean", "f_quantity_max", "f_quantity_min", "is_sold_out"])

        results = stocks_retail.get_stock_avail_by_country(stocks)\
            .orderBy(F.col("mdl_num_model_r3")).collect()
        expected_results = [
            (23, 202107, 'Z001', 'Z002', 2, 1, 28.0, 14.0, 21, 10, 0.5),
            (35, 202107, 'Z001', 'Z003', 1, 0, 10.0, 10.0, 10, 10, 0.0)
        ]
        self.assertEqual(set(results), set(expected_results))

    def test_get_stock_avail_for_all_countries(self):
        stocks = self.spark.createDataFrame([
            (200, 35, 202107, 'Z001', 'Z003', 0, 10.0, 10, 10, 0),
            (100, 23, 202107, 'Z001', 'Z002', 2, 18.0, 21, 15, 1),
            (150, 23, 202107, 'Z001', 'Z002', 0, 10.0, 10, 10, 0)],
            ["but_num_business_unit", "mdl_num_model_r3", "week_id", "purch_org", "sales_org", "nb_day_stock_null",
             "f_quantity_mean", "f_quantity_max", "f_quantity_min", "is_sold_out"])

        results = stocks_retail.get_stock_avail_for_all_countries(stocks)\
            .orderBy(F.col("mdl_num_model_r3")).collect()
        expected_results = [
            (35, 202107, 1, 0, 10.0, 10.0, 10, 10, 0.0),
            (23, 202107, 2, 1, 28.0, 14.0, 21, 10, 0.5)
        ]
        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
