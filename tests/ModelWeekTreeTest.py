from tests.PySparkTestCase import PySparkTestCase
from src.global import model_week_tree as mwt
import unittest
from pyspark.sql import functions as F


class ModelWeekTreeTest(PySparkTestCase):
    def test_get_model_week_tree(self):
        week = self.spark.createDataFrame([(202001, '2020-01-02')], ["wee_id_week", "day_first_day_week"])
        sku_h = self.spark.createDataFrame(
            [
                (1, 'mod1', 1, 10, 'fam10', 100, 'sdp100', 101, 'dep101', 0, 'univ0', 102, 'nat102', 103, 'brd103', '2020-01-01', '2020-02-28'),
                (2, 'mod2', 2, 20, 'fam20', 200, 'sdp200', 201, 'dep201', 9, 'univ9', 202, 'nat202', 203, 'brd203', '2020-01-01', '2020-02-28'),
                (3, 'mod3', 3, 30, 'fam30', 300, 'sdp300', 301, 'dep301', 9, 'univ9', 302, 'nat302', 303, 'brd303', '2020-01-01', '2020-02-28'),
                (4, 'mod4', 4, 40, 'fam40', 400, 'sdp400', 401, 'dep401', 9, 'univ9', 402, 'nat402', 403, 'brd403', '2020-01-01', '2020-02-28'),
                (4, 'mod4', 4, 49, 'fam49', 499, 'sdp499', 499, 'dep499', 9, 'univ9', 499, 'nat499', 499, 'brd499', '2021-01-01', '2021-02-28'),
            ],
            ["mdl_num_model_r3", "mdl_label", "sku_num_sku_r3", "fam_num_family", "family_label", "sdp_num_sub_department",
             "sdp_label", "dpt_num_department", "dpt_label", "unv_num_univers", "unv_label", "pnt_num_product_nature",
             "product_nature_label", "brd_label_brand", "brd_type_brand_libelle", "sku_date_begin", "sku_date_end"]
        )
        results = mwt.get_model_week_tree(sku_h, week).orderBy(F.col("week_id").asc(),F.col("model_id").asc()) .collect()
        expected_results = [
            (202001, 2, 20, 200, 201, 9, 202, 'mod2', 'fam20', 'sdp200', 'dep201', 'univ9', 'nat202', 203, 'brd203'),
            (202001, 3, 30, 300, 301, 9, 302, 'mod3', 'fam30', 'sdp300', 'dep301', 'univ9', 'nat302', 303, 'brd303'),
            (202001, 4, 40, 400, 401, 9, 402, 'mod4', 'fam40', 'sdp400', 'dep401', 'univ9', 'nat402', 403, 'brd403')
        ]
        self.assertEqual(set(results), set(expected_results))


if __name__ == "__main__":
    unittest.main()
