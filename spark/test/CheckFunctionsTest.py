from pyspark.sql import functions as F
from spark.test.PySparkTestCase import PySparkTestCase
from pyspark.sql.types import *
from spark.src import check_functions
import unittest
import datetime as dt


class CheckFunctionsTest(PySparkTestCase):
    def test_check_d_week(self):

        week_df = self.spark.createDataFrame(
            [
             (202101,), (202102,), (202103,), (202104,), (202105,), (202106,), (202107,), (202108,), (202109,), (202110,),
             (202111,), (202112,), (202113,), (202114,), (202115,), (202116,),
             (202117,), (202118,), (202119,), (202120,), (202121,), (202122,), (202123,), (202124,), (202125,), (202126,),
             (202127,), (202128,), (202129,), (202130,), (202131,), (202132,), (202133,),
             (202134,), (202135,), (202136,), (202137,), (202138,), (202139,), (202140,), (202141,), (202142,), (202143,),
             (202144,), (202145,), (202146,), (202147,), (202148,), (202149,), (202150,),
             (202151,), (202152,), (202201,), (202202,), (202203,), (202204,), (202205,), (202206,), (202207,), (202208,),
             (202209,), (202210,), (202211,), (202212,), (202213,), (202214,), (202215,),
             (202216,), (202217,), (202218,), (202219,), (202220,), (202221,), (202222,), (202223,), (202224,), (202225,),
             (202226,), (202227,), (202228,), (202229,), (202230,), (202231,), (202232,),
             (202233,), (202234,), (202235,), (202236,), (202237,), (202238,), (202239,), (202240,), (202241,), (202242,),
             (202243,), (202244,), (202245,), (202246,), (202247,), (202248,), (202249,),
             (202250,), (202251,), (202252,), (202301,), (202302,), (202303,), (202304,), (202305,), (202306,), (202307,),
             (202308,), (202309,), (202310,), (202311,), (202312,), (202313,), (202314,),
             (202315,), (202316,), (202317,), (202318,), (202319,), (202320,), (202321,), (202322,), (202323,), (202324,),
             (202325,), (202326,),
            ], "wee_id_week int"
        )
        check_functions.check_d_week(week_df, 202103)

    def test_check_d_week_not_104_weeks(self):
        week_df = self.spark.createDataFrame(
            [
             (202101,), (202102,), (202103,), (202104,), (202105,), (202106,), (202107,), (202108,), (202109,), (202110,),
             (202111,), (202112,), (202113,), (202114,), (202115,), (202116,),
             (202117,), (202118,), (202119,),
            ], "wee_id_week int"
        )
        self.assertRaises(Exception,check_functions.check_d_week(week_df, 202103))

    def test_check_d_day(self):
        day_df = self.spark.createDataFrame(
        [('2021-01-01', 202053), ('2021-01-02', 202053), ('2021-01-03', 202101), ('2021-01-04', 202101),
         ('2021-01-05', 202101), ('2021-01-06', 202101), ('2021-01-07', 202101), ('2021-01-08', 202101),
         ('2021-01-09', 202101), ('2021-01-10', 202102), ('2021-01-11', 202102), ('2021-01-12', 202102),
         ('2021-01-13', 202102), ('2021-01-14', 202102), ('2021-01-15', 202102), ('2021-01-16', 202102),
         ('2021-01-17', 202103), ('2021-01-18', 202103), ('2021-01-19', 202103), ('2021-01-20', 202103),
         ('2021-01-21', 202103), ('2021-01-22', 202103), ('2021-01-23', 202103), ('2021-01-24', 202104),
         ('2021-01-25', 202104), ('2021-01-26', 202104), ('2021-01-27', 202104), ('2021-01-28', 202104),
         ('2021-01-29', 202104), ('2021-01-30', 202104), ('2021-01-31', 202105), ('2021-02-01', 202105),
         ('2021-02-02', 202105), ('2021-02-03', 202105), ('2021-02-04', 202105), ('2021-02-05', 202105),
         ('2021-02-06', 202105), ('2021-02-07', 202106), ('2021-02-08', 202106), ('2021-02-09', 202106),
         ('2021-02-10', 202106), ('2021-02-11', 202106), ('2021-02-12', 202106), ('2021-02-13', 202106),
         ('2021-02-14', 202107), ('2021-02-15', 202107), ('2021-02-16', 202107), ('2021-02-17', 202107),
         ('2021-02-18', 202107), ('2021-02-19', 202107), ('2021-02-20', 202107), ('2021-02-21', 202108),
         ('2021-02-22', 202108), ('2021-02-23', 202108), ('2021-02-24', 202108), ('2021-02-25', 202108),
         ('2021-02-26', 202108), ('2021-02-27', 202108), ('2021-02-28', 202109), ('2021-03-01', 202109),
         ('2021-03-02', 202109), ('2021-03-03', 202109), ('2021-03-04', 202109), ('2021-03-05', 202109),
         ('2021-03-06', 202109), ('2021-03-07', 202110), ('2021-03-08', 202110), ('2021-03-09', 202110),
         ('2021-03-10', 202110), ('2021-03-11', 202110), ('2021-03-12', 202110), ('2021-03-13', 202110),
         ('2021-03-14', 202111), ('2021-03-15', 202111), ('2021-03-16', 202111), ('2021-03-17', 202111),
         ('2021-03-18', 202111), ('2021-03-19', 202111), ('2021-03-20', 202111), ('2021-03-21', 202112),
         ('2021-03-22', 202112), ('2021-03-23', 202112), ('2021-03-24', 202112), ('2021-03-25', 202112),
         ('2021-03-26', 202112), ('2021-03-27', 202112), ('2021-03-28', 202113), ('2021-03-29', 202113),
         ('2021-03-30', 202113), ('2021-03-31', 202113), ('2021-04-01', 202113), ('2021-04-02', 202113),
         ('2021-04-03', 202113), ('2021-04-04', 202114), ('2021-04-05', 202114), ('2021-04-06', 202114),
         ('2021-04-07', 202114), ('2021-04-08', 202114), ('2021-04-09', 202114), ('2021-04-10', 202114),
         ('2021-04-11', 202115), ('2021-04-12', 202115), ('2021-04-13', 202115), ('2021-04-14', 202115),
         ('2021-04-15', 202115), ('2021-04-16', 202115), ('2021-04-17', 202115), ('2021-04-18', 202116),
         ('2021-04-19', 202116), ('2021-04-20', 202116), ('2021-04-21', 202116), ('2021-04-22', 202116),
         ('2021-04-23', 202116), ('2021-04-24', 202116), ('2021-04-25', 202117), ('2021-04-26', 202117),
         ('2021-04-27', 202117), ('2021-04-28', 202117), ('2021-04-29', 202117), ('2021-04-30', 202117),
         ('2021-05-01', 202117), ('2021-05-02', 202118), ('2021-05-03', 202118), ('2021-05-04', 202118),
         ('2021-05-05', 202118), ('2021-05-06', 202118), ('2021-05-07', 202118), ('2021-05-08', 202118),
         ('2021-05-09', 202119), ('2021-05-10', 202119), ('2021-05-11', 202119), ('2021-05-12', 202119),
         ('2021-05-13', 202119), ('2021-05-14', 202119), ('2021-05-15', 202119), ('2021-05-16', 202120),
         ('2021-05-17', 202120), ('2021-05-18', 202120), ('2021-05-19', 202120), ('2021-05-20', 202120),
         ('2021-05-21', 202120), ('2021-05-22', 202120), ('2021-05-23', 202121), ('2021-05-24', 202121),
         ('2021-05-25', 202121), ('2021-05-26', 202121), ('2021-05-27', 202121), ('2021-05-28', 202121),
         ('2021-05-29', 202121), ('2021-05-30', 202122), ('2021-05-31', 202122), ('2021-06-01', 202122),
         ('2021-06-02', 202122), ('2021-06-03', 202122), ('2021-06-04', 202122), ('2021-06-05', 202122),
         ('2021-06-06', 202123), ('2021-06-07', 202123), ('2021-06-08', 202123), ('2021-06-09', 202123),
         ('2021-06-10', 202123), ('2021-06-11', 202123), ('2021-06-12', 202123), ('2021-06-13', 202124),
         ('2021-06-14', 202124), ('2021-06-15', 202124), ('2021-06-16', 202124), ('2021-06-17', 202124),
         ('2021-06-18', 202124), ('2021-06-19', 202124), ('2021-06-20', 202125), ('2021-06-21', 202125),
         ('2021-06-22', 202125), ('2021-06-23', 202125), ('2021-06-24', 202125), ('2021-06-25', 202125),
         ('2021-06-26', 202125), ('2021-06-27', 202126), ('2021-06-28', 202126), ('2021-06-29', 202126),
         ('2021-06-30', 202126), ('2021-07-01', 202126), ('2021-07-02', 202126), ('2021-07-03', 202126),
         ('2021-07-04', 202127), ('2021-07-05', 202127), ('2021-07-06', 202127), ('2021-07-07', 202127),
         ('2021-07-08', 202127), ('2021-07-09', 202127), ('2021-07-10', 202127), ('2021-07-11', 202128),
         ('2021-07-12', 202128), ('2021-07-13', 202128), ('2021-07-14', 202128), ('2021-07-15', 202128),
         ('2021-07-16', 202128), ('2021-07-17', 202128), ('2021-07-18', 202129), ('2021-07-19', 202129),
         ('2021-07-20', 202129), ('2021-07-21', 202129), ('2021-07-22', 202129), ('2021-07-23', 202129),
         ('2021-07-24', 202129), ('2021-07-25', 202130), ('2021-07-26', 202130), ('2021-07-27', 202130),
         ('2021-07-28', 202130), ('2021-07-29', 202130), ('2021-07-30', 202130), ('2021-07-31', 202130),
         ('2021-08-01', 202131), ('2021-08-02', 202131), ('2021-08-03', 202131), ('2021-08-04', 202131),
         ('2021-08-05', 202131), ('2021-08-06', 202131), ('2021-08-07', 202131), ('2021-08-08', 202132),
         ('2021-08-09', 202132), ('2021-08-10', 202132), ('2021-08-11', 202132), ('2021-08-12', 202132),
         ('2021-08-13', 202132), ('2021-08-14', 202132), ('2021-08-15', 202133), ('2021-08-16', 202133),
         ('2021-08-17', 202133), ('2021-08-18', 202133), ('2021-08-19', 202133), ('2021-08-20', 202133),
         ('2021-08-21', 202133), ('2021-08-22', 202134), ('2021-08-23', 202134), ('2021-08-24', 202134),
         ('2021-08-25', 202134), ('2021-08-26', 202134), ('2021-08-27', 202134), ('2021-08-28', 202134),
         ('2021-08-29', 202135), ('2021-08-30', 202135), ('2021-08-31', 202135), ('2021-09-01', 202135),
         ('2021-09-02', 202135), ('2021-09-03', 202135), ('2021-09-04', 202135), ('2021-09-05', 202136),
         ('2021-09-06', 202136), ('2021-09-07', 202136), ('2021-09-08', 202136), ('2021-09-09', 202136),
         ('2021-09-10', 202136), ('2021-09-11', 202136), ('2021-09-12', 202137), ('2021-09-13', 202137),
         ('2021-09-14', 202137), ('2021-09-15', 202137), ('2021-09-16', 202137), ('2021-09-17', 202137),
         ('2021-09-18', 202137), ('2021-09-19', 202138), ('2021-09-20', 202138), ('2021-09-21', 202138),
         ('2021-09-22', 202138), ('2021-09-23', 202138), ('2021-09-24', 202138), ('2021-09-25', 202138),
         ('2021-09-26', 202139), ('2021-09-27', 202139), ('2021-09-28', 202139), ('2021-09-29', 202139),
         ('2021-09-30', 202139), ('2021-10-01', 202139), ('2021-10-02', 202139), ('2021-10-03', 202140),
         ('2021-10-04', 202140), ('2021-10-05', 202140), ('2021-10-06', 202140), ('2021-10-07', 202140),
         ('2021-10-08', 202140), ('2021-10-09', 202140), ('2021-10-10', 202141), ('2021-10-11', 202141),
         ('2021-10-12', 202141), ('2021-10-13', 202141), ('2021-10-14', 202141), ('2021-10-15', 202141),
         ('2021-10-16', 202141), ('2021-10-17', 202142), ('2021-10-18', 202142), ('2021-10-19', 202142),
         ('2021-10-20', 202142), ('2021-10-21', 202142), ('2021-10-22', 202142), ('2021-10-23', 202142),
         ('2021-10-24', 202143), ('2021-10-25', 202143), ('2021-10-26', 202143), ('2021-10-27', 202143),
         ('2021-10-28', 202143), ('2021-10-29', 202143), ('2021-10-30', 202143), ('2021-10-31', 202144),
         ('2021-11-01', 202144), ('2021-11-02', 202144), ('2021-11-03', 202144), ('2021-11-04', 202144),
         ('2021-11-05', 202144), ('2021-11-06', 202144), ('2021-11-07', 202145), ('2021-11-08', 202145),
         ('2021-11-09', 202145), ('2021-11-10', 202145), ('2021-11-11', 202145), ('2021-11-12', 202145),
         ('2021-11-13', 202145), ('2021-11-14', 202146), ('2021-11-15', 202146), ('2021-11-16', 202146),
         ('2021-11-17', 202146), ('2021-11-18', 202146), ('2021-11-19', 202146), ('2021-11-20', 202146),
         ('2021-11-21', 202147), ('2021-11-22', 202147), ('2021-11-23', 202147), ('2021-11-24', 202147),
         ('2021-11-25', 202147), ('2021-11-26', 202147), ('2021-11-27', 202147), ('2021-11-28', 202148),
         ('2021-11-29', 202148), ('2021-11-30', 202148), ('2021-12-01', 202148), ('2021-12-02', 202148),
         ('2021-12-03', 202148), ('2021-12-04', 202148), ('2021-12-05', 202149), ('2021-12-06', 202149),
         ('2021-12-07', 202149), ('2021-12-08', 202149), ('2021-12-09', 202149), ('2021-12-10', 202149),
         ('2021-12-11', 202149), ('2021-12-12', 202150), ('2021-12-13', 202150), ('2021-12-14', 202150),
         ('2021-12-15', 202150), ('2021-12-16', 202150), ('2021-12-17', 202150), ('2021-12-18', 202150),
         ('2021-12-19', 202151), ('2021-12-20', 202151), ('2021-12-21', 202151), ('2021-12-22', 202151),
         ('2021-12-23', 202151), ('2021-12-24', 202151), ('2021-12-25', 202151), ('2021-12-26', 202152),
         ('2021-12-27', 202152), ('2021-12-28', 202152), ('2021-12-29', 202152), ('2021-12-30', 202152),
         ('2021-12-31', 202152), ('2022-01-01', 202152), ('2022-01-02', 202201), ('2022-01-03', 202201),
         ('2022-01-04', 202201), ('2022-01-05', 202201), ('2022-01-06', 202201), ('2022-01-07', 202201),
         ('2022-01-08', 202201), ('2022-01-09', 202202), ('2022-01-10', 202202), ('2022-01-11', 202202),
         ('2022-01-12', 202202), ('2022-01-13', 202202), ('2022-01-14', 202202), ('2022-01-15', 202202),
         ('2022-01-16', 202203), ('2022-01-17', 202203), ('2022-01-18', 202203), ('2022-01-19', 202203),
         ('2022-01-20', 202203), ('2022-01-21', 202203), ('2022-01-22', 202203), ('2022-01-23', 202204),
         ('2022-01-24', 202204), ('2022-01-25', 202204), ('2022-01-26', 202204), ('2022-01-27', 202204),
         ('2022-01-28', 202204), ('2022-01-29', 202204), ('2022-01-30', 202205), ('2022-01-31', 202205),
         ('2022-02-01', 202205), ('2022-02-02', 202205), ('2022-02-03', 202205), ('2022-02-04', 202205),
         ('2022-02-05', 202205), ('2022-02-06', 202206), ('2022-02-07', 202206), ('2022-02-08', 202206),
         ('2022-02-09', 202206), ('2022-02-10', 202206), ('2022-02-11', 202206), ('2022-02-12', 202206),
         ('2022-02-13', 202207), ('2022-02-14', 202207), ('2022-02-15', 202207), ('2022-02-16', 202207),
         ('2022-02-17', 202207), ('2022-02-18', 202207), ('2022-02-19', 202207), ('2022-02-20', 202208),
         ('2022-02-21', 202208), ('2022-02-22', 202208), ('2022-02-23', 202208), ('2022-02-24', 202208),
         ('2022-02-25', 202208), ('2022-02-26', 202208), ('2022-02-27', 202209), ('2022-02-28', 202209),
         ('2022-03-01', 202209), ('2022-03-02', 202209), ('2022-03-03', 202209), ('2022-03-04', 202209),
         ('2022-03-05', 202209), ('2022-03-06', 202210), ('2022-03-07', 202210), ('2022-03-08', 202210),
         ('2022-03-09', 202210), ('2022-03-10', 202210), ('2022-03-11', 202210), ('2022-03-12', 202210),
         ('2022-03-13', 202211), ('2022-03-14', 202211), ('2022-03-15', 202211), ('2022-03-16', 202211),
         ('2022-03-17', 202211), ('2022-03-18', 202211), ('2022-03-19', 202211), ('2022-03-20', 202212),
         ('2022-03-21', 202212), ('2022-03-22', 202212), ('2022-03-23', 202212), ('2022-03-24', 202212),
         ('2022-03-25', 202212), ('2022-03-26', 202212), ('2022-03-27', 202213), ('2022-03-28', 202213),
         ('2022-03-29', 202213), ('2022-03-30', 202213), ('2022-03-31', 202213), ('2022-04-01', 202213),
         ('2022-04-02', 202213), ('2022-04-03', 202214), ('2022-04-04', 202214), ('2022-04-05', 202214),
         ('2022-04-06', 202214), ('2022-04-07', 202214), ('2022-04-08', 202214), ('2022-04-09', 202214),
         ('2022-04-10', 202215), ('2022-04-11', 202215), ('2022-04-12', 202215), ('2022-04-13', 202215),
         ('2022-04-14', 202215), ('2022-04-15', 202215), ('2022-04-16', 202215), ('2022-04-17', 202216),
         ('2022-04-18', 202216), ('2022-04-19', 202216), ('2022-04-20', 202216), ('2022-04-21', 202216),
         ('2022-04-22', 202216), ('2022-04-23', 202216), ('2022-04-24', 202217), ('2022-04-25', 202217),
         ('2022-04-26', 202217), ('2022-04-27', 202217), ('2022-04-28', 202217), ('2022-04-29', 202217),
         ('2022-04-30', 202217), ('2022-05-01', 202218), ('2022-05-02', 202218), ('2022-05-03', 202218),
         ('2022-05-04', 202218), ('2022-05-05', 202218), ('2022-05-06', 202218), ('2022-05-07', 202218),
         ('2022-05-08', 202219), ('2022-05-09', 202219), ('2022-05-10', 202219), ('2022-05-11', 202219),
         ('2022-05-12', 202219), ('2022-05-13', 202219), ('2022-05-14', 202219), ('2022-05-15', 202220),
         ('2022-05-16', 202220), ('2022-05-17', 202220), ('2022-05-18', 202220), ('2022-05-19', 202220),
         ('2022-05-20', 202220), ('2022-05-21', 202220), ('2022-05-22', 202221), ('2022-05-23', 202221),
         ('2022-05-24', 202221), ('2022-05-25', 202221), ('2022-05-26', 202221), ('2022-05-27', 202221),
         ('2022-05-28', 202221), ('2022-05-29', 202222), ('2022-05-30', 202222), ('2022-05-31', 202222),
         ('2022-06-01', 202222), ('2022-06-02', 202222), ('2022-06-03', 202222), ('2022-06-04', 202222),
         ('2022-06-05', 202223), ('2022-06-06', 202223), ('2022-06-07', 202223), ('2022-06-08', 202223),
         ('2022-06-09', 202223), ('2022-06-10', 202223), ('2022-06-11', 202223), ('2022-06-12', 202224),
         ('2022-06-13', 202224), ('2022-06-14', 202224), ('2022-06-15', 202224), ('2022-06-16', 202224),
         ('2022-06-17', 202224), ('2022-06-18', 202224), ('2022-06-19', 202225), ('2022-06-20', 202225),
         ('2022-06-21', 202225), ('2022-06-22', 202225), ('2022-06-23', 202225), ('2022-06-24', 202225),
         ('2022-06-25', 202225), ('2022-06-26', 202226), ('2022-06-27', 202226), ('2022-06-28', 202226),
         ('2022-06-29', 202226), ('2022-06-30', 202226), ('2022-07-01', 202226), ('2022-07-02', 202226),
         ('2022-07-03', 202227), ('2022-07-04', 202227), ('2022-07-05', 202227), ('2022-07-06', 202227),
         ('2022-07-07', 202227), ('2022-07-08', 202227), ('2022-07-09', 202227), ('2022-07-10', 202228),
         ('2022-07-11', 202228), ('2022-07-12', 202228), ('2022-07-13', 202228), ('2022-07-14', 202228),
         ('2022-07-15', 202228), ('2022-07-16', 202228), ('2022-07-17', 202229), ('2022-07-18', 202229),
         ('2022-07-19', 202229), ('2022-07-20', 202229), ('2022-07-21', 202229), ('2022-07-22', 202229),
         ('2022-07-23', 202229), ('2022-07-24', 202230), ('2022-07-25', 202230), ('2022-07-26', 202230),
         ('2022-07-27', 202230), ('2022-07-28', 202230), ('2022-07-29', 202230), ('2022-07-30', 202230),
         ('2022-07-31', 202231), ('2022-08-01', 202231), ('2022-08-02', 202231), ('2022-08-03', 202231),
         ('2022-08-04', 202231), ('2022-08-05', 202231), ('2022-08-06', 202231), ('2022-08-07', 202232),
         ('2022-08-08', 202232), ('2022-08-09', 202232), ('2022-08-10', 202232), ('2022-08-11', 202232),
         ('2022-08-12', 202232), ('2022-08-13', 202232), ('2022-08-14', 202233), ('2022-08-15', 202233),
         ('2022-08-16', 202233), ('2022-08-17', 202233), ('2022-08-18', 202233), ('2022-08-19', 202233),
         ('2022-08-20', 202233), ('2022-08-21', 202234), ('2022-08-22', 202234), ('2022-08-23', 202234),
         ('2022-08-24', 202234), ('2022-08-25', 202234), ('2022-08-26', 202234), ('2022-08-27', 202234),
         ('2022-08-28', 202235), ('2022-08-29', 202235), ('2022-08-30', 202235), ('2022-08-31', 202235),
         ('2022-09-01', 202235), ('2022-09-02', 202235), ('2022-09-03', 202235), ('2022-09-04', 202236),
         ('2022-09-05', 202236), ('2022-09-06', 202236), ('2022-09-07', 202236), ('2022-09-08', 202236),
         ('2022-09-09', 202236), ('2022-09-10', 202236), ('2022-09-11', 202237), ('2022-09-12', 202237),
         ('2022-09-13', 202237), ('2022-09-14', 202237), ('2022-09-15', 202237), ('2022-09-16', 202237),
         ('2022-09-17', 202237), ('2022-09-18', 202238), ('2022-09-19', 202238), ('2022-09-20', 202238),
         ('2022-09-21', 202238), ('2022-09-22', 202238), ('2022-09-23', 202238), ('2022-09-24', 202238),
         ('2022-09-25', 202239), ('2022-09-26', 202239), ('2022-09-27', 202239), ('2022-09-28', 202239),
         ('2022-09-29', 202239), ('2022-09-30', 202239), ('2022-10-01', 202239), ('2022-10-02', 202240),
         ('2022-10-03', 202240), ('2022-10-04', 202240), ('2022-10-05', 202240), ('2022-10-06', 202240),
         ('2022-10-07', 202240), ('2022-10-08', 202240), ('2022-10-09', 202241), ('2022-10-10', 202241),
         ('2022-10-11', 202241), ('2022-10-12', 202241), ('2022-10-13', 202241), ('2022-10-14', 202241),
         ('2022-10-15', 202241), ('2022-10-16', 202242), ('2022-10-17', 202242), ('2022-10-18', 202242),
         ('2022-10-19', 202242), ('2022-10-20', 202242), ('2022-10-21', 202242), ('2022-10-22', 202242),
         ('2022-10-23', 202243), ('2022-10-24', 202243), ('2022-10-25', 202243), ('2022-10-26', 202243),
         ('2022-10-27', 202243), ('2022-10-28', 202243), ('2022-10-29', 202243), ('2022-10-30', 202244),
         ('2022-10-31', 202244), ('2022-11-01', 202244), ('2022-11-02', 202244), ('2022-11-03', 202244),
         ('2022-11-04', 202244), ('2022-11-05', 202244), ('2022-11-06', 202245), ('2022-11-07', 202245),
         ('2022-11-08', 202245), ('2022-11-09', 202245), ('2022-11-10', 202245), ('2022-11-11', 202245),
         ('2022-11-12', 202245), ('2022-11-13', 202246), ('2022-11-14', 202246), ('2022-11-15', 202246),
         ('2022-11-16', 202246), ('2022-11-17', 202246), ('2022-11-18', 202246), ('2022-11-19', 202246),
         ('2022-11-20', 202247), ('2022-11-21', 202247), ('2022-11-22', 202247), ('2022-11-23', 202247),
         ('2022-11-24', 202247), ('2022-11-25', 202247), ('2022-11-26', 202247), ('2022-11-27', 202248),
         ('2022-11-28', 202248), ('2022-11-29', 202248), ('2022-11-30', 202248), ('2022-12-01', 202248),
         ('2022-12-02', 202248), ('2022-12-03', 202248), ('2022-12-04', 202249), ('2022-12-05', 202249),
         ('2022-12-06', 202249), ('2022-12-07', 202249), ('2022-12-08', 202249), ('2022-12-09', 202249),
         ('2022-12-10', 202249), ('2022-12-11', 202250), ('2022-12-12', 202250), ('2022-12-13', 202250),
         ('2022-12-14', 202250), ('2022-12-15', 202250), ('2022-12-16', 202250), ('2022-12-17', 202250),
         ('2022-12-18', 202251), ('2022-12-19', 202251), ('2022-12-20', 202251), ('2022-12-21', 202251),
         ('2022-12-22', 202251), ('2022-12-23', 202251), ('2022-12-24', 202251), ('2022-12-25', 202252),
         ('2022-12-26', 202252), ('2022-12-27', 202252), ('2022-12-28', 202252), ('2022-12-29', 202252),
         ('2022-12-30', 202252), ('2022-12-31', 202252), ('2023-01-01', 202301), ('2023-01-02', 202301),
         ('2023-01-03', 202301), ('2023-01-04', 202301), ('2023-01-05', 202301), ('2023-01-06', 202301),
         ('2023-01-07', 202301), ('2023-01-08', 202302), ('2023-01-09', 202302), ('2023-01-10', 202302),
         ('2023-01-11', 202302), ('2023-01-12', 202302), ('2023-01-13', 202302), ('2023-01-14', 202302),
         ('2023-01-15', 202303), ('2023-01-16', 202303), ('2023-01-17', 202303), ('2023-01-18', 202303),
         ('2023-01-19', 202303), ('2023-01-20', 202303), ('2023-01-21', 202303), ('2023-01-22', 202304),
         ('2023-01-23', 202304), ('2023-01-24', 202304), ('2023-01-25', 202304), ('2023-01-26', 202304),
         ('2023-01-27', 202304), ('2023-01-28', 202304), ('2023-01-29', 202305), ('2023-01-30', 202305),
         ('2023-01-31', 202305), ('2023-02-01', 202305), ('2023-02-02', 202305), ('2023-02-03', 202305),
         ('2023-02-04', 202305), ('2023-02-05', 202306), ('2023-02-06', 202306), ('2023-02-07', 202306),
         ('2023-02-08', 202306), ('2023-02-09', 202306), ('2023-02-10', 202306), ('2023-02-11', 202306),
         ('2023-02-12', 202307), ('2023-02-13', 202307), ('2023-02-14', 202307), ('2023-02-15', 202307),
         ('2023-02-16', 202307), ('2023-02-17', 202307), ('2023-02-18', 202307), ('2023-02-19', 202308),
         ('2023-02-20', 202308), ('2023-02-21', 202308), ('2023-02-22', 202308), ('2023-02-23', 202308),
         ('2023-02-24', 202308), ('2023-02-25', 202308), ('2023-02-26', 202309), ('2023-02-27', 202309),
         ('2023-02-28', 202309), ('2023-03-01', 202309), ('2023-03-02', 202309), ('2023-03-03', 202309),
         ('2023-03-04', 202309), ('2023-03-05', 202310), ('2023-03-06', 202310), ('2023-03-07', 202310),
         ('2023-03-08', 202310), ('2023-03-09', 202310), ('2023-03-10', 202310), ('2023-03-11', 202310),
         ('2023-03-12', 202311), ('2023-03-13', 202311), ('2023-03-14', 202311), ('2023-03-15', 202311),
         ('2023-03-16', 202311), ('2023-03-17', 202311), ('2023-03-18', 202311), ('2023-03-19', 202312),
         ('2023-03-20', 202312), ('2023-03-21', 202312), ('2023-03-22', 202312), ('2023-03-23', 202312),
         ('2023-03-24', 202312), ('2023-03-25', 202312), ('2023-03-26', 202313), ('2023-03-27', 202313),
         ('2023-03-28', 202313), ('2023-03-29', 202313), ('2023-03-30', 202313), ('2023-03-31', 202313),
         ('2023-04-01', 202313), ('2023-04-02', 202314), ('2023-04-03', 202314), ('2023-04-04', 202314),
         ('2023-04-05', 202314), ('2023-04-06', 202314), ('2023-04-07', 202314), ('2023-04-08', 202314),
         ('2023-04-09', 202315), ('2023-04-10', 202315), ('2023-04-11', 202315), ('2023-04-12', 202315),
         ('2023-04-13', 202315), ('2023-04-14', 202315), ('2023-04-15', 202315), ('2023-04-16', 202316),
         ('2023-04-17', 202316), ('2023-04-18', 202316), ('2023-04-19', 202316), ('2023-04-20', 202316),
         ('2023-04-21', 202316), ('2023-04-22', 202316), ('2023-04-23', 202317), ('2023-04-24', 202317),
         ('2023-04-25', 202317), ('2023-04-26', 202317), ('2023-04-27', 202317), ('2023-04-28', 202317),
         ('2023-04-29', 202317), ('2023-04-30', 202318), ('2023-05-01', 202318), ('2023-05-02', 202318),
         ('2023-05-03', 202318), ('2023-05-04', 202318), ('2023-05-05', 202318), ('2023-05-06', 202318),
         ('2023-05-07', 202319), ('2023-05-08', 202319), ('2023-05-09', 202319), ('2023-05-10', 202319),
         ('2023-05-11', 202319), ('2023-05-12', 202319), ('2023-05-13', 202319), ('2023-05-14', 202320),
         ('2023-05-15', 202320), ('2023-05-16', 202320), ('2023-05-17', 202320), ('2023-05-18', 202320),
         ('2023-05-19', 202320), ('2023-05-20', 202320), ('2023-05-21', 202321), ('2023-05-22', 202321),
         ('2023-05-23', 202321), ('2023-05-24', 202321), ('2023-05-25', 202321), ('2023-05-26', 202321),
         ('2023-05-27', 202321), ('2023-05-28', 202322), ('2023-05-29', 202322), ('2023-05-30', 202322),
         ('2023-05-31', 202322), ('2023-06-01', 202322), ('2023-06-02', 202322), ('2023-06-03', 202322),
         ('2023-06-04', 202323), ('2023-06-05', 202323), ('2023-06-06', 202323), ('2023-06-07', 202323),
         ('2023-06-08', 202323), ('2023-06-09', 202323), ('2023-06-10', 202323), ('2023-06-11', 202324),
         ('2023-06-12', 202324), ('2023-06-13', 202324), ('2023-06-14', 202324), ('2023-06-15', 202324),],
         "day_id_day string, wee_id_week int"
        )
        check_functions.check_d_day(day_df, 202103)

    def test_check_d_day_not_104_weeks(self):
        day_df = self.spark.createDataFrame(
        [('2021-01-01', 202053), ('2021-01-02', 202053), ('2021-01-03', 202101), ('2021-01-04', 202101),
         ('2021-01-05', 202101), ('2021-01-06', 202101), ('2021-01-07', 202101), ('2021-01-08', 202101),
         ('2021-01-09', 202101), ('2021-01-10', 202102), ('2021-01-11', 202102), ('2021-01-12', 202102),
         ('2021-01-13', 202102), ('2021-01-14', 202102), ('2021-01-15', 202102), ('2021-01-16', 202102),
         ('2021-01-17', 202103), ('2021-01-18', 202103), ('2021-01-19', 202103), ('2021-01-20', 202103),
         ('2021-01-21', 202103), ('2021-01-22', 202103), ('2021-01-23', 202103), ('2021-01-24', 202104),
         ('2021-01-25', 202104), ('2021-01-26', 202104), ('2021-01-27', 202104), ('2021-01-28', 202104),
         ('2021-01-29', 202104), ('2021-01-30', 202104), ('2021-01-31', 202105), ('2021-02-01', 202105),
         ('2021-02-02', 202105), ('2021-02-03', 202105), ('2021-02-04', 202105), ('2021-02-05', 202105),
         ('2021-02-06', 202105), ('2021-02-07', 202106), ('2021-02-08', 202106), ('2021-02-09', 202106),
         ('2021-02-10', 202106), ('2021-02-11', 202106), ('2021-02-12', 202106), ('2021-02-13', 202106),
         ('2021-02-14', 202107), ('2021-02-15', 202107), ('2021-02-16', 202107), ('2021-02-17', 202107),
         ('2021-02-18', 202107), ('2021-02-19', 202107), ('2021-02-20', 202107),],
         "day_id_day string, wee_id_week int"
        )

        self.assertRaises(Exception,check_functions.check_d_day(day_df, 202103))

    def test_check_sales(self):
        sales_df = self.spark.createDataFrame(
          [(202115,95), (202116,90), (202117,97), (202118,100),], "week_id int, sales_quantity int"
        )
        check_functions.check_sales(sales_df, 202119)

    def test_check_sales_percent_minus_30(self):
        sales_df = self.spark.createDataFrame(
          [(202115,95), (202116,90), (202117,97), (202118,20),], "week_id int, sales_quantity int"
        )
        self.assertRaises(Exception,check_functions.check_sales(sales_df, 202119))

if __name__ == "__main__":
    unittest.main()
