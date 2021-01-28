import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime as dt
import os
import unittest
from shutil import rmtree


class TestInit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Build local spark session, and set up conf
        cls.spark = SparkSession.builder.master("local").appName("Test Init").getOrCreate()
        cls.spark.sparkContext.setLogLevel('WARN')
        cls.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        cls.spark.conf.set("spark.driver.memory", "4g")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "5")

        # Paths for the test directory
        cls.location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))  # test folder path
        cls.assets = os.path.join(cls.location, "assets")  # asset folder path
        cls.data = cls.spark.read.json(os.path.join(cls.assets, "indicators"))

    def test_simple(self):

        self.assertEqual(self.data.isStreaming, False)


if __name__ == '__main__':
    unittest.main()
