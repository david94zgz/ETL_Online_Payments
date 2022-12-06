"""
test_balances_negative_job.py
~~~~~~~~~~~~~~~
This module contains unit tests for the transformation steps of the ETL
job defined in balances_negative_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from jobs.balances_negative_job import transform_data
from data_generator.conf.conf import eurusd_rate


class test_balances_negative_job(unittest.TestCase):

    def setUp(self):
        """Start Spark, define eur/usd rate
        """
        self.spark = (SparkSession
                            .builder
                            .appName("calculate_negative_balances")
                            .getOrCreate())
        self.eurusd_rate = eurusd_rate

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # Prepare an input data frame that mimics our source data.
        balances_input_schema = StructType([
            StructField("account", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType())
        ])
        balances_input_data = [("a10",130.,"usd"),("a11",198.,"eur"),("a12",798.,"usd"),
                            ("a13",693.,"eur"),("a14",679.,"usd"),("a15",439.,"eur"),
                            ("b10",942.,"usd"),("b11",581.,"usd"),("b12",710.,"eur"),
                            ("b13",645.,"usd"),("b14",685.,"usd"),("b15",746.,"eur")]
        balances_input_df = self.spark.createDataFrame(data=balances_input_data, schema=balances_input_schema)

        payments_input_schema = StructType([
            StructField("paid_from", StringType()),
            StructField("paid_to", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType())
        ])
        payments_input_data = [("a10","a10",384.27,"eur"),("b10","b15",355.39,"usd"),("a13","b10",424.33,"usd"),
                            ("b13","a13",331.5,"usd"),("a15","b13",418.17,"eur"),("a13","a12",130.73,"usd"),
                            ("a11","b10",210.15,"usd"),("b10","a14",265.07,"usd"),("a15","a10",27.13,"usd"),
                            ("a10","b12",345.56,"usd"),("a12","b10",152.95,"eur"),("b11","a10",57.42,"eur"),
                            ("a12","a15",319.06,"usd"),("a13","b14",79.34,"eur"),("b15","b12",196.87,"usd"),
                            ("b15","a15",128.47,"usd"),("b13","a13",157.11,"usd"),("a15","b15",176.78,"eur"),
                            ("b12","b12",20.96,"eur"),("a11","a12",183.37,"eur"),("a10","b12",265.79,"eur")]
        payments_input_df = self.spark.createDataFrame(data=payments_input_data, schema=payments_input_schema)

        # Prepare an expected data frame which is the output that we expect.  
        expected_schema = StructType([
            StructField("account", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType())
        ])
        expected_data = [("a10",-407.22,"usd"),("a11",-185.51,"eur")]
        expected_data_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Apply our transformation to the input data frame
        transformed_df = transform_data(balances_input_df, payments_input_df, self.eurusd_rate)

        # Assert the output and data of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        transformed_field_list = [*map(field_list, transformed_df.schema.fields)]
        expected_field_list = [*map(field_list, expected_data_df.schema.fields)]
        
        identical_field_list = transformed_field_list == expected_field_list
        identical_data = sorted(expected_data_df.collect()) == sorted(transformed_df.collect())

        # assert
        self.assertTrue(identical_field_list)
        self.assertTrue(identical_data)


if __name__ == '__main__':
    unittest.main()