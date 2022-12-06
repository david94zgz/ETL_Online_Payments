"""
etl_job.py
~~~~~~~~~~

This Python module contains the Apache Spark ETL job that generates the 
'balance_negative' dataframe, which will be saved in data as a CSV and as
a Parquet file. This 'balance_negative' is a DF with the account with a 
negative amount after all the payments are effective.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from data_generator.conf.conf import eurusd_rate


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application
    spark = (SparkSession
    .builder
    .appName("calculate_negative_balances")
    .getOrCreate())

    # execute ETL pipeline
    balances_df, payments_df = extract_data(spark)
    balances_negative = transform_data(balances_df, payments_df, eurusd_rate)
    load_data(balances_negative)

    # terminate Spark application
    spark.stop()
    return None


def extract_data(spark):
    """Load balances and payments data from CSV file format at the 'data'
    directoy.

    :param spark: Spark session.
    :return: balances and payments DataFrame.
    """
    balances_input_schema = StructType([
            StructField("account", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType())
        ])
    balances_df = (spark.read.format("csv")
        .option("header", "true")
        .schema(balances_input_schema)
        .load('data/balances.csv'))
    
    payments_input_schema = StructType([
            StructField("paid_from", StringType()),
            StructField("paid_to", StringType()),
            StructField("amount", DoubleType()),
            StructField("currency", StringType())
        ])
    payments_df = (spark.read.format("csv")
        .option("header", "true")
        .schema(payments_input_schema)
        .load("data/payments.csv"))

    return balances_df, payments_df


def transform_data(balances_df, payments_df, eurusd_rate):  
    """Transform original datasets to a new one with the accounts with negative
    balance after the payments are effective.

    :param balances_df: Input balances DataFrame.
    :param payments_df: Input payments DataFrame.
    :param eurusd_rate: eur/usd rate
    :return: Transformed DataFrame.
    """
    clean_payments_df = payments_df.filter(F.col("paid_from") != F.col("paid_to"))

    payments_paid_eur = clean_payments_df.filter(F.col("currency") == "eur"). \
                              groupBy("paid_from"). \
                              agg(F.round(F.sum("amount"), 2).alias("amount_paid_eur"))
    payments_paid_usd = clean_payments_df.filter(F.col("currency") == "usd"). \
                              groupBy("paid_from"). \
                              agg(F.round(F.sum("amount"), 2).alias("amount_paid_usd"))

    payments_received_eur = clean_payments_df.filter(F.col("currency") == "eur"). \
                              groupBy("paid_to"). \
                              agg(F.round(F.sum("amount"), 2). alias("amount_received_eur"))
    payments_received_usd = clean_payments_df.filter(F.col("currency") == "usd"). \
                              groupBy("paid_to"). \
                              agg(F.round(F.sum("amount"), 2). alias("amount_received_usd"))

    balances_after_payments = balances_df.join(payments_paid_eur, 
                                            balances_df.account == payments_paid_eur.paid_from, 
                                            "left"). \
                                        join(payments_paid_usd, 
                                            balances_df.account == payments_paid_usd.paid_from, 
                                            "left"). \
                                        join(payments_received_eur,
                                            balances_df.account == payments_received_eur.paid_to,
                                            "left"). \
                                        join(payments_received_usd,
                                            balances_df.account == payments_received_usd.paid_to,
                                            "left"). \
                                        fillna(0). \
                                        withColumn("amount", F.when(balances_df.currency == "eur", 
                                                                  F.round(F.col("amount") - F.col("amount_paid_eur") + F.col("amount_received_eur") - (F.col("amount_paid_usd") - F.col("amount_received_usd")) * F.lit(1/eurusd_rate), 2)). \
                                                            otherwise(
                                                                  F.round(F.col("amount") - (F.col("amount_paid_eur") - F.col("amount_received_eur")) * F.lit(eurusd_rate) - F.col("amount_paid_usd") + F.col("amount_received_usd"), 2))). \
                                        drop("amount_paid_usd", "amount_paid_eur", "amount_received_usd", "amount_received_eur", "paid_from", "paid_to")

    balances_negative = balances_after_payments.filter("amount < 0")
    
    return balances_negative


def load_data(balances_negative):
    """Collect data locally and save as a CSV and Parquet in 'data' directory.

    :param df: transformed DataFrame to save.
    :return: None
    """
    balances_negative.write. \
        option("header", "true"). \
        option("delimiter", ","). \
        mode("overwrite"). \
        csv("data/balances_negative_csv")

    balances_negative.write. \
        option("header", "true"). \
        option("delimiter", ","). \
        mode("overwrite"). \
        parquet("data/balances_negative_parquet")

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
