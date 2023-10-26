import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
import pathlib


def process(spark: SparkSession, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """

    flights_df = spark.read \
        .option("header", "true") \
        .parquet(flights_path)

    result_flights_df = flights_df.filter(~F.col('TAIL_NUMBER').isNull())\
        .groupBy(F.col('TAIL_NUMBER')
                 ) \
        .agg(F.count(F.col('FLIGHT_NUMBER')).alias('count'))\
        .select(F.col('TAIL_NUMBER'),
                F.col('count'))\
        .orderBy(F.col('count').desc())\
        .limit(10)

    result_flights_df.write.mode("overwrite").parquet(result_path)



def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Create the SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)