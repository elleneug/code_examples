import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """

    flights_df = spark.read.parquet(flights_path)

    result_df = flights_df.where(F.col('TAIL_NUMBER').isNotNull()) \
        .groupBy(F.col('ORIGIN_AIRPORT'), F.col('DESTINATION_AIRPORT')
                 ) \
        .agg(F.count(F.col('TAIL_NUMBER')).alias('tail_count'),
             F.avg(F.col('AIR_TIME')).alias('avg_air_time')) \
        .select(F.col('ORIGIN_AIRPORT'),
                F.col('DESTINATION_AIRPORT'),
                F.col('tail_count'),
                F.col('avg_air_time')
                ) \
        .orderBy(F.col('tail_count').desc()) \
        .limit(10)

    result_df.write.parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob2').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
