import argparse
import os
os.environ['SPARK_VERSION'] = '3.1'

import pydeequ
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


def process(spark, data_path, report_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param report_path: путь сохранения отчета
    """
    df = spark.read.parquet(data_path)
    
    check_completeness = Check(spark, CheckLevel.Warning, "Data Quality Check")\
        .isComplete("id")\
        .isComplete("vendor_code")\
        .isComplete("name")\
        .isComplete("type")\
        .isComplete("label")\
        .isComplete("price")\
        .isComplete("discount")\
        .isComplete("available_count")\
        .isComplete("preorder_count")
    
    check_dataset = Check(spark, CheckLevel.Error, "Users Dataset Check")\
            .isUnique("id")\
            .hasSize(lambda x: x >= 100000)
    
    check_discount = Check(spark, CheckLevel.Warning, "Discount Check")\
                .isNonNegative("discount")\
                .hasMax("discount", lambda x: x <= 100)

    check_orders = Check(spark, CheckLevel.Warning, "Discount Check")\
                .isNonNegative("available_count")\
                .isNonNegative("preorder_count")
    
    

    check = Check(spark, CheckLevel.Warning, "Data Quality Check")
    checkResult = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(check_completeness) \
            .addCheck(check_dataset) \
            .addCheck(check_discount) \
            .addCheck(check_orders) \
            .run()
    
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    
    final_df = VerificationResult.successMetricsAsDataFrame(spark, checkResult)
    final_df.write.mode("overwrite").parquet(report_path)
       
    return
    

def main(data_path, report_path):
    spark = _spark_session()
    process(spark, data_path, report_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession \
        .builder.appName('PySparkAnalyzer') \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='data.parquet', help='Please set datasets path.')
    parser.add_argument('--report_path', type=str, default='report', help='Please set target report path.')
    args = parser.parse_args()
    data_path = args.data_path
    report_path = args.report_path
    main(data_path, report_path)
