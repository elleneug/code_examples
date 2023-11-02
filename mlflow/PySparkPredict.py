import argparse
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession

import mlflow

os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'
os.environ['AWS_ACCESS_KEY_ID'] = 'VsSLmhBg5or3QeP-bYwW'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'e61seRCXf_STt5CFDQ8yoRXHWWHam_D9_pqnHGDe'


schema = StructType([
    StructField("driver_id", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("car_class", StringType(), True),
    StructField("driving_experience", IntegerType(), True),
    StructField("speeding_penalties", IntegerType(), True),
    StructField("parking_penalties", IntegerType(), True),
    StructField("total_car_accident", IntegerType(), True),
])


def process(spark, data_path, result, model_uri):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param result: путь сохранения результата
    """

    if not os.path.exists(result):
        os.makedirs(result)

    # Загрузить датасет
    data_df = spark.read.schema(schema).parquet(data_path)

    # Загрузить модель из MLflow
    model = mlflow.spark.load_model(model_uri)

    # Получить предсказания
    predictions = model.transform(data_df).select("driver_id", "prediction")

    # Сохранить датасет с предсказаниями в указанный путь
    predictions.write.parquet(os.path.join(result, "predictions.parquet"))

def main(data_path, result_path, model_uri):
    spark = _spark_session()
    process(spark, data_path, result_path, model_uri)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkPredict').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='data.parquet', help='Please set datasets path.')
    parser.add_argument('--result', type=str, default='result', help='Please set result path.')
    parser.add_argument('--model_uri', type=str, default='s3://kc-mlflow/341/b7bc411a8da04297864ffd3d800bb5f7/artifacts/e-sidorova/', help='The URI of the model to load from MLflow.')
    args = parser.parse_args()
    data = args.data
    result = args.result
    model_uri = args.model_uri
    main(args.data, args.result, args.model_uri)
