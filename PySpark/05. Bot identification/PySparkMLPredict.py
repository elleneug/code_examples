import argparse
from pyspark.sql import DataFrame
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

MODEL_PATH = 'spark_ml_model'

def main(data_path, model_path, result_path):
    spark = _spark_session()
    df = spark.read.parquet(data_path)
    model = PipelineModel.load(model_path)
    predictions = model.transform(df)
    result_df = predictions.select("session_id", "prediction")
    result_df.write.parquet(result_path)
    return

def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    parser.add_argument('--data_path', type=str, default='test.parquet', help='Please set datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    result_path = args.result_path
    main(data_path, model_path, result_path)
