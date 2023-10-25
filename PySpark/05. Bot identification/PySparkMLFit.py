import operator
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.sql import DataFrame

MODEL_PATH = 'spark_ml_model'
LABEL_COL = 'is_bot'

def model_params(gbt):
    return ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [2, 4, 6, 8]) \
        .addGrid(gbt.maxBins, [16, 32, 64]) \
        .build()


def get_transformation_stages() -> list:
    user_type_index = StringIndexer(inputCol='user_type', outputCol="user_type_index")
    platform_index = StringIndexer(inputCol='platform', outputCol="platform_index")
    assembler = VectorAssembler(inputCols=['user_type_index', 'duration', 'platform_index', 'item_info_events',
                                           'select_item_events', 'make_order_events', 'events_per_min'], outputCol="features")
    return [user_type_index, platform_index, assembler]

def build_model() -> Pipeline:
    gbt = GBTClassifier(featuresCol="features", labelCol="is_bot")
    stages = get_transformation_stages() + [gbt]
    pipeline = Pipeline(stages=stages)
    return pipeline

def build_evaluator() -> MulticlassClassificationEvaluator:
    evaluator = MulticlassClassificationEvaluator(labelCol="is_bot", predictionCol="prediction", metricName="f1")
    return evaluator

def build_tvs(rand_forest, evaluator, model_params) -> TrainValidationSplit:
    tvs = TrainValidationSplit(estimator=rand_forest,
                               estimatorParamMaps=model_params,
                               evaluator=evaluator,
                               # 80% of the data will be used for training, 20% for validation.
                               trainRatio=0.8)
    return tvs

def process(spark, data_path, model_path):
    df = spark.read.parquet(data_path)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=1234)

    pipeline = build_model()
    evaluator = build_evaluator()
    tvs = build_tvs(pipeline, evaluator, model_params(pipeline.getStages()[-1]))  # get the GBT from the end of the pipeline
    models = tvs.fit(train_df)
    best = models.bestModel
    predictions = best.transform(test_df)
    f1_score = evaluator.evaluate(predictions)  # this is now the f1 score
    print(f"F1 Score: {f1_score}")
    best.save(model_path)
    return best


def main(data_path, model_path):
    spark = _spark_session()
    process(spark, data_path, model_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)
