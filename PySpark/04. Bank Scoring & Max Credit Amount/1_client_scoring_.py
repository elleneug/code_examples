from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.sql.types import IntegerType


def model_params(rf):
    return ParamGridBuilder() \
        .addGrid(rf.maxDepth, [2, 3, 4, 5]) \
        .addGrid(rf.maxBins, [2, 3, 4]) \
        .build()


def prepare_data(df: DataFrame, assembler) -> DataFrame:
    df = df.withColumn("is_married", df.married.cast(IntegerType()))
    sex_index = StringIndexer(inputCol='sex', outputCol="sex_index")
    #married_index = StringIndexer(inputCol='married', outputCol="is_married")
    df = sex_index.fit(df).transform(df)
    #df = married_index.fit(df).transform(df)
    df = assembler.transform(df)
    return df


def vector_assembler() -> VectorAssembler:
    #input_cols = [col for col in train_df.columns if col != 'is_credit_closed']
    assembler = VectorAssembler(inputCols=['age', 'sex_index', 'is_married', 'salary', 'successfully_credit_completed', 'credit_completed_amount', 'active_credits', 'active_credits_amount', 'credit_amount'], outputCol="features")
    return assembler


def build_random_forest() -> RandomForestClassifier:
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_credit_closed")
    return rf


def build_evaluator() -> MulticlassClassificationEvaluator:
    evaluator = MulticlassClassificationEvaluator(labelCol="is_credit_closed", predictionCol="prediction", metricName="accuracy")
    return evaluator


def build_tvs(rand_forest, evaluator, model_params) -> TrainValidationSplit:
    tvs = TrainValidationSplit(estimator=rand_forest,
                               estimatorParamMaps=model_params,
                               evaluator=evaluator,
                               # 80% of the data will be used for training, 20% for validation.
                               trainRatio=0.8)
    return tvs


def train_model(train_df, test_df) -> (RandomForestClassificationModel, float):
    assembler = vector_assembler()
    train_pdf = prepare_data(train_df, assembler)
    test_pdf = prepare_data(test_df, assembler)
    rf = build_random_forest()
    evaluator = build_evaluator()
    tvs = build_tvs(rf, evaluator, model_params(rf))
    models = tvs.fit(train_pdf)
    best = models.bestModel
    predictions = best.transform(test_pdf)
    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy: {accuracy}")
    print(f'Model maxDepth: {best._java_obj.getMaxDepth()}')
    print(f'Model maxBins: {best._java_obj.getMaxBins()}')
    return best, accuracy


if __name__ == "__main__":
    spark = SparkSession.builder.appName('PySparkMLJob').getOrCreate()
    train_df = spark.read.parquet("train.parquet")
    test_df = spark.read.parquet("test.parquet")
    train_model(train_df, test_df)
