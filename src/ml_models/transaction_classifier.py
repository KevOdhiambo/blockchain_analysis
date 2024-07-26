from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

class TransactionClassifier:
    def __init__(self, spark):
        self.spark = spark

    def prepare_features(self, df):
        assembler = VectorAssembler(inputCols=["amount_usd", "confirmations", "size"], outputCol="features")
        return assembler.transform(df)

    def train_model(self, train_data):
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
        pipeline = Pipeline(stages=[rf])
        model = pipeline.fit(train_data)
        return model

    def evaluate_model(self, model, test_data):
        predictions = model.transform(test_data)
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print(f"Model Accuracy: {accuracy}")

    def classify_transactions(self, df):
        feature_df = self.prepare_features(df)
        train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)
        model = self.train_model(train_data)
        self.evaluate_model(model, test_data)
        return model.transform(feature_df)