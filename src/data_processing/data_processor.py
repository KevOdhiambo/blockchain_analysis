from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType

class BlockchainDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BlockchainDataProcessing") \
            .getOrCreate()

    def load_data(self, file_path):
        return self.spark.read.json(file_path)

    def preprocess_data(self, df):
        usd_conversion_rate = 50000  # Example rate, should be updated dynamically
        usd_conversion_udf = udf(lambda x: x * usd_conversion_rate, DoubleType())
        
        return df.withColumn("amount_usd", usd_conversion_udf(col("amount")))

    def analyze_data(self, df):
        avg_transaction = df.agg({"amount_usd": "avg"}).collect()[0][0]
        print(f"Average transaction amount: ${avg_transaction:.2f}")

        transaction_count = df.count()
        print(f"Total number of transactions: {transaction_count}")

        suspicious_count = df.filter(col("label") == 1).count()
        print(f"Number of suspicious transactions: {suspicious_count}")

    def process_and_analyze(self, file_path):
        df = self.load_data(file_path)
        processed_df = self.preprocess_data(df)
        self.analyze_data(processed_df)
        return processed_df