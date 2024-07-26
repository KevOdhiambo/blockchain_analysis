import unittest
from pyspark.sql import SparkSession
from src.data_processing.data_processor import BlockchainDataProcessor

class TestDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("BlockchainDataProcessingTest") \
            .master("local[2]") \
            .getOrCreate()
        
        cls.test_data = [
            {"hash": "0x123", "amount": 1.5, "confirmations": 10, "size": 500, "label": 0},
            {"hash": "0x456", "amount": 2.0, "confirmations": 20, "size": 600, "label": 1},
        ]
        cls.test_df = cls.spark.createDataFrame(cls.test_data)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.processor = BlockchainDataProcessor()

    def test_preprocess_data(self):
        processed_df = self.processor.preprocess_data(self.test_df)
        self.assertTrue("amount_usd" in processed_df.columns)
        
        # Check if USD conversion is correct
        usd_amounts = processed_df.select("amount_usd").collect()
        self.assertAlmostEqual(usd_amounts[0][0], 1.5 * 50000, places=2)
        self.assertAlmostEqual(usd_amounts[1][0], 2.0 * 50000, places=2)

if __name__ == '__main__':
    unittest.main()