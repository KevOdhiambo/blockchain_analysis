import os
from data_ingestion.blockchain_data_ingester import BlockchainDataIngester
from data_processing.data_processor import BlockchainDataProcessor
from ml_models.transaction_classifier import TransactionClassifier
from utils.aws_utils import create_s3_bucket, upload_file_to_s3

def main():
    # Set up AWS resources
    bucket_name = 'my-blockchain-bucket'
    region = 'us-west-2'
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    create_s3_bucket(bucket_name, region)

    # Ingest mock data
    ingester = BlockchainDataIngester(aws_access_key_id, aws_secret_access_key, region)
    mock_data = ingester.generate_mock_data()
    ingester.ingest_data(bucket_name, 'transactions.json', mock_data)

    # Process and analyze data
    processor = BlockchainDataProcessor()
    processed_df = processor.process_and_analyze(f's3://{bucket_name}/transactions.json')

    # Train and evaluate ML model
    classifier = TransactionClassifier(processor.spark)
    classified_df = classifier.classify_transactions(processed_df)

    # Save results
    classified_df.write.parquet(f's3://{bucket_name}/classified_transactions')

    print("Blockchain data analysis completed successfully!")

if __name__ == "__main__":
    main()