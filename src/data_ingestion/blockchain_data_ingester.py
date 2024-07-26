import json
import boto3
from botocore.exceptions import ClientError

class BlockchainDataIngester:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.s3 = boto3.client('s3', 
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key,
                               region_name=region_name)

    def ingest_data(self, bucket_name, file_name, data):
        try:
            self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=json.dumps(data))
            print(f"Successfully ingested data to {bucket_name}/{file_name}")
        except ClientError as e:
            print(f"Error ingesting data: {e}")

    def generate_mock_data(self, num_transactions=1000):
        import random
        mock_data = []
        for _ in range(num_transactions):
            transaction = {
                "hash": f"0x{''.join(random.choices('0123456789abcdef', k=64))}",
                "amount": random.uniform(0.1, 10),
                "confirmations": random.randint(1, 100),
                "size": random.randint(200, 1000),
                "label": random.randint(0, 1)  # 0 for normal, 1 for suspicious
            }
            mock_data.append(transaction)
        return mock_data