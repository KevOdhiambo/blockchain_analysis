# Blockchain Data Analysis with Machine Learning

This project processes and analyzes blockchain data using machine learning techniques, ensuring high performance and scalability. It's built with Python and hosted on AWS.

## Features

- Data ingestion from blockchain sources (mock data for demonstration)
- High-performance data processing using Apache Spark
- Machine learning model for transaction classification
- Scalable architecture using AWS services

## Prerequisites

- Python 3.8+
- AWS account with appropriate permissions
- Apache Spark
- PySpark
- Boto3

## Installation

1. Clone the repository: git clone https://github.com/yourusername/blockchain-ml-analysis.git
cd blockchain-ml-
2. Install the required packages: pip install -r requirements.txt
3. Set up your AWS credentials:
- Create an IAM user with appropriate permissions
- Set environment variables:
  ```
  export AWS_ACCESS_KEY_ID=your_access_key
  export AWS_SECRET_ACCESS_KEY=your_secret_key
  ```

## Usage

Run the main script: python src/main.py

## Running Tests

To run the unit tests: python -m unittest discover tests

## Docker

To build and run the Docker container:
docker build -t blockchain-ml-analysis .
docker run -e AWS_ACCESS_KEY_ID=your_access_key -e AWS_SECRET_ACCESS_KEY=your_secret_key blockchain-ml-analysis

## Project Structure

- `src/`: Source code
- `tests/`: Unit tests
- `requirements.txt`: Python dependencies
- `Dockerfile`: Container definition for deployment

## License

This project is licensed under the MIT License.