"""
Data ingestion pipeline to consume messages from a Kafka topic
and upload them as JSON objects to an AWS S3 bucket for the Bronze layer.
This script is self-contained in a single file but uses a function-based
approach for better code organization and maintainability.
"""
import time
import requests
import json
import sys
import os
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv
import boto3
from botocore.client import Config

# Load environment variables at the top level
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

AWS_ENDPOINT = os.getenv('AWS_ENDPOINT_URL')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_S3_BUCKET = os.getenv('AWS_BUCKET_NAME')

def setup_kafka_consumer(broker, topic):
    """
    Initializes and returns a Kafka consumer instance.

    Args:
        broker (str): The Kafka broker address.
        topic (str): The Kafka topic to consume from.

    Returns:
        KafkaConsumer: The initialized KafkaConsumer object.
    """
    if not broker or not topic:
        print("Error: KAFKA_BROKER or KAFKA_TOPIC variables are not set.", file=sys.stderr)
        return None

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            enable_auto_commit=True,
            auto_offset_reset='earliest',
            group_id="bronze-consumer",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Kafka consumer initialized successfully.")
        return consumer
    except Exception as e:
        print(f"Error initializing Kafka consumer: {e}", file=sys.stderr)
        return None

def setup_s3_client(endpoint, access_key, secret_access_key, region):
    """
    Initializes and returns an S3 client instance.
    """
    if not access_key or not secret_access_key:
        print("Error: AWS access keys are not set.", file=sys.stderr)
        return None

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,   # e.g. "http://localhost:9000" or AWS S3 URL
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"})  
        )
        print("S3 client initialized successfully.")
        return s3
    except Exception as e:
        print(f"Error initializing S3 client: {e}", file=sys.stderr)
        return None


def process_and_upload_record(record, s3_client, bucket_name):
    """
    Processes a single Kafka record and uploads it to S3.

    Args:
        record (dict): The message value from Kafka.
        s3_client (boto3.client): The initialized S3 client.
        bucket_name (str): The name of the S3 bucket.
    """
    if not bucket_name:
        print("Error: AWS_BUCKET_NAME variable is not set.", file=sys.stderr)
        return

    # print(f"Received record: {record}")
    
    # Extract symbol and timestamp from the record
    symbol = record.get('symbol', 'unknown')
    timestamp_str = record.get('timestamp', datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    
    try:
        dt_object = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        print(f"Warning: Invalid timestamp format '{timestamp_str}'. Using current time.", file=sys.stderr)
        dt_object = datetime.utcnow()

    date_str = dt_object.strftime('%Y-%m-%d')
    s3_key = f"bronze/{symbol}/{date_str}/{symbol}_{dt_object.timestamp()}.json"
    
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(record),
            ContentType='application/json'
        )
        print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}", file=sys.stderr)

def consumer_main():
    """
    Main function to run the Kafka to S3 data pipeline.
    """
    consumer = setup_kafka_consumer(KAFKA_BROKER, KAFKA_TOPIC)
    s3_client = setup_s3_client(AWS_ENDPOINT, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION)

    if not consumer or not s3_client:
        sys.exit(1)

    try:
        for message in consumer:
            process_and_upload_record(message.value, s3_client, AWS_S3_BUCKET)
            sys.stdout.flush()
    except Exception as e:
        print(f"A fatal error occurred in the main application loop: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    consumer_main()
