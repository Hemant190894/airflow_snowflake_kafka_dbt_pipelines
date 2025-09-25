# airflow_snowflake_kafka_dbt_pipelines


Medallion Architecture Data Pipeline
This project implements a complete data pipeline using the Medallion Architecture. Real-time stock quote data is ingested via Kafka and stored in a MinIO data lake. Apache Airflow orchestrates the flow of data through three distinct layers in Snowflake:

Bronze: Raw data loaded as-is.

Silver: Cleaned and structured data from the raw layer.

Gold: Aggregated and curated data for analytics.

The pipeline uses dbt for all transformations, ensuring a maintainable and version-controlled approach to data quality.

Tech Stack
Orchestration: Apache Airflow

Real-Time Data: Apache Kafka

Data Lake: MinIO

Data Warehouse: Snowflake

Transformation: dbt (Data Build Tool)

Containerization: Docker

Cloud/S3 Client: Boto3 (orchestrated via custom modules)
