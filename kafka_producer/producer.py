import os, sys
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

def get_minio_prefix():
    """
    Constructs the MinIO prefix based on the current date.
    """
    execution_date = datetime.now()
    prefix = f"bronze/AAPL/{execution_date.strftime('%Y-%m-%d')}/"
    return prefix

def download_from_minio(**kwargs):
    """
    Downloads all objects from a MinIO bucket to a local directory.
    Uses Airflow Connections and Variables.
    """
    minio_conn = BaseHook.get_connection('minio_conn')
    bucket = Variable.get("minio_bucket")
    local_dir = Variable.get("local_dir")
    prefix = get_minio_prefix()

    print(f"Connecting to MinIO with endpoint: {minio_conn.host}")
    
    try:
        os.makedirs(local_dir, exist_ok=True)
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_conn.host,
            aws_access_key_id=minio_conn.login,
            aws_secret_access_key=minio_conn.password
        )
        
        print(f"Connected to MinIO, accessing bucket: {bucket} with prefix: {prefix}")
        
        # New logging to check for files
        print(f"Searching for files with prefix: {prefix} in bucket: {bucket}")
        
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])

        if not objects:
            print(f"No objects found with prefix: {prefix}")
            return []

        # New logging to show the files found
        print(f"Found the following objects in MinIO: {objects}")

        local_files = []
        
        for obj in objects:
            key = obj["Key"]
            if not key.endswith('/'):
                local_file = os.path.join(local_dir, os.path.basename(key))
                s3.download_file(bucket, key, local_file)
                print(f"Downloaded {key} -> {local_file}")
                local_files.append(local_file)
        
        print(f"Found {len(local_files)} file(s) in MinIO bucket {bucket}")
        return local_files
    except Exception as e:
        print(f"Error during MinIO download: {e}", file=sys.stderr)
        return []

def load_to_snowflake(**kwargs):
    """
    Loads downloaded files from the local directory into a Snowflake table.
    Uses Airflow Connections and XCom.
    """
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')

    print(f"Files to load: {local_files}")
    if not local_files:
        print("No files to load. Exiting.")
        return

    conn = None
    cur = None
    try:
        snowflake_conn = BaseHook.get_connection('snowflake_conn')
        
        user = snowflake_conn.login
        password = snowflake_conn.password
        account = snowflake_conn.extra_dejson.get('account')
        warehouse = snowflake_conn.extra_dejson.get('warehouse')
        database = snowflake_conn.extra_dejson.get('database')
        schema = snowflake_conn.extra_dejson.get('schema')

        print(f"Connecting to Snowflake with user: {user}")
        print(f"Using account: {account}")
        
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        cur = conn.cursor()

        for f in local_files:
            res = cur.execute(f"PUT file://{f} @{database}.{schema}.%bronze_stock_quotes_raw").fetchall()
            print(f"PUT result for {f}: {res}")

        res = cur.execute(f"""
            COPY INTO {database}.{schema}.bronze_stock_quotes_raw
            FROM @{database}.{schema}.%bronze_stock_quotes_raw
            FILE_FORMAT = (TYPE=JSON)
        """).fetchall()
        print(f"COPY INTO result: {res}")

    except Exception as e:
        print(f"Error during Snowflake load: {e}", file=sys.stderr)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task1 >> task2
    