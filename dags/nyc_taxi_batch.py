from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from sqlalchemy import create_engine

import pandas as pd
import os

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
# URL FOR later API integration
# TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# Alt function for loading whole directory
def load_bronze_dir(dir_path):

    print(f"Loading directory {dir_path}")

    files = [f for f in os.listdir(dir_path) if f.endswith(".parquet")]
    print(f"Found {len(files)} files in {dir_path}")

    engine = create_engine(DB_CONN)

    for filename in sorted(files):
        file_path = os.path.join(dir_path, filename)
        print(f"Loading {filename}...")

        df = pd.read_parquet(file_path)
        df["_loaded_at"] = datetime.utcnow()
        df["_source_file"] = filename  # handy for traceability

        df.to_sql(
            name="yellow_trips_raw",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            chunksize=10_000,
        )
        print(f"  → {len(df):,} rows loaded")


def load_bronze(file_path):

    print(f"Reading local file: {file_path}")

    # Testing API fetching... ideally able to get new parquet every month

    # url = TAXI_URL.format(year=year, month=month)
    # print(f"Downloading: {url}")

    df = pd.read_parquet(file_path)
    df["loaded_at"] = datetime.now()
    df["source_file"] = file_path

    engine = create_engine(DB_CONN)
    df.to_sql(
        name="taxi_bronze",
        con=engine,
        schema="bronze",
        if_exists="append", # Make sure the data is appended on each run
        index=False,
        chunksize=10_000,
    )
    print(f"Loaded {len(df):,} rows into bronze.taxi_bronze")

with DAG(
    "bgd_taxi",
    start_date=datetime(2024, 1, 1), # Placeholder - for when the script is finishes to fetch data from API
    schedule="@monthly", # Same as above - but the files are published monthly, so it makes sense
    catchup=False,
    tags=["BGD"]
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=load_bronze,
        op_kwargs={"file_path": "/opt/airflow/data/yellow_tripdata_2024-01.parquet"},
        # Provide path to parquet file - work out how to launch this thing from cmd?
        # Also when launching from docker - needs to be attached volume path
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command="cd /opt/airflow/dbt && dbt run --select silver --profiles-dir .",
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command="cd /opt/airflow/dbt && dbt run --select gold --profiles-dir .",
    )

    # Tests - apparently good for proper production environments?
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    ingest_bronze >> dbt_silver >> dbt_gold >> dbt_test
