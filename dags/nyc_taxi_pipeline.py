from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests, pandas as pd
from sqlalchemy import create_engine

DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
# URL FOR later API integration
# TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


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
