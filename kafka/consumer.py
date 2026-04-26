import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from datetime import datetime

TOPIC = "taxi-trips"
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost/airflow"
BATCH_SIZE = 500   # write to Postgres every 500 messages

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="bronze-writer",
)

engine = create_engine(DB_CONN)
buffer = []

print(f"Listening to topic '{TOPIC}'...")

for message in consumer:
    record = message.value
    record["_loaded_at"] = datetime.utcnow()
    buffer.append(record)

    if len(buffer) >= BATCH_SIZE:
        df = pd.DataFrame(buffer)
        df.to_sql(
            name="yellow_trips_raw",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
        )
        print(f"Written batch of {len(buffer)} rows to Bronze")
        buffer.clear()