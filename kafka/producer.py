import pandas as pd
import json
from kafka import KafkaProducer
from datetime import datetime

TOPIC = "taxi-trips"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

def stream_parquet(file_path: str):
    df = pd.read_parquet(file_path)
    print(f"Streaming {len(df):,} rows to topic '{TOPIC}'...")

    for _, row in df.iterrows():
        message = row.to_dict()
        message["_produced_at"] = datetime.utcnow().isoformat()
        producer.send(TOPIC, value=message)

    producer.flush()
    print("Done streaming.")

if __name__ == "__main__":
    stream_parquet("data/") # Provide data path