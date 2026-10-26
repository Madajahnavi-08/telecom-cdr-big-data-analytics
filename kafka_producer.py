"""Demo CDR producer. Requires Kafka running and kafka-python installed."""
import csv, json, os, time
from pathlib import Path
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telecom-cdr")
CSV_PATH = Path(__file__).parent / "sample_data" / "cdr_sample.csv"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    retries=5,
)
with CSV_PATH.open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        row["duration_sec"] = float(row["duration_sec"])
        row["data_mb"] = float(row["data_mb"])
        producer.send(TOPIC, row)
        print(f"Sent CDR event: {row['timestamp']} | {row['source']}")
        time.sleep(0.25)  # simulate incoming events
producer.flush()
producer.close()
print(f"Finished publishing records to topic: {TOPIC}")
