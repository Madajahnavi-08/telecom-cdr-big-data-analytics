# Multi-source Big Data Fusion for Telecom CDR Analytics

A B.Tech final-year Big Data Analytics project demonstrating multi-source telecom Call Detail Record (CDR) integration, event streaming with Apache Kafka, stream processing with Spark Structured Streaming, and interactive analysis in Streamlit.

## Features

- Interactive Streamlit dashboard with KPI cards and charts
- Upload and normalize CDR CSV data from different source systems
- Demo dataset of 600 synthetic records (not real subscriber data)
- Traffic trends, source contribution, call status, top cell towers, and top callers
- Download filtered records as CSV
- Kafka producer that publishes CDR records as JSON events
- Spark Structured Streaming consumer that reads Kafka and writes JSON output

## Architecture

```text
CDR Source A ─┐
CDR Source B ─┼─> CSV/JSON normalization ─> Apache Kafka (telecom-cdr)
CDR Source C ─┘                                  │
                                                 v
                                    Spark Structured Streaming
                                                 │
                                                 v
                                       JSON output / analytics
                                                 │
                                                 v
                                  Streamlit dashboard + CSV export
```

The Streamlit dashboard runs independently in demo/upload mode. The Kafka/Spark scripts are an optional streaming demonstration and require a running Kafka broker plus the Spark Kafka connector package.

## 1. Create an environment

Python 3.10 or 3.11 is recommended for the easiest compatibility.

```bash
python -m venv .venv
```

Windows:
```bash
.venv\Scripts\activate
```

macOS/Linux:
```bash
source .venv/bin/activate
```

Install dashboard dependencies:
```bash
pip install -r requirements.txt
```

## 2. Run the Streamlit dashboard

```bash
streamlit run app.py
```

Streamlit opens a local browser tab. The demo dataset loads automatically. You can upload your own CSV using the sidebar.

Expected columns:
`timestamp, source, caller_id, receiver_id, duration_sec, data_mb, cell_id, call_status`

The app accepts some common alternate column names, such as `event_time`, `call_time`, `caller`, `receiver`, `duration`, and `status`.

## 3. Optional Kafka + Spark streaming pipeline

First start a Kafka broker and create a topic named `telecom-cdr` (or set `KAFKA_TOPIC` to your topic). Make sure the Kafka broker is reachable at `localhost:9092`.

Terminal 1 — Spark consumer (example command; Kafka connector must match your Spark/Scala versions):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_streaming_consumer.py
```

Terminal 2 — publish demo events:
```bash
python kafka_producer.py
```

Environment variables:
- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `telecom-cdr`)
- `CDR_OUTPUT_PATH` (default `output/cdr_stream`)
- `CDR_CHECKPOINT_PATH` (default `output/checkpoint`)

> The streaming scripts are optional and were not required to launch the interactive dashboard. Kafka/Spark setup depends on your local environment and compatible Java, Spark, Scala, and Kafka versions.

## Data privacy

Only synthetic sample records are included. Do not upload real subscriber identifiers or sensitive telecom records to a public repository. Mask or anonymize identifiers before sharing project data.

## Suggested GitHub repository name

`telecom-cdr-big-data-analytics`

## Suggested GitHub description

Multi-source telecom CDR analytics using Apache Kafka, Spark Structured Streaming, Python, and Streamlit.

## Suggested topics

`big-data`, `apache-kafka`, `spark-structured-streaming`, `streamlit`, `telecom-analytics`, `python`, `btech-project`

## Resume description

Built a multi-source telecom CDR analytics prototype that normalizes records from different systems, demonstrates event ingestion through Apache Kafka and processing with Spark Structured Streaming, and presents call traffic, status, data usage, and cell-tower insights through an interactive Streamlit dashboard.
