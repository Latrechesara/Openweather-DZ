# Module 05: ClickHouse Data Consumer & Ingestion Pipeline

This module details setting up the storage and consumption layer in **ClickHouse**. It covers the implementation of the native ingestion pipeline and provides a detailed architectural comparison between ClickHouse's **Native Kafka Engine** and an **External Python Consumer (`src/consumer.py`)**.

---

## 1. Architectural Comparison for Students

In real-time streaming architectures, there are two primary methods for transferring streaming data from Apache Kafka into ClickHouse:

```
[ Method A: Native ClickHouse Ingestion ]
Kafka Topic ──> ClickHouse Kafka Engine ──(Materialized View)──> Persistent MergeTree Table

[ Method B: External Application Consumer ]
Kafka Topic ──> Python Consumer (src/consumer.py) ──(Batch Insert)──> Persistent MergeTree Table
```

### Comparative Analysis

| Dimension | Native Kafka Engine (Method A) | External Python Consumer (Method B) |
| :--- | :--- | :--- |
| **Operational Overhead** | **Zero application code.** Managed entirely by ClickHouse. | Requires deploying, monitoring, and maintaining an external script/service. |
| **Throughput & Latency** | **Ultra-high performance.** Written in optimized C++ with micro-batching. | Limited by Python GIL, runtime serialization, and network I/O overhead. |
| **Data Transformation** | Performed via SQL inside a `Materialized View`. | Full flexibility of Python code (e.g., regex, machine learning, third-party APIs). |
| **Error Handling** | System-driven offset management and error logging. | Explicit `try/except` logic, retry loops, and Dead Letter Queues (DLQ). |
| **Scalability** | Scales automatically with ClickHouse cluster resources. | Must scale manually using Python process managers (e.g., Kubernetes, systemd, Docker). |

### Which Architectural Choice is Better?

> **Verdict for Production Pipelines: Method A (Native ClickHouse Kafka Engine) is Superior.**
>
> In modern data engineering, pushing ingestion logic directly to the analytical engine (**In-Database Ingestion**) is the industry standard. It eliminates the Python application layer as a single point of failure, drastically reduces network overhead, and handles scale seamlessly without extra deployment complexity.
>
> **When to select Method B (Python Consumer):** Use a Python consumer only if you must execute complex Python-native tasks *before* writing to the database—such as running real-time machine learning inference, complex geocoding, or calling third-party APIs for data enrichment.

---

## 2. Option A Implementation: Native ClickHouse Engine

### Step 1: Connect to ClickHouse Terminal

Access the interactive ClickHouse shell inside your Docker container:

```bash
docker exec -it weather_clickhouse clickhouse-client
```

### Step 2: Deploy Schema & Ingestion Pipeline

Execute the following DDL statements to set up the database, persistent table, stream engine, and materialized view:

```sql
-- 1. Create dedicated database namespace
CREATE DATABASE IF NOT EXISTS weather_analytics;

-- 2. Create target persistent MergeTree analytical table
CREATE TABLE IF NOT EXISTS weather_analytics.weather_metrics (
    city LowCardinality(String),
    country LowCardinality(String),
    temperature Float32,
    feels_like Float32,
    humidity UInt8,
    pressure UInt32,
    wind_speed Float32,
    weather_description String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (city, timestamp);

-- 3. Create Kafka engine stream table
CREATE TABLE IF NOT EXISTS weather_analytics.weather_metrics_kafka (
    city String,
    country String,
    temperature Float32,
    feels_like Float32,
    humidity UInt8,
    pressure UInt32,
    wind_speed Float32,
    weather_description String,
    timestamp UInt64
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'weather_metrics',
         kafka_group_name = 'clickhouse_weather_consumer_group',
         kafka_format = 'JSONEachRow';

-- 4. Create Materialized View to route Kafka records to MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS weather_analytics.weather_metrics_mv 
TO weather_analytics.weather_metrics AS
SELECT 
    city,
    country,
    temperature,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    weather_description,
    toDateTime(timestamp) AS timestamp
FROM weather_analytics.weather_metrics_kafka;
```

---

## 3. Option B Implementation: External Python Consumer (`src/consumer.py`)

If your pipeline requires custom Python transformations or explicit application-level retries, implement `src/consumer.py`:

```python
import json
import logging
import os
import clickhouse_connect
from dotenv import load_dotenv
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "weather_metrics"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

def run_consumer():
    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, 
        port=CLICKHOUSE_PORT, 
        database="weather_analytics"
    )

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="python_weather_consumer_group"
    )

    logging.info(f"Started Python Kafka Consumer on topic: '{TOPIC_NAME}'")
    batch = []

    for message in consumer:
        r = message.value
        batch.append([
            r["city"], r["country"], r["temperature"], r["feels_like"],
            r["humidity"], r["pressure"], r["wind_speed"],
            r["weather_description"], r["timestamp"]
        ])

        if len(batch) >= 10:
            ch_client.insert("weather_metrics", batch)
            logging.info(f"Batch inserted {len(batch)} records into ClickHouse.")
            batch.clear()

if __name__ == "__main__":
    run_consumer()
```

### Execution via `uv`

To run the custom consumer script:

```bash
uv add clickhouse-connect
uv run python src/consumer.py
```

---

## 4. Operational Monitoring & Verification

### Verify Ingested Records

Query the real-time weather table inside ClickHouse:

```sql
SELECT 
    city, 
    temperature, 
    humidity, 
    weather_description, 
    timestamp 
FROM weather_analytics.weather_metrics 
ORDER BY timestamp DESC 
LIMIT 10;
```

### Monitor Consumer Lag

Inspect active consumer streams and offsets:

```sql
SELECT 
    table, 
    consumer_id, 
    partition, 
    current_offset 
FROM system.kafka_consumers 
WHERE database = 'weather_analytics';
```

---

## 5. Live Production Guidelines

When serving this live streaming pipeline to external public users:

1. **Host Infrastructure:** Deploy the containerized stack on a cloud server (AWS EC2, DigitalOcean, Hetzner).
2. **TLS / Reverse Proxy:** Expose web interfaces through Nginx or Caddy with automated HTTPS certificates (Let's Encrypt).
3. **Storage Retention:** Configure Time-To-Live (TTL) policies on ClickHouse tables to manage disk usage automatically:
   ```sql
   ALTER TABLE weather_analytics.weather_metrics 
   MODIFY TTL timestamp + INTERVAL 30 DAY;
   ```