# 🌤️ MeteoFlow-DZ: Real-Time Weather Streaming & Analytics Infrastructure

An enterprise-grade, end-to-end real-time event-driven streaming pipeline and interactive geospatial dashboard designed to process and visualize real-time weather telemetry across major Algerian Wilayas.

---

## 🎯 System Architecture

```
+---------------------+     HTTP GET      +-------------------+     Kafka Event Stream    +--------------------+
|  OpenWeather API    | ----------------> |  Python Producer  | ------------------------> |  Apache Kafka      |
| (External Telemetry)|                   | (Background Task) |                           | (KRaft Mode Engine)|
+---------------------+                   +-------------------+                           +--------------------+
                                                                                                    |
                                                                                                    | Native Ingestion
                                                                                                    v
+---------------------+    SQL Queries    +-------------------+    Materialized View View    +--------------------+
|   Plotly Dash App   | <---------------- |  ClickHouse DB    | <--------------------------- | Kafka Engine Table |
| (Interactive UI/Map)|                   | (Columnar OLAP)   |                              | (In-Memory Buffer) |
+---------------------+                   +-------------------+                              +--------------------+
```

### Key Architectural Characteristics
- **Zero-Consumer Engine:** Leverages ClickHouse's native **Kafka Table Engine** and **Materialized Views** to directly ingest, transform, and store streaming records without external consumer worker overhead.
- **ZooKeeper-Less Messaging:** Uses **Apache Kafka in KRaft mode** for lightweight, robust event streaming and partition management.
- **Automated First-Boot Schema Initialization:** Fully automates database schema and table setup using Docker Compose entrypoint volume bindings (`/docker-entrypoint-initdb.d`), requiring zero manual terminal or SQL intervention.

---

## 🛠️ Tech Stack & Infrastructure

- **Language & Package Manager:** Python 3.11+, [`uv`](https://github.com/astral-sh/uv) (Astral workspace manager)
- **Message Broker:** Apache Kafka (Confluent Platform `7.6.0`, KRaft Mode)
- **Analytical Storage:** ClickHouse DBMS (`latest` image with `MergeTree` time-series engine)
- **Visualization:** Plotly Dash, Bootstrap Components, OpenStreetMap Geospatial Scatter
- **Orchestration:** Docker & Docker Compose v2

---

## 📁 Repository Structure

```
realtime_weather/
├── docker-compose.yml          # Multi-container service specification
├── Dockerfile                  # Container definition for Producer & Dash App
├── pyproject.toml              # UV dependency configuration
├── .env                        # Environment parameters & API keys
├── init-scripts/
│   └── 01_schema.sql           # Auto-executed ClickHouse DDL on container boot
└── src/
    ├── __init__.py
    ├── producer.py             # OpenWeather API poller & Kafka event producer
    └── app.py                  # Real-time Plotly Dash web application
```

---

## 🚀 Automated Database Initialization (Zero-Touch Setup)

To avoid manual terminal commands or interactive SQL shell queries during deployment, ClickHouse executes initialization scripts placed in the container's `/docker-entrypoint-initdb.d/` directory upon first volume mount.

### `init-scripts/01_schema.sql`

```sql
-- 1. Create target analytical database
CREATE DATABASE IF NOT EXISTS weather_analytics;

-- 2. Create persistent MergeTree time-series table
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

-- 3. Create Kafka Engine buffer table
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

-- 4. Create Materialized View for automated stream piping
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

## ⚙️ Environment Configuration (`.env`)

Create a `.env` file in the root directory prior to deployment:

```env
# OpenWeather API Configuration
OPENWEATHER_API_KEY=your_actual_openweather_api_key

# Infrastructure Connection Parameters
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_PASSWORD=weather123
```

---

## 🐳 Docker Compose Orchestration (`docker-compose.yml`)

```yaml
version: '3.8'

services:
  # Apache Kafka Broker in KRaft Mode
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: weather_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      KAFKA_METADATA_LOG_DIR: '/tmp/kraft-combined-logs'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      CLUSTER_ID: 'Mk3OEYBSD3TuFYwWPvQO4A=='

  # ClickHouse Columnar Database Engine
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: weather_clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      - CLICKHOUSE_PASSWORD=weather123
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./init-scripts:/docker-entrypoint-initdb.d # Automated DDL execution on boot

  # OpenWeather API Producer Service
  producer:
    build: .
    container_name: weather_producer
    command: python src/producer.py
    depends_on:
      - kafka
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - OPENWEATHER_API_KEY=${OPENWEATHER_API_KEY}

  # Plotly Dash Live Dashboard
  dashboard:
    build: .
    container_name: weather_dashboard
    command: python src/app.py
    depends_on:
      - clickhouse
    ports:
      - "8050:8050"
    environment:
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PASSWORD=weather123

volumes:
  clickhouse_data:
```

---

## ⚡ Deployment & Running

### 1. Fresh Single-Command Launch

To wipe any legacy container state, build images, and bring up the full microservice suite:

```bash
docker compose down -v
docker compose up --build -d
```

### 2. Verify Infrastructure Status

Ensure all four microservice containers are in the `Up` state:

```bash
docker compose ps
```

### 3. Access the Dashboard

Open your browser and navigate to:
```
http://localhost:8050
```

---

## 📊 Operational & Inspection Queries

You can execute queries against ClickHouse via curl or directly inside the container to verify live metric ingestion:

```bash
# Check raw ingested records across Algerian cities
docker exec -it weather_clickhouse clickhouse-client --query="
SELECT city, temperature, humidity, weather_description, timestamp 
FROM weather_analytics.weather_metrics 
ORDER BY timestamp DESC 
LIMIT 10;
"

# Compute real-time city aggregations
docker exec -it weather_clickhouse clickhouse-client --query="
SELECT 
    city,
    count() AS record_count,
    round(avg(temperature), 2) AS avg_temp_c,
    max(temperature) AS max_temp_c,
    min(temperature) AS min_temp_c
FROM weather_analytics.weather_metrics
GROUP BY city
ORDER BY avg_temp_c DESC;
"
```