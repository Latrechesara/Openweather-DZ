# Module 03: Real-Time Weather Architecture & Infrastructure Setup

This module establishes the foundational infrastructure for our real-time streaming pipeline. Using **Docker Compose**, we orchestrate a modern event-driven architecture featuring **Apache Kafka (KRaft mode)** for message streaming, **ClickHouse** for high-throughput real-time analytics, and a Python-based data ingestion worker.

---

## Architecture Overview

```
+---------------------+     HTTP      +------------------+     Kafka Topic    +-----------------+
|  OpenWeather API    |  -----------> |  Python Producer |  --------------->  |  Apache Kafka   |
| (External Data Source)              |   (Poller Task)  |   (weather-events) |  (KRaft Mode)   |
+---------------------+               +------------------+                    +-----------------+
                                                                                       |
                                                                                       | Stream Processing
                                                                                       v
+---------------------+     Queries   +------------------+     Batch Ingest   +-----------------+
|   Plotly Dash App   |  <----------- |    ClickHouse    |  <---------------  | Python Consumer |
| (Live Dashboard UI) |               | (Analytical DB)  |                    | (Kafka Consumer)|
+---------------------+               +------------------+                    +-----------------+
```

### Core Components

1. **Apache Kafka (KRaft Mode):** Serves as the distributed event log to decouple ingestion from storage. Running in KRaft mode eliminates the operational overhead of ZooKeeper.
2. **ClickHouse:** A column-oriented DBMS optimized for real-time analytical processing (OLAP) and fast aggregation queries.
3. **OpenWeather API Ingestion:** Collects live metrics (temperature, humidity, wind, pressure) for specified target cities at defined intervals.
4. **Plotly Dash Frontend:** Queries ClickHouse dynamically to display real-time interactive charts, historical trends, and geographical metrics.

---

## 1. Project Directory Structure

Ensure your project workspace (`realtime_weather/`) is organized as follows:

```
realtime_weather/
├── .env
├── docker-compose.yml
├── pyproject.toml
├── instructions/
│   ├── 01_environment_setup.md
│   ├── 02_kafka_clickhouse.md
│   └── 03_realtime_architecture.md
└── src/
    ├── __init__.py
    ├── producer.py
    ├── consumer.py
    └── app.py
```

---

## 2. Environment Configuration (`.env`)

Create a `.env` file in the root directory to manage your secret API credentials and configuration variables safely:

```env
# OpenWeather API Configuration
OPENWEATHER_API_KEY=your_actual_api_key_here
WEATHER_CITIES=London,New York,Tokyo,Paris,Algiers
FETCH_INTERVAL_SECONDS=60

# Kafka Broker Configuration
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=weather_metrics

# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=weather_analytics
```

---

## 3. Docker Compose Infrastructure (`docker-compose.yml`)

Create the `docker-compose.yml` file to orchestrate Kafka and ClickHouse:

```yaml:docker-compose.yml
version: '3.8'

services:
  # Apache Kafka Broker in KRaft Mode (No ZooKeeper required)
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: weather_kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    volumes:
      - kafka_data:/tmp/kraft-combined-logs

  # ClickHouse Columnar Analytical Database
  clickhouse:
    image: clickhouse/clickhouse-server:23.8-alpine
    container_name: weather_clickhouse
    ports:
      - "8123:8123" # HTTP interface
      - "9000:9000" # Native client interface
    environment:
      CLICKHOUSE_DB: weather_analytics
      CLICKHOUSE_USER: default
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

volumes:
  kafka_data:
  clickhouse_data:
```

---

## 4. Operational Execution Steps

### Step 1: Install Python Workspace Dependencies
Use `uv` to manage your environment dependencies:

```powershell
uv add kafka-python-ng clickhouse-driver requests plotly dash python-dotenv
```

### Step 2: Boot Up the Infrastructure
Start Kafka and ClickHouse in detached mode:

```powershell
docker compose up -d
```

### Step 3: Verify Service Health
Check that both containers are running smoothly:

```powershell
docker compose ps
```

To test ClickHouse connectivity via curl:

```powershell
curl "http://localhost:8123/?query=SELECT%201"
```
*(A output of `1` indicates ClickHouse is online and listening.)*

