# Module 05: ClickHouse Real-Time Consumer & Ingestion Pipeline

This module details setting up the storage and consumption layer in **ClickHouse**. Rather than using external Python consumer loops, ClickHouse provides a native, highly performant **Kafka Engine** paired with **Materialized Views** to continuously pull events from Apache Kafka and persist them directly into columnar storage.

## 1. Architecture & Ingestion Flow

```
+--------------------------+
|  Kafka Broker (KRaft)    |
|  Topic: weather_metrics  |
+------------+-------------+
             |
             | (Continuous Background Pull)
             v
+--------------------------+
|  Kafka Table Engine      |
|  (Buffer / Stream Table) |
+------------+-------------+
             |
             | (Triggered on Block Ingestion)
             v
+--------------------------+
|  Materialized View       |
|  (Transforms & Routes)   |
+------------+-------------+
             |
             | (Persistent Disk Write)
             v
+--------------------------+
|  MergeTree Table         |
|  (Analytical Database)   |
+--------------------------+
```

## 2. Advantages of Native ClickHouse Ingestion

* **Zero Custom Consumer Code:** Eliminates the operational overhead of running, scaling, and monitoring custom Python consumer scripts.
* **Micro-Batch Performance:** Automatically bundles incoming JSON streams into optimized native columnar blocks in memory before flushing to disk.
* **Offset Management:** Manages Kafka consumer group offsets natively inside ClickHouse transactions to ensure at-least-once processing guarantees.
```powershell
docker compose up --build -d
```
docker compose up --build -d
## 3. Database Schema Implementation

### Step 1: Access ClickHouse Client

Open a terminal and connect to the interactive client inside your running container:

```powershell
docker exec -it weather_clickhouse clickhouse-client
```

### Step 2: Execute DDL Statements

Run the following SQL code inside the ClickHouse client shell:

```sql
-- 1. Create dedicated database namespace
CREATE DATABASE IF NOT EXISTS weather_analytics;

-- 2. Create the target persistent MergeTree table
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

-- 3. Create the Kafka Engine interface table
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

## 4. Verification & Operational Queries

### Query Ingested Stream Records

Check that Algerian city events emitted by `src/producer.py` are being written live:

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

### Analytical Aggregations

Compute live metric summaries across cities:

```sql
SELECT 
    city,
    count() AS total_records,
    round(avg(temperature), 2) AS avg_temp_c,
    max(temperature) AS max_temp_c,
    min(temperature) AS min_temp_c
FROM weather_analytics.weather_metrics
GROUP BY city
ORDER BY avg_temp_c DESC;
```

## 5. Live Production Guidelines

When deploying this streaming pipeline into production environments for public users:

1. **Host Infrastructure:** Run the Docker Compose stack on a cloud VM (AWS EC2, DigitalOcean droplet, Hetzner) with a domain name attached.
2. **Reverse Proxy & SSL:** Place Nginx or Caddy in front of the dashboard and database ports, configured with HTTPS via Let's Encrypt.
3. **Data Retention Policy:** Define a TTL (Time-To-Live) on the target table to automatically manage disk footprint:
   ```sql
   ALTER TABLE weather_analytics.weather_metrics 
   MODIFY TTL timestamp + INTERVAL 30 DAY;
   ```