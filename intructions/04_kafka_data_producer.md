# Module 04: OpenWeather API Ingestion & Apache Kafka Producer

This module covers building the continuous data ingestion engine for our real-time weather streaming stack. We create `src/producer.py` to regularly fetch live weather metrics from the **OpenWeather API**, structure the payload into clean JSON records, and stream them into our **Apache Kafka** broker (`localhost:9092`).

---

## 1. Data Producer Architecture

```
+------------------------+
|  OpenWeather API       |
|  (JSON Weather Data)   |
+------------------------+
            |
            | HTTP GET Request (e.g., every 60s)
            v
+------------------------+
|   Python Producer      |
|  (`src/producer.py`)   |
|                        |
|  - Reads .env config   |
|  - Enriches payload    |
|  - Serializes JSON     |
+------------------------+
            |
            | Stream Event Messages
            v
+------------------------+
|     Apache Kafka       |
| Topic: `weather_data`  |
+------------------------+
```

---

## 2. Producer Logic & Flow

The producer script operates in an infinite execution loop:

1. **Loads Credentials:** Reads `OPENWEATHER_API_KEY`, target cities, and Kafka broker parameters from the `.env` file.
2. **Polls OpenWeather API:** Issues HTTP GET requests for each target city (e.g., Algiers, London, New York, Tokyo, Paris).
3. **Payload Normalization:** Extracts core metrics (temperature, pressure, humidity, wind speed, weather conditions) and attaches a UTC timestamp (`ingested_at`).
4. **Kafka Serialization:** Converts Python dictionary objects into UTF-8 encoded JSON strings.
5. **Event Dispatch:** Publishes the serialized messages to the Kafka topic (`weather_metrics`).
6. **Sleep Interval:** Pauses for a specified duration (`FETCH_INTERVAL_SECONDS`) before executing the next polling cycle.

---

## 3. Producer Implementation (`src/producer.py`)

Create or update the file at `src/producer.py`:

```python
import json
import logging
import os
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Load environment variables from .env file
load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "your_default_api_key")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "weather_metrics"

# List of major Algerian cities
CITIES = [
    "Algiers,DZ",
    "Oran,DZ",
    "Constantine,DZ",
    "Annaba,DZ",
    "Setif,DZ",
    "Ouargla,DZ",
    "Ghardaia,DZ",
    "Tamanrasset,DZ"
]

def create_kafka_producer(broker_address: str) -> KafkaProducer:
    """Initializes and returns a KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=broker_address,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

def fetch_weather(city: str, api_key: str) -> dict | None:
    """Fetches real-time weather metrics for a given city from OpenWeather API."""
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Extract relevant analytical metrics
        payload = {
            "city": data["name"],
            "country": data["sys"]["country"],
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"],
            "weather_description": data["weather"][0]["description"],
            "timestamp": int(time.time())
        }
        return payload
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch weather data for {city}: {e}")
        return None

def run_producer():
    """Main loop streaming weather events to Kafka continuously."""
    producer = create_kafka_producer(KAFKA_BROKER)
    logging.info(f"Starting Weather Data Producer for Algerian Cities. Streaming to topic: '{TOPIC_NAME}'")

    try:
        while True:
            for city in CITIES:
                record = fetch_weather(city, OPENWEATHER_API_KEY)
                if record:
                    producer.send(TOPIC_NAME, value=record)
                    logging.info(f"Published weather record for {record['city']}: {record['temperature']}°C")
                time.sleep(1)  # Rate limiting between API calls
            
            logging.info("Sleeping for 60 seconds before next polling cycle...")
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
```

---

## 4. Execution & Verification

### Step 1: Ensure API Key is Configured
Open `.env` in your project root and verify `OPENWEATHER_API_KEY` contains your real API key.

### Step 2: Run the Producer
In your PowerShell terminal (with the virtual environment activated):

```powershell
uv run python src/producer.py```

### Expected Console Output

```
2026-10-22 18:40:00,123 [INFO] Connecting to Kafka broker at localhost:9092...
2026-10-22 18:40:00,456 [INFO] Starting Weather Data Producer. Streaming to topic: 'weather_metrics'
2026-10-22 18:40:00,789 [INFO] Published weather record for Algiers: 22.5°C
2026-10-22 18:40:01,012 [INFO] Published weather record for London: 14.2°C
2026-10-22 18:40:01,234 [INFO] Published weather record for New York: 18.0°C
2026-10-22 18:40:01,456 [INFO] Published weather record for Tokyo: 19.8°C
2026-10-22 18:40:01,678 [INFO] Published weather record for Paris: 16.1°C
2026-10-22 18:40:01,679 [INFO] Sleeping for 60 seconds...
```

---

## 5. Summary Checklist

- [x] Python `requests` handles OpenWeather API GET calls.
- [x] Payloads are structured into standard JSON objects with normalized UTC timestamps.
- [x] `kafka-python-ng` serializes Python dictionaries to byte-encoded JSON messages.
- [x] `producer.py` streams real-time data directly into the Kafka topic `weather_metrics`.