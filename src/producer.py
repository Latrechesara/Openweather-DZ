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
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "weather_metrics")

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