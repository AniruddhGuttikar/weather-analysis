import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Easy config to switch later
USE_API = False  # Set this to True later to switch to real API

TOPIC_MAIN = "weather-data"
TOPIC_HUMIDITY = "weather-humidity"
TOPIC_ALERTS = "weather-alerts"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ['Bangalore', 'Delhi', 'Mumbai', 'Chennai', 'Kolkata']

def generate_random_weather():
    return {
        "city": random.choice(cities),
        "temperature": round(random.uniform(15, 45), 2),
        "humidity": round(random.uniform(10, 100), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def get_api_weather():
    # Placeholder for future API integration
    # This can be implemented later with `requests.get(...)`
    return generate_random_weather()

def run_producer():
    print("🌤️ Weather producer started...\n")
    while True:
        weather_data = get_api_weather() if USE_API else generate_random_weather()

        # Send to main topic
        producer.send(TOPIC_MAIN, value=weather_data)

        # Send to humidity topic
        producer.send(TOPIC_HUMIDITY, value={
            "city": weather_data["city"],
            "humidity": weather_data["humidity"],
            "timestamp": weather_data["timestamp"]
        })

        # Optional: Alert for high temperature
        if weather_data["temperature"] > 40:
            producer.send(TOPIC_ALERTS, value={
                "city": weather_data["city"],
                "temperature": weather_data["temperature"],
                "alert": "🔥 High Temperature Alert!",
                "timestamp": weather_data["timestamp"]
            })

        print(f"[{weather_data['timestamp']}] Sent weather for {weather_data['city']} → {weather_data['temperature']}°C")
        time.sleep(2)  # Adjust to simulate higher/lower frequency

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n🛑 Weather producer stopped.")
