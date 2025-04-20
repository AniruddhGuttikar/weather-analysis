import json
import time
import logging
import requests
import random
from datetime import datetime
from kafka import KafkaProducer
from typing import List, Dict, Any

from src.config import (
    OPENWEATHERMAP_API_KEY,
    WEATHER_API_ENDPOINT,
    KAFKA_BOOTSTRAP_SERVERS,
    WEATHER_RAW_TOPIC,
    WEATHER_ALERTS_TOPIC,
    AIR_QUALITY_TOPIC,
    CITIES
)
from src.utils import get_timestamp, kelvin_to_celsius

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherProducer:
    def __init__(self):
        """Initialize the Kafka producer and set up the topics."""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10)
        )
        self.cities = CITIES
        logger.info(f"Weather producer initialized with {len(self.cities)} cities")
        logger.info(f"OPENWEATHERMAP_API_KEY: {OPENWEATHERMAP_API_KEY}")

    def fetch_weather_data(self, city: str) -> Dict[str, Any]:
        """Fetch weather data for a given city from OpenWeatherMap API."""
        try:
            params = {
                'q': city,
                'appid': OPENWEATHERMAP_API_KEY,
                'units': 'standard'  # Kelvin
            }
            response = requests.get(WEATHER_API_ENDPOINT, params=params)
            if response.status_code == 200:
                data = response.json()
                
                # Extract relevant weather data
                weather_data = {
                    'city_id': data['id'],
                    'city_name': city,
                    'temperature': kelvin_to_celsius(data['main']['temp']),  # Convert to Celsius
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'wind_speed': data['wind']['speed'],
                    'weather_condition': data['weather'][0]['main'],
                    'timestamp': get_timestamp(),
                    'lat': data['coord']['lat'],
                    'lon': data['coord']['lon']
                }
                
                logger.info(f"Fetched weather data for {city}: {weather_data['temperature']:.1f}°C, {weather_data['weather_condition']}, {weather_data['humidity']}% humidity, {weather_data['pressure']} hPa, {weather_data['wind_speed']} m/s wind speed")
                return weather_data
            else:
                logger.error(f"Error fetching weather data for {city}: {response.status_code} - {response.text}")
                # Generate mock data if API call fails
                return self._generate_mock_data(city)
        except Exception as e:
            logger.error(f"Exception while fetching weather data for {city}: {e}")
            # Generate mock data if exception occurs
            return self._generate_mock_data(city)

    def _generate_mock_data(self, city: str) -> Dict[str, Any]:
        """Generate mock weather data when API is unavailable."""
        weather_conditions = ["Clear", "Clouds", "Rain", "Thunderstorm", "Drizzle", "Snow", "Mist"]
        
        mock_data = {
            'city_id': hash(city) % 100000,  # Generate a pseudo-unique ID
            'city_name': city,
            'temperature': random.uniform(0, 30),  # Random temperature between 0-30°C
            'humidity': random.uniform(30, 90),  # Random humidity between 30-90%
            'pressure': random.uniform(980, 1040),  # Random pressure
            'wind_speed': random.uniform(0, 20),  # Random wind speed
            'weather_condition': random.choice(weather_conditions),
            'timestamp': get_timestamp()
        }
        
        logger.info(f"Generated mock weather data for {city}: {mock_data['temperature']:.1f}°C, {mock_data['weather_condition']}")
        return mock_data

    def check_for_extreme_weather(self, weather_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for extreme weather conditions and generate alerts if necessary."""
        alerts = []
        
        # Check for extreme temperatures
        if weather_data['temperature'] > 35:
            alerts.append({
                'city_name': weather_data['city_name'],
                'alert_type': 'extreme_heat',
                'alert_message': f"Extreme heat detected in {weather_data['city_name']} with temperature {weather_data['temperature']:.1f}°C",
                'temperature': weather_data['temperature'],
                'timestamp': get_timestamp()
            })
        
        elif weather_data['temperature'] < -10:
            alerts.append({
                'city_name': weather_data['city_name'],
                'alert_type': 'extreme_cold',
                'alert_message': f"Extreme cold detected in {weather_data['city_name']} with temperature {weather_data['temperature']:.1f}°C",
                'temperature': weather_data['temperature'],
                'timestamp': get_timestamp()
            })
        return alerts
    
    def fetch_air_quality_data(self, city, lat: float, lon: float) -> Dict[str, Any]:
        """Fetch air quality data for given coordinates from OpenWeatherMap."""
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': OPENWEATHERMAP_API_KEY
            }
            response = requests.get("http://api.openweathermap.org/data/2.5/air_pollution", params=params)
            if response.status_code == 200:
                data = response.json()
                aqi_data = data['list'][0]
                logger.info(f"Fetched air quality data for {city}: AQI {aqi_data['main']['aqi']}, Components: {aqi_data['components']}")
                return {
                    'city_name': city,
                    'aqi': aqi_data['main']['aqi'],
                    'pm2_5': aqi_data['components']['pm2_5'],
                    'pm10': aqi_data['components']['pm10'],
                    'no2': aqi_data['components']['no2'],
                    'no': aqi_data['components']['no'],
                    'so2': aqi_data['components']['so2'],
                    'co': aqi_data['components']['co'],
                    'o3': aqi_data['components']['o3'],
                    'nh3': aqi_data['components']['nh3'],
                    'timestamp': get_timestamp()
                }
            else:
                logger.error(f"Failed to fetch air quality: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Exception while fetching air quality data: {e}")
        
        # Return mock/fallback data
        return {
            'aqi': random.randint(1, 5),
            'components': {
                'co': round(random.uniform(0.1, 2.0), 2),
                'pm2_5': round(random.uniform(5, 60), 2),
                'pm10': round(random.uniform(10, 100), 2)
            },
            'timestamp': get_timestamp()
        }




    def publish_weather_data(self):
        """Fetch and publish weather data for all cities."""
        for city in self.cities:
            # Fetch weather data
            weather_data = self.fetch_weather_data(city)
            
            # Publish to raw weather topic
            self.producer.send(WEATHER_RAW_TOPIC, value=weather_data)
            
            lat = weather_data['lat']
            lon = weather_data['lon']
            
            # Fetch air quality data
            air_quality_data = self.fetch_air_quality_data(city, lat, lon)
            self.producer.send(AIR_QUALITY_TOPIC, value=air_quality_data)

            # Check for extreme weather and publish alerts
            alerts = self.check_for_extreme_weather(weather_data)
            for alert in alerts:
                self.producer.send(WEATHER_ALERTS_TOPIC, value=alert)
                logger.warning(f"Weather alert: {alert['alert_message']}")
        
        # Ensure all messages are sent
        self.producer.flush()

    def run(self, interval: int = 30):
        """Run the producer to continuously fetch and publish weather data."""
        try:
            while True:
                logger.info(f"Fetching weather data for {len(self.cities)} cities...")
                self.publish_weather_data()
                logger.info(f"Sleeping for {interval} seconds...")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Error in producer: {e}")
        finally:
            self.producer.close()
            logger.info("Producer closed")


if __name__ == "__main__":
    producer = WeatherProducer()
    producer.run()