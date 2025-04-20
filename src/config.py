import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# OpenWeatherMap API settings
OPENWEATHERMAP_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "YOUR_API_KEY")
WEATHER_API_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
WEATHER_RAW_TOPIC = "weather-raw"
WEATHER_PROCESSED_TOPIC = "weather-processed"
WEATHER_ALERTS_TOPIC = "weather-alerts"
AIR_QUALITY_TOPIC = "air-quality"

# Spark settings
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
CHECKPOINT_LOCATION = "/tmp/checkpoint"

# PostgreSQL settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Streaming window settings (in seconds)
WINDOW_SIZE = 60  # 15 minutes
SLIDING_INTERVAL = 30  # 5 minutes

# Database connection string
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Cities to monitor
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    "Fairbanks", "Iqaluit", "Barrow", "Reykjavik", "Tuktoyaktuk", 
    "Norilsk", "Miami", "Tampa", "Cozumel", "Galveston", "San Juan", "Baghdad",
    "Marrakech", "Chihuahua","Las Vegas", "Banjul", "Bangalore", "Delhi", "Goa"
]
