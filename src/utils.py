import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from src.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_timestamp():
    """Return current timestamp in ISO format."""
    return datetime.now().isoformat()

def get_postgres_connection():
    """Create and return a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

def initialize_database():
    """Create tables in PostgreSQL if they don't exist."""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Weather raw data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            id SERIAL PRIMARY KEY,
            city_id INTEGER NOT NULL,
            city_name VARCHAR(100) NOT NULL,
            temperature FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            pressure FLOAT NOT NULL,
            wind_speed FLOAT NOT NULL,
            weather_condition VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL
        )
        """)

        # Weather alerts table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts (
            id SERIAL PRIMARY KEY,
            city_name VARCHAR(100) NOT NULL,
            alert_type VARCHAR(50) NOT NULL,
            alert_message TEXT NOT NULL,
            temperature FLOAT,
            timestamp TIMESTAMP NOT NULL
        )
        """)

        # Air Quality table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS air_quality (
            id SERIAL PRIMARY KEY,
            city_name VARCHAR(100) NOT NULL,
            aqi FLOAT NOT NULL,
            pm2_5 FLOAT NOT NULL,
            pm10 FLOAT NOT NULL,
            no2 FLOAT NOT NULL,
            no FLOAT NOT NULL,
            so2 FLOAT NOT NULL,
            co FLOAT NOT NULL,
            o3 FLOAT NOT NULL,
            nh3 FLOAT NOT NULL,
            timestamp TIMESTAMP NOT NULL
        )
        """)
        
        # Weather Statistics table (for Operation 1)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_stats (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            city_name VARCHAR(100) NOT NULL,
            avg_temperature FLOAT NOT NULL,
            max_temperature FLOAT NOT NULL,
            min_temperature FLOAT NOT NULL,
            avg_humidity FLOAT NOT NULL,
            avg_pressure FLOAT NOT NULL,
            avg_wind_speed FLOAT NOT NULL,
            record_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Alert Counts table (for Operation 2)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS alert_counts (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            alert_type VARCHAR(50) NOT NULL,
            alert_count INTEGER NOT NULL,
            avg_temperature FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # AQI Summary table (for Operation 3)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS aqi_summary (
            id SERIAL PRIMARY KEY,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            city_name VARCHAR(100) NOT NULL,
            avg_aqi FLOAT NOT NULL,
            avg_pm2_5 FLOAT NOT NULL,
            avg_pm10 FLOAT NOT NULL,
            avg_o3 FLOAT NOT NULL,
            measurement_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        conn.commit()
        logger.info("Database tables created successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}")
    finally:
        cursor.close()
        conn.close()

def bulk_insert_weather_data(weather_data_list):
    """Insert multiple weather data records into the database."""
    if not weather_data_list:
        return
    
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        # Prepare data for bulk insert
        values = [(
            item['city_id'],
            item['city_name'],
            item['temperature'],
            item['humidity'],
            item['pressure'],
            item['wind_speed'],
            item['weather_condition'],
            item['timestamp']
        ) for item in weather_data_list]
        
        execute_values(
            cursor,
            """
            INSERT INTO weather_raw 
            (city_id, city_name, temperature, humidity, pressure, wind_speed, weather_condition, timestamp)
            VALUES %s
            """,
            values
        )
        
        conn.commit()
        logger.info(f"Inserted {len(weather_data_list)} weather records into database")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting weather data: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_weather_aggregation(aggregation_data):
    """Insert weather aggregation data into the database."""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            INSERT INTO weather_aggregated 
            (window_start, window_end, city_name, avg_temperature, min_temperature, max_temperature, 
             avg_humidity, avg_pressure, avg_wind_speed, common_condition, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                aggregation_data['window_start'],
                aggregation_data['window_end'],
                aggregation_data['city_name'],
                aggregation_data['avg_temperature'],
                aggregation_data['min_temperature'],
                aggregation_data['max_temperature'],
                aggregation_data['avg_humidity'],
                aggregation_data['avg_pressure'],
                aggregation_data['avg_wind_speed'],
                aggregation_data['common_condition'],
                datetime.now()
            )
        )
        
        conn.commit()
        logger.info(f"Inserted aggregation data for {aggregation_data['city_name']}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting aggregation data: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_weather_alert(alert_data):
    """Insert weather alert into the database."""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            INSERT INTO weather_alerts 
            (city_name, alert_type, alert_message, temperature, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                alert_data['city_name'],
                alert_data['alert_type'],
                alert_data['alert_message'],
                alert_data.get('temperature'),
                alert_data['timestamp']
            )
        )
        
        conn.commit()
        logger.info(f"Inserted alert for {alert_data['city_name']}: {alert_data['alert_type']}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting alert data: {e}")
    finally:
        cursor.close()
        conn.close()

def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit."""
    return (celsius * 9/5) + 32

def kelvin_to_celsius(kelvin):
    """Convert Kelvin to Celsius."""
    return kelvin - 273.15