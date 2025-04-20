import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min, max, count, when, lit, 
    hour, date_format, mode, expr
)

from src.config import (
    SPARK_MASTER,
    POSTGRES_URL,
    POSTGRES_PROPERTIES
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkBatchProcessor:
    def __init__(self):
        """Initialize Spark session for batch processing."""
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("WeatherAnalysisBatch") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Batch Processor initialized")

    def load_from_postgres(self, table_name):
        """Load data from PostgreSQL table."""
        df = self.spark.read \
            .jdbc(
                url=POSTGRES_URL,
                table=table_name,
                properties=POSTGRES_PROPERTIES
            )
        
        logger.info(f"Loaded {df.count()} records from {table_name}")
        return df

    def process_raw_weather_data(self, df):
        """Process raw weather data in batch mode."""
        # Perform aggregations by city
        aggregated_df = df \
            .groupBy("city_name") \
            .agg(
                avg("temperature").alias("avg_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("pressure").alias("avg_pressure"),
                avg("wind_speed").alias("avg_wind_speed"),
                mode("weather_condition").alias("common_condition")
            )
        
        return aggregated_df

    def hourly_temperature_analysis(self, df):
        """Analyze temperature by hour of day."""
        hourly_df = df \
            .withColumn("hour_of_day", hour(col("timestamp"))) \
            .groupBy("city_name", "hour_of_day") \
            .agg(
                avg("temperature").alias("avg_temperature"),
                min("temperature").alias("min_temperature"),
                max("temperature").alias("max_temperature"),
                count("*").alias("record_count")
            ) \
            .orderBy("city_name", "hour_of_day")
        
        return hourly_df

    def temperature_distribution(self, df):
        """Create temperature distribution by categories."""
        temp_df = df \
            .withColumn("temp_category", 
                when(col("temperature") < 0, "freezing")
                .when(col("temperature") < 10, "cold")
                .when(col("temperature") < 20, "mild")
                .when(col("temperature") < 30, "warm")
                .otherwise("hot")
            ) \
            .groupBy("city_name", "temp_category") \
            .count() \
            .orderBy("city_name", "temp_category")
        
        return temp_df

    def weather_condition_frequency(self, df):
        """Analyze frequency of different weather conditions."""
        condition_df = df \
            .groupBy("city_name", "weather_condition") \
            .count() \
            .withColumn("percentage", 
                expr("count * 100.0 / sum(count) over (partition by city_name)")
            ) \
            .orderBy("city_name", col("count").desc())
        
        return condition_df

    def alert_summary(self, alerts_df):
        """Summarize weather alerts."""
        if alerts_df.count() == 0:
            return self.spark.createDataFrame([], "city_name STRING, alert_type STRING, alert_count INT")
            
        alert_summary_df = alerts_df \
            .groupBy("city_name", "alert_type") \
            .count() \
            .withColumnRenamed("count", "alert_count") \
            .orderBy("city_name", col("alert_count").desc())
        
        return alert_summary_df

    def save_to_postgres(self, df, table_name):
        """Save DataFrame to PostgreSQL."""
        df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=f"{table_name}_batch",
                mode="overwrite",
                properties=POSTGRES_PROPERTIES
            )
        
        logger.info(f"Saved {df.count()} records to {table_name}_batch")

    def run_batch_analysis(self):
        """Run batch analysis on the collected weather data."""
        try:
            start_time = time.time()
            
            # Load data from PostgreSQL
            raw_weather_df = self.load_from_postgres("weather_raw")
            alerts_df = self.load_from_postgres("weather_alerts")
            
            # Skip processing if no data
            if raw_weather_df.count() == 0:
                logger.warning("No weather data available for batch processing")
                return
            
            # Perform various analyses
            city_aggregations = self.process_raw_weather_data(raw_weather_df)
            hourly_analysis = self.hourly_temperature_analysis(raw_weather_df)
            temp_distribution = self.temperature_distribution(raw_weather_df)
            condition_freq = self.weather_condition_frequency(raw_weather_df)
            alert_summary = self.alert_summary(alerts_df)
            
            # Save results to PostgreSQL
            self.save_to_postgres(city_aggregations, "city_aggregations")
            self.save_to_postgres(hourly_analysis, "hourly_analysis")
            self.save_to_postgres(temp_distribution, "temp_distribution")
            self.save_to_postgres(condition_freq, "condition_frequency")
            self.save_to_postgres(alert_summary, "alert_summary")
            
            # Show sample results
            logger.info("Batch processing results:")
            logger.info("City Aggregations:")
            city_aggregations.show(10, truncate=False)
            
            logger.info("Temperature Distribution:")
            temp_distribution.show(10, truncate=False)
            
            # Print execution time
            end_time = time.time()
            logger.info(f"Batch processing completed in {end_time - start_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
        finally:
            logger.info("Stopping Spark session")
            self.spark.stop()


if __name__ == "__main__":
    processor = SparkBatchProcessor()
    processor.run_batch_analysis()