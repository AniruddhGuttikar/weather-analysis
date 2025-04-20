# Weather Analysis System

A simple yet functional weather analysis system using Apache Spark Streaming, Kafka, and PostgreSQL.

## Project Description

This project demonstrates a complete data pipeline for weather data analysis:

1. **Data Collection**: Collects weather data from OpenWeatherMap API (with fallback to mock data)
2. **Stream Processing**: Uses Apache Spark Streaming to process the data in real-time
3. **Batch Processing**: Implements batch processing for comparison with streaming results
4. **Storage**: Stores both raw and processed data in PostgreSQL
5. **Messaging**: Uses Kafka for data distribution between components

## Architecture

```
Weather API → Producer → Kafka Topics → Spark Streaming → PostgreSQL
                                          ↓
                                     Spark Batch
```

The system utilizes three Kafka topics:

- `weather-raw`: Raw weather data from cities
- `weather-alerts`: Weather alerts for extreme conditions
- `air-quality`: Air quality data

## Prerequisites

- Docker and Docker Compose
- OpenWeatherMap API key (optional - will use mock data if not provided)

## Setup Instructions

1. Clone the repository:

   ```
   git clone <repository-url>
   cd weather-analysis
   ```

2. (Optional) Create a `.env` file with your OpenWeatherMap API key:

   ```
   OPENWEATHERMAP_API_KEY=your_api_key_here
   ```

3. Build and start the Docker containers:

   ```
   docker-compose up -d
   ```

4. docker-compose exec weather-app python -c "from src.utils import initialize_database; initialize_database()"

5. Start the weather producer:

   ```
   docker-compose exec weather-app python -m src.producer.weather_producer
   ```

6. Start the Spark streaming processor:

   ```
   docker-compose exec weather-app python -m src.consumer.spark_streaming
   ```

7. (In a separate terminal) Run the batch processing after some data has been collected:
   ```
   docker-compose exec weather-app python -m src.batch.spark_batch.py
   ```

## Components

### Weather Producer

Collects weather data from the OpenWeatherMap API for various cities and publishes it to Kafka topics.

### Spark Streaming Consumer

Processes weather data in real-time using sliding and tumbling windows:

- Calculates average temperature, humidity, pressure for specific time windows
- Detects extreme weather conditions
- Aggregates data by city and weather condition

### Spark Batch Processor

Processes the entire dataset from PostgreSQL for comparison with streaming results:

- City-based aggregations
- Hourly temperature analysis
- Temperature distribution
- Weather condition frequency analysis

### PostgreSQL Database

Stores all raw and processed data in structured tables.

## Streaming vs. Batch Comparison

The project demonstrates the following differences between streaming and batch processing:

1. **Processing Model**:

   - Streaming: Processes data in small chunks as it arrives using windowing operations
   - Batch: Processes the entire dataset at once

2. **Latency**:

   - Streaming: Low latency, results available shortly after data arrival
   - Batch: Higher latency, results only available after processing the entire dataset

3. **Accuracy**:

   - Streaming: May have approximate results due to windowing and watermarking
   - Batch: More accurate results as it has access to the complete dataset

4. **Resource Usage**:
   - Streaming: Constant but lower resource usage
   - Batch: High resource usage during processing, then idle

## Evaluation and Results

To evaluate the comparison between streaming and batch processing:

1. Run the streaming consumer for at least 30 minutes to collect sufficient data
2. Run the batch processor
3. Compare the results in the PostgreSQL tables:
   ```
   weather_aggregated vs city_aggregations_batch
   ```

The differences in results can be attributed to:

- Window size in streaming vs. complete dataset in batch
- Watermarking in streaming vs. no data exclusion in batch
- Late-arriving data handling differences

## Extending the Project

- Add a web dashboard to visualize results
- Implement machine learning models for weather prediction
- Expand to more cities or additional weather metrics
- Add anomaly detection for unusual weather patterns
