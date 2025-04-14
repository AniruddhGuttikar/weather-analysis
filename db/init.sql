-- -- Create a table to store streaming weather data
-- CREATE TABLE IF NOT EXISTS streaming_weather (
--     id SERIAL PRIMARY KEY,
--     city VARCHAR(100),
--     temperature DOUBLE PRECISION,
--     humidity DOUBLE PRECISION,
--     timestamp TIMESTAMPTZ,
--     window_start TIMESTAMPTZ,
--     window_end TIMESTAMPTZ
-- );

-- -- Create an index on the city and window_start for faster queries
-- CREATE INDEX IF NOT EXISTS idx_city_timestamp ON streaming_weather (city, window_start);




-- Create the weather_data table for streaming data
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    timestamp TIMESTAMPTZ DEFAULT current_timestamp, -- Timestamp for data entry
    window_start TIMESTAMPTZ,  -- Start of the window for stream
    window_end TIMESTAMPTZ,    -- End of the window for stream
    UNIQUE (city, window_start)  -- Prevent duplicates for a city in the same window
);

-- Create indexes for batch processing and streaming analysis
-- Index on city and timestamp for efficient querying
CREATE INDEX IF NOT EXISTS idx_city_timestamp ON weather_data (city, timestamp);
CREATE INDEX IF NOT EXISTS idx_window_start ON weather_data (window_start);
CREATE INDEX IF NOT EXISTS idx_window_end ON weather_data (window_end);

-- (Optional) Partition the table by day to improve performance on large datasets
-- This helps with batch processing over large time ranges by making queries faster
CREATE TABLE IF NOT EXISTS weather_data_partitioned (
    LIKE weather_data INCLUDING ALL
) PARTITION BY RANGE (window_start);

-- Create partitions for the weather_data table for each day (example)
-- You can create partitions dynamically based on your data ingestion
CREATE TABLE IF NOT EXISTS weather_data_2025_04_01 PARTITION OF weather_data_partitioned
    FOR VALUES FROM ('2025-04-01 00:00:00') TO ('2025-04-02 00:00:00');
CREATE TABLE IF NOT EXISTS weather_data_2025_04_02 PARTITION OF weather_data_partitioned
    FOR VALUES FROM ('2025-04-02 00:00:00') TO ('2025-04-03 00:00:00');

-- Add more partitions as per the range you need, or use dynamic scripts to automate partition creation

-- If you want to manage this process dynamically, consider using a script or a scheduling tool (cron) to add partitions for future dates.
