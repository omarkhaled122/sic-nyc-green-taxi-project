-- dags/spark_jobs/create_hive_tables.sql
-- Create database and dimension/fact tables for NYC Green Taxi data warehouse

CREATE DATABASE IF NOT EXISTS taxi_warehouse;
USE taxi_warehouse;

-- Dimension: Time
CREATE TABLE IF NOT EXISTS DimTime (
    time_id STRING,
    full_date TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    second INT,
    weekday INT,
    quarter INT
)
STORED AS PARQUET;

-- Dimension: Location
CREATE TABLE IF NOT EXISTS DimLocation (
    location_id INT,
    center_lat DOUBLE,
    center_lon DOUBLE,
    geo_hash STRING,
    borough STRING
)
STORED AS PARQUET;

-- Dimension: Payment
CREATE TABLE IF NOT EXISTS DimPayment (
    payment_id INT,
    payment_type STRING
)
STORED AS PARQUET;

-- Fact: Trip
CREATE TABLE IF NOT EXISTS FactTrip (
    trip_id STRING,
    start_time_id STRING,  
    start_location_id INT,       
    end_location_id INT,         
    payment_id INT,         
    trip_duration_sec INT,
    trip_distance_km DOUBLE,
    total_amount DOUBLE
)
PARTITIONED BY (year_month STRING)
STORED AS PARQUET;

-- Staging table for transformed data
CREATE TABLE IF NOT EXISTS staging_rides_geo (
    trip_id STRING,
    start_time TIMESTAMP,
    pu_location_id INT,
    do_location_id INT,
    payment_id INT,
    start_geo_hash STRING,
    end_geo_hash STRING,
    trip_duration_sec INT,
    trip_distance_km DOUBLE,
    total_amount DOUBLE
)
PARTITIONED BY (year_month STRING)
STORED AS PARQUET;
