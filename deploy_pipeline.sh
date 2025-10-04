#!/bin/bash
# deploy_pipeline.sh (updated)

set -e

echo "Deploying NYC Green Taxi Pipeline..."

# Create required directories
echo "Creating directories..."
docker exec namenode mkdir -p /root/airflow/dags/utils
docker exec namenode mkdir -p /root/airflow/dags/spark_jobs

# Install required Python packages in namenode
echo "Installing required packages..."
docker exec namenode pip install geohash2 || echo "Geohash2 installation failed in namenode, continuing..."
docker exec nodemanager pip install geohash2 || echo "Geohash2 installation failed in nodemanager, continuing..."

# Copy DAG files
echo "Copying DAG files..."
docker cp ./dags/main_pipeline.py namenode:/root/airflow/dags/
docker cp ./dags/utils/coordinates.py namenode:/root/airflow/dags/utils/
docker cp ./dags/utils/__init__.py namenode:/root/airflow/dags/utils/

# Copy Spark jobs
echo "Copying Spark jobs..."
docker cp ./dags/spark_jobs/__init__.py namenode:/root/airflow/dags/spark_jobs/
docker cp ./dags/spark_jobs/data_quality_transform.py namenode:/root/airflow/dags/spark_jobs/
docker cp ./dags/spark_jobs/apply_dimensional_model.py namenode:/root/airflow/dags/spark_jobs/

# Create HDFS directories
echo "Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/raw
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/transformed
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/staging
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/lookup

# Set permissions
echo "Setting permissions..."
docker exec namenode hdfs dfs -chmod -R 777 /data/green_taxi

# Create Hive database if not exists
echo "Creating Hive database..."
docker exec hive-server beeline -u jdbc:hive2://hive-server:10000 -e "CREATE DATABASE IF NOT EXISTS taxi_warehouse;"

# Test DAG parsing
echo "Testing DAG parsing..."
docker exec namenode python /root/airflow/dags/main_pipeline.py
if [ $? -eq 0 ]; then
    echo "✓ DAG parsed successfully"
else
    echo "✗ DAG parsing failed"
    exit 1
fi

echo ""
echo "Deployment complete!"
echo "=============================="
echo "Access points:"
echo "- Airflow UI: http://localhost:43000 (airflow/airflow)"
echo "- HDFS UI: http://localhost:49870"
echo "- Spark UI: http://localhost:48080"
echo "- YARN UI: http://localhost:48088"
echo "- Hue UI: http://localhost:48890"
echo ""
echo "To trigger the pipeline:"
echo "docker exec namenode airflow dags trigger green_taxi_monthly_pipeline --exec-date 2024-02-01"
