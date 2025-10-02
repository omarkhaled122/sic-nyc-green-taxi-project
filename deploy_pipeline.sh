#!/bin/bash
# deploy_pipeline.sh

set -e  # Exit on any error

echo "Deploying Green Taxi Pipeline to Airflow..."

# Create required directories
echo "Creating directories..."
docker exec namenode mkdir -p /root/airflow/dags/utils

# Copy DAG files
echo "Copying DAG files..."
docker cp ./dags/main_pipeline.py namenode:/root/airflow/dags/
docker cp ./dags/utils/coordinates.py namenode:/root/airflow/dags/utils/
docker cp ./dags/utils/__init__.py namenode:/root/airflow/dags/utils/

# Create HDFS directories
echo "Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/raw
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/transformed
docker exec namenode hdfs dfs -mkdir -p /data/green_taxi/lookup

# Set permissions
echo "Setting permissions..."
docker exec namenode hdfs dfs -chmod -R 777 /data/green_taxi

# Test DAG parsing
echo "Testing DAG parsing..."
docker exec namenode python /root/airflow/dags/main_pipeline.py
if [ $? -eq 0 ]; then
    echo "DAG parsed successfully"
else
    echo "DAG parsing failed"
    exit 1
fi

echo ""
echo "Deployment complete!"
echo "Access Airflow UI at http://localhost:43000"
echo "Default credentials: airflow/airflow"
echo ""
echo "To trigger the DAG manually:"
echo "docker exec namenode airflow dags trigger green_taxi_monthly_pipeline"
