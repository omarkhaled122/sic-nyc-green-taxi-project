#!/bin/bash
# test_complete_pipeline.sh

echo "Testing Complete NYC Green Taxi Pipeline"
echo "========================================"

# Test HDFS
echo "1. Testing HDFS..."
docker exec namenode hdfs dfs -ls / > /dev/null 2>&1
[ $? -eq 0 ] && echo "✓ HDFS is accessible" || echo "✗ HDFS not accessible"

# Test Hive
echo "2. Testing Hive..."
docker exec hive-server beeline -u jdbc:hive2://hive-server:10000 -e "SHOW DATABASES;" > /dev/null 2>&1
[ $? -eq 0 ] && echo "✓ Hive is accessible" || echo "✗ Hive not accessible"

# Test Spark
echo "3. Testing Spark..."
docker exec namenode spark-submit --version > /dev/null 2>&1
[ $? -eq 0 ] && echo "✓ Spark is configured" || echo "✗ Spark not configured"

# Test Python packages
echo "4. Testing Python packages..."
docker exec namenode python -c "import pandas, numpy, requests; print('✓ Required packages available')" 2>/dev/null || echo "✗ Missing packages"

# Test geohash (optional)
echo "5. Testing geohash (optional)..."
docker exec namenode python -c "import geohash; print('✓ Geohash available in namenode')" 2>/dev/null || echo "! Geohash not available (will use fallback)"
docker exec nodemanager python -c "import geohash; print('✓ Geohash available in nodemanager')" 2>/dev/null || echo "! Geohash not available (will use fallback)"

echo ""
echo "Test complete!"
