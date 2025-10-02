#!/bin/bash
# test_deployment.sh

echo "Testing pipeline deployment..."

# Test Python imports
echo "Testing Python imports..."
docker exec namenode python -c "
try:
    import pandas
    import numpy
    import requests
    print('✓ Required packages available')
except ImportError as e:
    print('✗ Missing package:', e)
    exit(1)
"

# Test HDFS connectivity
echo "Testing HDFS..."
docker exec namenode hdfs dfs -ls / > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "HDFS accessible"
else
    echo "HDFS not accessible"
    exit 1
fi

echo "All tests passed! Ready to deploy."
