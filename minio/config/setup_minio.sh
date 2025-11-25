#!/bin/bash
set -e

sleep 4 # Wait for MinIO to be fully up

### ===========================================================
### CONFIG
### ===========================================================

MINIO_HOST="http://minio:9000"
ROOT_USER="admin"
ROOT_PASS="admin12345"

# Users for your ETL
AIRFLOW_USER="airflow"
AIRFLOW_PASS="airflow12345"

SPARK_USER="spark"
SPARK_PASS="spark12345"

### Buckets
RAW_BUCKET_MAG="mag-raw"
RAW_BUCKET_EUV="euv-raw"
PROCESSED_BUCKET_MAG="mag-processed"
PROCESSED_BUCKET_EUV="euv-processed"

### ===========================================================
echo "Configuring MinIO…"
### ===========================================================

mc alias set local $MINIO_HOST $ROOT_USER $ROOT_PASS >/dev/null

### ===========================================================
### CHECK IF ALREADY CONFIGURED
### ===========================================================

if mc ls local/$RAW_BUCKET_MAG >/dev/null 2>&1; then
    echo "MinIO is already configured. Exiting..."
    exit 0
fi

### ===========================================================
echo "Creating buckets…"
### ===========================================================

mc mb --ignore-existing local/$RAW_BUCKET_MAG
mc mb --ignore-existing local/$RAW_BUCKET_EUV
mc mb --ignore-existing local/$PROCESSED_BUCKET_MAG
mc mb --ignore-existing local/$PROCESSED_BUCKET_EUV

### ===========================================================
echo "Creating policies…"
### ===========================================================

mc admin policy create local airflow-policy /tmp/config/airflow-policy.json
mc admin policy create local spark-policy /tmp/config/spark-policy.json

### ===========================================================
echo "Creating users…"
### ===========================================================

mc admin user add local $AIRFLOW_USER $AIRFLOW_PASS || true
mc admin user add local $SPARK_USER $SPARK_PASS || true

### ===========================================================
echo "Assiging policies to users…"
### ===========================================================

mc admin policy attach local airflow-policy --user $AIRFLOW_USER
mc admin policy attach local spark-policy --user $SPARK_USER

### ===========================================================
echo "Configuration complete"
echo "Buckets:"
echo "  - $RAW_BUCKET_MAG"
echo "  - $RAW_BUCKET_EUV"
echo "  - $PROCESSED_BUCKET_MAG"
echo "  - $PROCESSED_BUCKET_EUV"

echo "Users:"
echo "  - Airflow → $AIRFLOW_USER / $AIRFLOW_PASS"
echo "  - Spark → $SPARK_USER / $SPARK_PASS"
### ===========================================================
