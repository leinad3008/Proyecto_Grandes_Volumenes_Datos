#!/bin/bash
# Run Spark ML models
# Usage: ./run_spark_model.sh <model_type>
# Example: ./run_spark_model.sh clustering

if [ $# -eq 0 ]; then
    echo "Usage: ./run_spark_model.sh <train/prediction>"
    echo ""
    exit 1
fi

MODEL_TYPE=$1

case $MODEL_TYPE in
    train)
        SCRIPT="als_movielens_train.py"
        ;;
    prediction)
        SCRIPT="movielens_predict.py"
        ;;
    *)
        echo "Unknown model type: $MODEL_TYPE"
        exit 1
        ;;
esac

echo "Running $MODEL_TYPE model..."
docker exec -u 0 \
    spark-master /opt/spark/bin/spark-submit \
    --executor-memory 4g \
    --driver-memory 4g \
    --total-executor-cores 2 \
    /opt/spark-apps/$SCRIPT \
    | tee /tmp/spark.log

echo "Done!"