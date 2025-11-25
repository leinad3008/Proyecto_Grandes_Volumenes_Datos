#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month

def parse_args():
    parser = argparse.ArgumentParser(
        description="Spark transform job for Yellow Taxi data"
    )
    parser.add_argument("input_path", help="Input Parquet path (e.g. s3a://.../raw)")
    parser.add_argument("output_path", help="Output base path (e.g. s3a://.../processed)")
    parser.add_argument("file_name", help="File name (e.g. yellow_tripdata_2021-01.parquet)")
    parser.add_argument("year", type=int, help="Pickup year (e.g. 2021)")
    parser.add_argument("month", type=int, help="Pickup month as number (1-12)")
    args = parser.parse_args()

    return args


def main():
    args = parse_args()

    input_path = args.input_path
    output_path = args.output_path
    file_name = args.file_name
    year_arg = args.year
    month_arg = args.month

    spark: SparkSession = SparkSession.builder \
        .appName(f"Transform-YellowTaxi-{year_arg}-{month_arg}") \
        .getOrCreate()

    input_file = f"{input_path}/{year_arg}/{file_name}"
    df = spark.read.parquet(input_file)

    output_file = f"{output_path}/year={year_arg}/month={month_arg}/"

    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(output_file)

    spark.stop()


if __name__ == "__main__":
    main()
