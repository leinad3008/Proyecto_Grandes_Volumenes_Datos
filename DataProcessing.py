# Load dependencies
!pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs, col
import os

spark = (
    SparkSession.builder
    .appName("GOES Spark Pipeline")
    .getOrCreate()
)

spark

#Load parquetes in spark and merge
df_mag = spark.read.parquet("parquet/mag")
df_euv = spark.read.parquet("parquet/euv")