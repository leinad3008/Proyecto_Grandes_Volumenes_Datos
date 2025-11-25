from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("REST_Airflow_Spark_Test").getOrCreate()

spark.sparkContext.setJobGroup("Load data", "Loading customer data")
df = spark.createDataFrame([("hello", 1), ("world", 2)], ["word", "count"])
spark.sparkContext.setLocalProperty("spark.job.description", "Validating data")
df.show()

spark.stop()
