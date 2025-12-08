from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


# 1. Spark Session

spark = SparkSession.builder \
    .appName("ALS MovieLens Full") \
    .getOrCreate()

path = "/opt/spark/data/movielens/"


# 2. PostgreSQL connection config and load

pg_url = "jdbc:postgresql://postgres:5432/postgresdb"
pg_properties = {
    "user": "postgres",
    "password": "postgres12345",
    "driver": "org.postgresql.Driver"
}

#ratings = spark.read.jdbc(url=pg_url, table="ratings", properties=pg_properties)

ratings = spark.read.jdbc(
    url=pg_url,
    table="(SELECT * FROM ratings LIMIT 100000) AS ratings_limit",
    properties=pg_properties
)
ratings.show(5)
"""
movies = spark.read.jdbc(url=pg_url, table="movies", properties=pg_properties)
tags = spark.read.jdbc(url=pg_url, table="tags", properties=pg_properties)
links = spark.read.jdbc(url=pg_url, table="links", properties=pg_properties)
genome_tags = spark.read.jdbc(url=pg_url, table="genome_tags", properties=pg_properties)
genome_scores = spark.read.jdbc(url=pg_url, table="genome_scores", properties=pg_properties)
"""

# 3. data preparation for ALS
# ALS sues only ratings
als_df = ratings.select(
    col("userId").cast("integer"),
    col("movieId").cast("integer"),
    col("rating").cast("float")
)

# 4. Join ratings with movie titles for better interpretability
#ratings_with_titles = als_df.join(movies, on="movieId", how="left")

# 5. Split Train / Test
train, test = als_df.randomSplit([0.8, 0.2], seed=42)


# 6. Configure ALS model

als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    nonnegative=True,
    implicitPrefs=False,
    maxIter=3,
    regParam=0.1,
    rank=5,
    coldStartStrategy="drop"
)


# 7. Train model

model = als.fit(train)

# 8. Save the model
model.write().overwrite().save("/opt/spark-apps/als_movielens_full_model")

spark.stop()