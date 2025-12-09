from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("ALS MovieLens Load & Predict") \
    .getOrCreate()
pg_url = "jdbc:postgresql://postgres:5432/postgresdb"
pg_properties = {
    "user": "postgres",
    "password": "postgres12345",
    "driver": "org.postgresql.Driver"
}

# Cargar ratings de prueba

ratings = spark.read.jdbc(
    url="jdbc:postgresql://postgres:5432/postgresdb",
    table="(SELECT * FROM ratings LIMIT 25000000) AS ratings_test",
    properties={"user": "postgres", "password": "postgres12345", "driver": "org.postgresql.Driver"}
)

#ratings = spark.read.jdbc(url=pg_url, table="ratings", properties=pg_properties)
als_df = ratings.select(
    "userId",
    "movieId",
    "rating"
).withColumn("userId", ratings["userId"].cast("integer")) \
 .withColumn("movieId", ratings["movieId"].cast("integer")) \
 .withColumn("rating", ratings["rating"].cast("float"))

# Cargar modelo
model = ALSModel.load("/opt/spark-apps/als_movielens_full_model")

# Hacer join solo con los factores (renombrando las columnas internas para evitar ambigüedad)
users = model.userFactors.withColumnRenamed("id", "userId_model")
items = model.itemFactors.withColumnRenamed("id", "movieId_model")

# Hacer join con los ratings de prueba
ratings_mapped = als_df.join(users, als_df.userId == users.userId_model, "inner") \
                       .join(items, als_df.movieId == items.movieId_model, "inner") \
                       .select(
                           als_df.userId.alias("userId"),
                           als_df.movieId.alias("movieId"),
                           als_df.rating,
                       )

# Hacer predicciones
predictions = model.transform(ratings_mapped)

# Mostrar predicciones
predictions.show(10)



evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse}")

predictions.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgresdb") \
    .option("dbtable", "als_predictions") \
    .option("user", "postgres") \
    .option("password", "postgres12345") \
    .mode("overwrite") \
    .save()


spark.stop()