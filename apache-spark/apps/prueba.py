from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel

# -----------------------------
# Inicializar Spark
# -----------------------------
spark = SparkSession.builder \
    .appName("Inspeccion Modelo ALS") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# -----------------------------
# Cargar modelo ALS
# -----------------------------
model_path = "/opt/spark-apps/als_movielens_full"
model = ALSModel.load(model_path)

# -----------------------------
# Verificar cantidad de usuarios y películas en el modelo
# -----------------------------
num_users = model.userFactors.count()
num_items = model.itemFactors.count()
print(f"Usuarios en el modelo: {num_users}")
print(f"Películas en el modelo: {num_items}")

# Mostrar algunos IDs de usuarios y películas
print("Ejemplos de userId en el modelo:")
model.userFactors.select("id").show(10)

print("Ejemplos de movieId en el modelo:")
model.itemFactors.select("id").show(10)

# -----------------------------
# Opcional: inspeccionar predicciones sobre un subconjunto de ratings
# -----------------------------
# Si quieres probar con ratings existentes para ver cuántas predicciones se generan
# ratings_df = spark.read.option("header", "true").csv("/opt/spark-apps/ratings_sample.csv")
# ratings_df = ratings_df.selectExpr("cast(userId as int) userId", "cast(movieId as int) movieId", "cast(rating as fl


"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import explode, col

# -------------------------------
# 1️⃣ Crear SparkSession
# -------------------------------
spark = SparkSession.builder \
    .appName("ALS MovieLens Top-N Predictions") \
    .getOrCreate()

# -------------------------------
# 2️⃣ Conexión PostgreSQL
# -------------------------------
pg_url = "jdbc:postgresql://postgres:5432/postgresdb"
pg_properties = {
    "user": "postgres",
    "password": "postgres12345",
    "driver": "org.postgresql.Driver"
}

# -------------------------------
# 3️⃣ Cargar modelo ALS
# -------------------------------
model = ALSModel.load("/opt/spark-apps/als_movielens_full_model")

# -------------------------------
# 4️⃣ Generar top-N predicciones por usuario
# -------------------------------
# Obtener las 10 mejores recomendaciones por usuario
top_recs = model.recommendForAllUsers(10)

# Flatten para guardar
from pyspark.sql.functions import explode, col
top_recs_flat = top_recs.select(
    col("userId"),
    explode(col("recommendations")).alias("rec")
).select(
    col("userId"),
    col("rec.movieId"),
    col("rec.rating").alias("prediction")
)

# Mostrar algunas predicciones
top_recs_flat.show(10)

# -------------------------------
# 6️⃣ Guardar en PostgreSQL
# -------------------------------
top_recs_flat.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "als_top_predictions") \
    .option("user", pg_properties["user"]) \
    .option("password", pg_properties["password"]) \
    .mode("overwrite") \
    .save()

# -------------------------------
# 7️⃣ Cerrar Spark
# -------------------------------
spark.stop()
"""