from pyspark.sql import SparkSession

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Export_Gold_To_MongoDB") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.write.database", "medallon") \
    .config("spark.mongodb.write.collection", "gold") \
    .getOrCreate()

# Ruta del CSV GOLD
ruta_csv = "hdfs:///user/hadoop/datalake/gold.csv"

# Leer CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta_csv)

print("===== ESQUEMA DE LOS DATOS =====")
df.printSchema()

print("===== PRIMEROS REGISTROS =====")
df.show(5)

# Guardar en MongoDB
df.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.write.connection.uri", "mongodb://localhost:27017") \
    .option("spark.mongodb.write.database", "medallon") \
    .option("spark.mongodb.write.collection", "gold") \
    .save()

print("✅ Datos exportados a MongoDB correctamente")

spark.stop()