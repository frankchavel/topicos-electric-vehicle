from pyspark.sql import SparkSession

# Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export-Functional-To-CSV") \
    .enableHiveSupport() \
    .getOrCreate()

# Base de datos y tabla de tu proyecto
database = "topicosb_functional"
table = "vehicle_resumen_estatal"

# Leer tabla Hive
df = spark.table(f"{database}.{table}")

# Ruta dentro del proyecto (como el docente)
output_path = "file:/home/hadoop/topicos-electric-vehicle/datalake/temp"

# Guardar CSV
df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv(output_path)

print("Exportación completada")

spark.stop()