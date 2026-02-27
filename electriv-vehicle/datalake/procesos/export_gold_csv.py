from pyspark.sql import SparkSession

# =====================================================
# 1. Crear sesión Spark con soporte Hive
# =====================================================

spark = SparkSession.builder \
    .appName("Export-Functional-Vehicle-To-CSV") \
    .enableHiveSupport() \
    .getOrCreate()

# =====================================================
# 2. Base de datos y tabla (TU PROYECTO)
# =====================================================

database = "dev_functional"
table = "vehicle_resumen_estatal"

print(f"Leyendo tabla: {database}.{table}")

# Leer tabla desde Hive Metastore
df = spark.table(f"{database}.{table}")

# =====================================================
# 3. Ruta de salida (LOCAL - WSL)
# =====================================================

output_path = "file:/mnt/c/Users/fchav/Documents/Topicos/electriv-vehicle/datalake/temp/export_vehicle_csv"

# =====================================================
# 4. Exportar a CSV
# =====================================================

(
    df.coalesce(1)  # genera 1 solo archivo CSV
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(output_path)
)

print("✅ Exportación completada correctamente")

# =====================================================
# 5. Cerrar Spark
# =====================================================

spark.stop()