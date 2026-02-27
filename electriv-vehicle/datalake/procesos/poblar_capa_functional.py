#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Proceso FUNCTIONAL - Vehicle Resumen Estatal
Conversión Hive SQL → PySpark
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# =====================================================
# 1. PARAMETROS
# =====================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description="Carga capa Functional")
    parser.add_argument('--env', type=str, default='DEV')
    parser.add_argument('--username', type=str, default='hadoop')
    parser.add_argument('--base_path', type=str, default='/user')
    parser.add_argument('--source_db', type=str, default='curated')
    return parser.parse_args()

# =====================================================
# 2. SPARK SESSION
# =====================================================

def create_spark():
    return SparkSession.builder \
        .appName("ProcesoFunctional-VehicleResumen") \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =====================================================
# 3. CREAR DATABASE
# =====================================================

def crear_database(spark, env, username, base_path):

    db_name = f"{env}_functional".lower()
    location = f"{base_path}/{username}/datalake/{env.upper()}_FUNCTIONAL"

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name}
        LOCATION '{location}'
    """)

    print(f"✅ Database creada: {db_name}")
    return db_name

# =====================================================
# 4. CREAR TABLA FUNCTIONAL
# =====================================================

def crear_tabla(spark, db, username, base_path, env):

    location = f"{base_path}/{username}/datalake/{env.upper()}_FUNCTIONAL/vehicle_resumen_estatal"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.VEHICLE_RESUMEN_ESTATAL(
            STATE STRING,
            ELECTRIC_VEHICLE_TYPE STRING,
            TOTAL_VEHICLES INT,
            PROMEDIO_ELECTRIC_RANGE DOUBLE
        )
        PARTITIONED BY (MODEL_YEAR INT)
        STORED AS PARQUET
        LOCATION '{location}'
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)

    print("✅ Tabla Functional creada")

# =====================================================
# 5. PROCESO FUNCTIONAL (GROUP BY)
# =====================================================

def procesar_functional(spark, db_functional, db_source):

    print("🔄 Leyendo datos desde CURATED...")

    df = spark.table(f"{db_source}.ELECTRIC_VEHICLE")

    # ===== AGREGACION (equivalente SQL) =====
    df_result = df.groupBy(
        "STATE",
        "ELECTRIC_VEHICLE_TYPE",
        "MODEL_YEAR"
    ).agg(
        count("*").cast("int").alias("TOTAL_VEHICLES"),
        avg("ELECTRIC_RANGE").alias("PROMEDIO_ELECTRIC_RANGE")
    )

    print(f"✅ Registros agregados: {df_result.count()}")

    # ===== INSERT OVERWRITE =====
    df_result.write \
        .mode("overwrite") \
        .insertInto(f"{db_functional}.VEHICLE_RESUMEN_ESTATAL")

    print("✅ Datos cargados en FUNCTIONAL")

# =====================================================
# 6. MAIN
# =====================================================

def main():

    args = parse_arguments()
    spark = create_spark()

    env_lower = args.env.lower()

    db_functional = crear_database(
        spark,
        env_lower,
        args.username,
        args.base_path
    )

    db_source = f"{env_lower}_{args.source_db}"

    crear_tabla(
        spark,
        db_functional,
        args.username,
        args.base_path,
        args.env
    )

    procesar_functional(
        spark,
        db_functional,
        db_source
    )

    print("\n🎉 FUNCTIONAL COMPLETADO")

    spark.sql(f"SHOW PARTITIONS {db_functional}.VEHICLE_RESUMEN_ESTATAL").show()

    spark.stop()

if __name__ == "__main__":
    main()