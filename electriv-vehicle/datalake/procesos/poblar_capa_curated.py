#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Proceso CURATED - Electric Vehicle
Conversión desde Hive SQL hacia PySpark (estilo docente)
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import *

# =====================================================
# 1. PARAMETROS
# =====================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description="Carga capa Curated")
    parser.add_argument('--env', type=str, default='TopicosB')
    parser.add_argument('--username', type=str, default='hadoop')
    parser.add_argument('--base_path', type=str, default='/user')
    parser.add_argument('--source_db', type=str, default='landing')
    return parser.parse_args()

# =====================================================
# 2. SPARK SESSION (MISMO ESTILO DOCENTE)
# =====================================================

def create_spark():
    return SparkSession.builder \
        .appName("ProcesoCurated-ElectricVehicle") \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

# =====================================================
# 3. CREAR DATABASE
# =====================================================

def crear_database(spark, env, username, base_path):

    db_name = f"{env}_curated".lower()
    location = f"{base_path}/{username}/datalake/{env.upper()}_CURATED"

    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name}
        LOCATION '{location}'
    """)

    print(f"✅ Database creada: {db_name}")
    return db_name

# =====================================================
# 4. CREAR TABLA CURATED
# =====================================================

def crear_tabla(spark, db, username, base_path, env):

    location = f"{base_path}/{username}/datalake/{env.upper()}_CURATED/ELECTRIC_VEHICLE"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.ELECTRIC_VEHICLE(
            VIN STRING,
            COUNTY STRING,
            CITY STRING,
            STATE STRING,
            POSTAL_CODE STRING,
            MAKE STRING,
            MODEL STRING,
            ELECTRIC_VEHICLE_TYPE STRING,
            CAFV_ELIGIBILITY STRING,
            ELECTRIC_RANGE INT,
            LEGISLATIVE_DISTRICT STRING,
            DOL_VEHICLE_ID STRING,
            VEHICLE_LOCATION STRING,
            ELECTRIC_UTILITY STRING,
            CENSUS_TRACT_2020 STRING
        )
        PARTITIONED BY (MODEL_YEAR INT)
        STORED AS PARQUET
        LOCATION '{location}'
        TBLPROPERTIES ('parquet.compression'='SNAPPY')
    """)

    print("✅ Tabla Curated creada")

# =====================================================
# 5. PROCESO CURATED (TU INSERT OVERWRITE)
# =====================================================

def procesar_curated(spark, db_curated, db_source):

    print("🔄 Leyendo datos desde Landing...")

    df = spark.table(f"{db_source}.ELECTRIC_VEHICLE")

    # ===== LIMPIEZA (tu WHERE + CAST) =====
    df_clean = df.select(
        trim(col("VIN")).alias("VIN"),
        upper(trim(col("COUNTY"))).alias("COUNTY"),
        upper(trim(col("CITY"))).alias("CITY"),
        upper(trim(col("STATE"))).alias("STATE"),
        col("POSTAL_CODE").cast("string"),
        upper(trim(col("MAKE"))).alias("MAKE"),
        upper(trim(col("MODEL"))).alias("MODEL"),
        col("ELECTRIC_VEHICLE_TYPE").cast("string"),
        col("CAFV_ELIGIBILITY").cast("string"),
        col("ELECTRIC_RANGE").cast("int"),
        col("LEGISLATIVE_DISTRICT").cast("string"),
        col("DOL_VEHICLE_ID").cast("string"),
        col("VEHICLE_LOCATION").cast("string"),
        col("ELECTRIC_UTILITY").cast("string"),
        col("CENSUS_TRACT_2020").cast("string"),
        col("MODEL_YEAR").cast("int")
    ).filter(
        col("VIN").isNotNull() &
        col("MODEL_YEAR").isNotNull() &
        (col("MODEL_YEAR") > 2000) &
        (col("ELECTRIC_RANGE") >= 0)
    )

    print(f"✅ Registros después de calidad: {df_clean.count()}")

    # ===== INSERT OVERWRITE equivalente =====
    df_clean.write \
    .mode("overwrite") \
    .insertInto(f"{db_curated}.ELECTRIC_VEHICLE", overwrite=True)

    print("✅ Datos cargados en CURATED")

# =====================================================
# 6. MAIN
# =====================================================

def main():

    args = parse_arguments()
    spark = create_spark()

    env_lower = args.env.lower()

    db_curated = crear_database(
        spark,
        env_lower,
        args.username,
        args.base_path
    )

    db_source = f"{env_lower}_{args.source_db}"

    crear_tabla(
        spark,
        db_curated,
        args.username,
        args.base_path,
        args.env
    )

    procesar_curated(
        spark,
        db_curated,
        db_source
    )

    print("\n🎉 CURATED COMPLETADO")
    spark.sql(f"SHOW TABLES IN {db_curated}").show()

    spark.stop()

if __name__ == "__main__":
    main()