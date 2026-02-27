#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
from pyspark.sql import SparkSession

# =============================================================================
# 1. Parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Carga capa Landing')
    parser.add_argument('--env', default='DEV')
    parser.add_argument('--username', default='hadoop')
    parser.add_argument('--base_path', default='/user')
    parser.add_argument(
        '--schema_path',
        default='/user/hadoop/datalake/schema'
    )
    parser.add_argument(
        '--source_db',
        default='dev_workload'
    )
    return parser.parse_args()

# =============================================================================
# 2. Spark Session
# =============================================================================

def create_spark():
    return SparkSession.builder \
        .appName("Landing-ElectricVehicle") \
        .enableHiveSupport() \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

# =============================================================================
# 3. Crear database landing
# =============================================================================

def crear_database(spark, env, username, base_path):

    db_name = f"{env}_landing".lower()
    location = f"{base_path}/{username}/datalake/{env}_landing"

    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name}
        LOCATION '{location}'
    """)

    print(f"✅ Database creada: {db_name}")
    return db_name

# =============================================================================
# 4. Crear tabla AVRO
# =============================================================================

def crear_tabla(spark, db_name, args):

    location = f"{args.base_path}/{args.username}/datalake/{args.env}_landing/electric_vehicle"

    schema_url = f"hdfs://{args.schema_path}/{args.env}_landing/electric_vehicle.avsc"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.ELECTRIC_VEHICLE
        STORED AS AVRO
        LOCATION '{location}'
        TBLPROPERTIES(
            'avro.schema.url'='{schema_url}',
            'avro.output.codec'='snappy'
        )
    """)

    print("✅ Tabla ELECTRIC_VEHICLE creada")

# =============================================================================
# 5. Insertar datos desde WORKLOAD
# =============================================================================

def insertar_datos(spark, db_landing, db_workload):

    spark.sql(f"""
        INSERT OVERWRITE TABLE {db_landing}.ELECTRIC_VEHICLE
        SELECT
            VIN,
            COUNTY,
            CITY,
            STATE,
            POSTAL_CODE,
            MODEL_YEAR,
            MAKE,
            MODEL,
            ELECTRIC_VEHICLE_TYPE,
            CAFV_ELIGIBILITY,
            ELECTRIC_RANGE,
            LEGISLATIVE_DISTRICT,
            DOL_VEHICLE_ID,
            VEHICLE_LOCATION,
            ELECTRIC_UTILITY,
            CENSUS_TRACT_2020
        FROM {db_workload}.ELECTRIC_VEHICLE
    """)

    print("✅ Datos cargados a Landing")

# =============================================================================
# 6. MAIN
# =============================================================================

def main():

    args = parse_arguments()
    spark = create_spark()

    try:

        env = args.env.lower()

        db_landing = crear_database(
            spark,
            env,
            args.username,
            args.base_path
        )

        db_workload = f"{env}_workload"

        crear_tabla(spark, db_landing, args)

        insertar_datos(spark, db_landing, db_workload)

        print("\n📊 Validación:")
        spark.sql(
            f"SELECT * FROM {db_landing}.ELECTRIC_VEHICLE LIMIT 10"
        ).show(truncate=False)

        print("\n🎉 Landing completado correctamente")

    except Exception as e:
        print("❌ Error:", e)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()