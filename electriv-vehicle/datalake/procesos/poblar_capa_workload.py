#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ==========================================================
# 1. Argumentos 
# ==========================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description="Carga Workload Electric Vehicle")
    parser.add_argument('--env', default='DEV')
    parser.add_argument('--username', default='hadoop')
    parser.add_argument('--base_path', default='/user')
    parser.add_argument('--local_data_path', default='/user/hadoop/dataset')
    return parser.parse_args()

# ==========================================================
# 2. Spark Session con Hive
# ==========================================================

def create_spark():
    return SparkSession.builder \
        .appName("Workload_ElectricVehicle") \
        .enableHiveSupport() \
        .getOrCreate()

# ==========================================================
# 3. Esquema (tu CREATE TABLE convertido)
# ==========================================================

SCHEMA_ELECTRIC_VEHICLE = StructType([
    StructField("VIN", StringType(), True),
    StructField("COUNTY", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("POSTAL_CODE", StringType(), True),
    StructField("MODEL_YEAR", IntegerType(), True),
    StructField("MAKE", StringType(), True),
    StructField("MODEL", StringType(), True),
    StructField("ELECTRIC_VEHICLE_TYPE", StringType(), True),
    StructField("CAFV_ELIGIBILITY", StringType(), True),
    StructField("ELECTRIC_RANGE", IntegerType(), True),
    StructField("LEGISLATIVE_DISTRICT", IntegerType(), True),
    StructField("DOL_VEHICLE_ID", LongType(), True),
    StructField("VEHICLE_LOCATION", StringType(), True),
    StructField("ELECTRIC_UTILITY", StringType(), True),
    StructField("CENSUS_TRACT_2020", StringType(), True)
])

# ==========================================================
# 4. Crear database workload
# ==========================================================

def crear_database(spark, env, username, base_path):

    db_name = f"{env}_workload"
    location = f"{base_path}/{username}/datalake/{db_name}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'")

    print(f"✅ Database creada: {db_name}")
    return db_name

# ==========================================================
# 5. Crear tabla externa
# ==========================================================

def crear_tabla(spark, db_name, username, base_path):

    location = f"{base_path}/{username}/datalake/{db_name}/electric_vehicle"

    spark.sql(f"DROP TABLE IF EXISTS {db_name}.electric_vehicle")

    create_sql = f"""
    CREATE TABLE {db_name}.electric_vehicle(
        VIN STRING,
        COUNTY STRING,
        CITY STRING,
        STATE STRING,
        POSTAL_CODE STRING,
        MODEL_YEAR INT,
        MAKE STRING,
        MODEL STRING,
        ELECTRIC_VEHICLE_TYPE STRING,
        CAFV_ELIGIBILITY STRING,
        ELECTRIC_RANGE INT,
        LEGISLATIVE_DISTRICT INT,
        DOL_VEHICLE_ID BIGINT,
        VEHICLE_LOCATION STRING,
        ELECTRIC_UTILITY STRING,
        CENSUS_TRACT_2020 STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """

    spark.sql(create_sql)

    return location

# ==========================================================
# 6. Cargar CSV (equivalente LOAD DATA)
# ==========================================================

def cargar_datos(spark, args, db_name):

    ruta = f"{args.local_data_path}/Electric_Vehicle.csv"

    print(f"📥 Leyendo archivo: {ruta}")

    df = spark.read.csv(
        ruta,
        schema=SCHEMA_ELECTRIC_VEHICLE,
        header=True,
        sep=","
    )

    df.write.mode("overwrite").insertInto(f"{db_name}.electric_vehicle")

    print("✅ Datos cargados")

# ==========================================================
# 7. MAIN
# ==========================================================

def main():

    args = parse_arguments()
    spark = create_spark()

    try:
        db = crear_database(spark, args.env, args.username, args.base_path)

        crear_tabla(spark, db, args.username, args.base_path)

        cargar_datos(spark, args, db)

        print("🔍 Validación:")
        spark.sql(f"SELECT * FROM {db}.electric_vehicle LIMIT 10").show(truncate=False)

        print("🎉 Workload completado")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()