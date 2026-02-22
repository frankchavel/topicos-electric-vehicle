-- -------------------------------------------------------------------------------------------------------
-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f curate.sql --hiveconf "PARAM_USERNAME=hadoop" --hiveconf "ENV=DEV"
-- -------------------------------------------------------------------------------------------------------

-- -------------------------------------------------------------------------------------------------------
-- 1. Parámetros
-- -------------------------------------------------------------------------------------------------------

SET ENV=DEV;
SET PARAM_USERNAME=hadoop;

-- -------------------------------------------------------------------------------------------------------
-- FORZAR MODO LOCAL (SOLUCIONA TU ERROR)
-- -------------------------------------------------------------------------------------------------------

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=true;

-- -------------------------------------------------------------------------------------------------------
-- 2. Base de datos
-- -------------------------------------------------------------------------------------------------------

DROP DATABASE IF EXISTS ${hiveconf:ENV}_curated CASCADE;

CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_curated
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_curated';

-- -------------------------------------------------------------------------------------------------------
-- 3. Configuración
-- -------------------------------------------------------------------------------------------------------

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- -------------------------------------------------------------------------------------------------------
-- 4. Tabla Curated
-- -------------------------------------------------------------------------------------------------------

CREATE TABLE ${hiveconf:ENV}_curated.ELECTRIC_VEHICLE(
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
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_curated/electric_vehicle'
TBLPROPERTIES('parquet.compression'='SNAPPY');

-- -------------------------------------------------------------------------------------------------------
-- 5. Insert con limpieza
-- -------------------------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${hiveconf:ENV}_curated.ELECTRIC_VEHICLE
PARTITION (MODEL_YEAR)
SELECT
    CAST(T.VIN AS STRING),
    CAST(T.COUNTY AS STRING),
    CAST(T.CITY AS STRING),
    CAST(T.STATE AS STRING),
    CAST(T.POSTAL_CODE AS STRING),
    CAST(T.MAKE AS STRING),
    CAST(T.MODEL AS STRING),
    CAST(T.ELECTRIC_VEHICLE_TYPE AS STRING),
    CAST(T.CAFV_ELIGIBILITY AS STRING),
    CAST(T.ELECTRIC_RANGE AS INT),
    CAST(T.LEGISLATIVE_DISTRICT AS STRING),
    CAST(T.DOL_VEHICLE_ID AS STRING),
    CAST(T.VEHICLE_LOCATION AS STRING),
    CAST(T.ELECTRIC_UTILITY AS STRING),
    CAST(T.CENSUS_TRACT_2020 AS STRING),
    CAST(T.MODEL_YEAR AS INT)
FROM ${hiveconf:ENV}_landing.ELECTRIC_VEHICLE T
WHERE 
    T.VIN IS NOT NULL
    AND T.MODEL_YEAR IS NOT NULL
    AND CAST(T.MODEL_YEAR AS INT) > 2000
    AND CAST(T.ELECTRIC_RANGE AS INT) >= 0;

-- -------------------------------------------------------------------------------------------------------
-- 6. Validación
-- -------------------------------------------------------------------------------------------------------

SELECT * FROM ${hiveconf:ENV}_curated.ELECTRIC_VEHICLE LIMIT 10;

SHOW PARTITIONS ${hiveconf:ENV}_curated.ELECTRIC_VEHICLE;