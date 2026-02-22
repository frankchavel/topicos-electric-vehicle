-- -------------------------------------------------------------------------------------------------------
-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f landing.sql --hiveconf "PARAM_USERNAME=hadoop" --hiveconf "ENV=DEV"
-- -------------------------------------------------------------------------------------------------------

-- -------------------------------------------------------------------------------------------------------
-- 1. Definición de parámetros
-- -------------------------------------------------------------------------------------------------------

SET ENV=DEV;
SET PARAM_USERNAME=hadoop;

-- -------------------------------------------------------------------------------------------------------
-- 2. Eliminación de base de datos
-- -------------------------------------------------------------------------------------------------------

DROP DATABASE IF EXISTS ${hiveconf:ENV}_landing CASCADE;

-- -------------------------------------------------------------------------------------------------------
-- 3. Creación de base de datos
-- -------------------------------------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS ${hiveconf:ENV}_landing
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_landing';

-- -------------------------------------------------------------------------------------------------------
-- 4. Tunning
-- -------------------------------------------------------------------------------------------------------

SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- -------------------------------------------------------------------------------------------------------
-- 5. Creación tabla ELECTRIC_VEHICLE en LANDING (formato AVRO)
-- -------------------------------------------------------------------------------------------------------

CREATE TABLE ${hiveconf:ENV}_landing.ELECTRIC_VEHICLE
STORED AS AVRO
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_landing/electric_vehicle'
TBLPROPERTIES(
    'avro.schema.url'='hdfs:///user/${hiveconf:PARAM_USERNAME}/datalake/schema/${hiveconf:ENV}_landing/electric_vehicle.avsc',
    'avro.output.codec'='snappy'
);


-- -------------------------------------------------------------------------------------------------------
-- 6. Inserción de datos desde WORKLOAD
-- -------------------------------------------------------------------------------------------------------

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;
SET hive.exec.mode.local.auto=true;

INSERT INTO TABLE ${hiveconf:ENV}_landing.ELECTRIC_VEHICLE
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
FROM ${hiveconf:ENV}_workload.ELECTRIC_VEHICLE;


-- -------------------------------------------------------------------------------------------------------
-- 7. Validación
-- -------------------------------------------------------------------------------------------------------

SELECT * FROM ${hiveconf:ENV}_landing.ELECTRIC_VEHICLE LIMIT 10;

