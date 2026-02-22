-- -------------------------------------------------------------------------------------------------------
-- CREACION BASE DE DATOS FUNCTIONAL
-- -------------------------------------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS DEV_FUNCTIONAL
LOCATION '/user/hadoop/datalake/DEV_FUNCTIONAL';

USE DEV_FUNCTIONAL;

-- -------------------------------------------------------------------------------------------------------
-- CONFIGURACION
-- -------------------------------------------------------------------------------------------------------

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.execution.engine=mr;
SET mapreduce.framework.name=local;

-- -------------------------------------------------------------------------------------------------------
-- CREACION TABLA FUNCIONAL
-- -------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS VEHICLE_RESUMEN_ESTATAL(
    STATE STRING,
    ELECTRIC_VEHICLE_TYPE STRING,
    TOTAL_VEHICLES INT,
    PROMEDIO_ELECTRIC_RANGE DOUBLE
)
PARTITIONED BY (MODEL_YEAR INT)
STORED AS PARQUET
LOCATION '/user/hadoop/datalake/DEV_FUNCTIONAL/vehicle_resumen_estatal'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- -------------------------------------------------------------------------------------------------------
-- PROCESO FUNCIONAL
-- -------------------------------------------------------------------------------------------------------

INSERT OVERWRITE TABLE VEHICLE_RESUMEN_ESTATAL
PARTITION (MODEL_YEAR)
SELECT
    STATE,
    ELECTRIC_VEHICLE_TYPE,
    COUNT(*) AS TOTAL_VEHICLES,
    AVG(ELECTRIC_RANGE) AS PROMEDIO_ELECTRIC_RANGE,
    MODEL_YEAR
FROM DEV_CURATED.ELECTRIC_VEHICLE
GROUP BY
    STATE,
    ELECTRIC_VEHICLE_TYPE,
    MODEL_YEAR;

-- -------------------------------------------------------------------------------------------------------
-- VALIDACION
-- -------------------------------------------------------------------------------------------------------

SELECT * FROM VEHICLE_RESUMEN_ESTATAL LIMIT 10;

SHOW PARTITIONS VEHICLE_RESUMEN_ESTATAL;