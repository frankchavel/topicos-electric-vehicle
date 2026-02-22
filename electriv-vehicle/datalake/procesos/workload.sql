-- -------------------------------------------------------------------------------------------------------
-- COMANDO DE EJECUCION
-- beeline -u jdbc:hive2:// -f workload.sql --hiveconf "PARAM_USERNAME=hadoop" --hiveconf "ENV=DEV"
-- -------------------------------------------------------------------------------------------------------

-- -------------------------------------------------------------------------------------------------------
-- 1. Definición de parámetros
-- -------------------------------------------------------------------------------------------------------

SET ENV=DEV;
SET PARAM_USERNAME=hadoop;

-- -------------------------------------------------------------------------------------------------------
-- 2. Uso de base de datos WORKLOAD
-- -------------------------------------------------------------------------------------------------------

USE ${hiveconf:ENV}_workload;

-- -------------------------------------------------------------------------------------------------------
-- 3. Eliminación de tabla si existe
-- -------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS ELECTRIC_VEHICLE;

-- -------------------------------------------------------------------------------------------------------
-- 4. Creación de tabla ELECTRIC_VEHICLE
-- -------------------------------------------------------------------------------------------------------

CREATE TABLE ELECTRIC_VEHICLE(
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
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/datalake/${hiveconf:ENV}_workload/electric_vehicle'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);

-- -------------------------------------------------------------------------------------------------------
-- 5. Carga de datos
-- -------------------------------------------------------------------------------------------------------

LOAD DATA LOCAL INPATH '/mnt/c/Users/fchav/Documents/Topicos/electriv-vehicle/datalake/data/Electric_Vehicle.csv'
INTO TABLE ELECTRIC_VEHICLE;

-- -------------------------------------------------------------------------------------------------------
-- 6. Validación
-- -------------------------------------------------------------------------------------------------------

SELECT * FROM ELECTRIC_VEHICLE LIMIT 10;
