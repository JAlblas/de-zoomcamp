CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_fhv_tripdata`
OPTIONS
(
  format = 'PARQUET',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/fhv_tripdata_2019-*.parquet']
);


CREATE OR REPLACE TABLE `de-taxi-401414.trips_data_all.fhv_tripdata_nonpartitioned`
AS
SELECT *
FROM `trips_data_all
.external_fhv_tripdata`;