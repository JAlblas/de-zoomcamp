CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_yellow_tripdata`
OPTIONS
(
  format = 'PARQUET',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/yellow_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `de-taxi-401414.trips_data_all.yellow_tripdata_nonpartitioned`
AS
SELECT *
FROM `trips_data_all
.external_yellow_tripdata`;