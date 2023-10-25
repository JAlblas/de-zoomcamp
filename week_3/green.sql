CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_green_tripdata`
OPTIONS
(
  format = 'PARQUET',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/green_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `de-taxi-401414.trips_data_all.green_tripdata_nonpartitioned`
AS
SELECT *
EXCEPT (ehail_fee) FROM `trips_data_all.external_green_tripdata`;