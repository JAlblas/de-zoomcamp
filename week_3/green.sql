CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_green_tripdata`
OPTIONS
(
  format = 'PARQUET',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/green_tripdata_2019-*.parquet', 'gs://taxi_data_lake_de-taxi-401414/raw/green_tripdata_2019-*.parquet']
);