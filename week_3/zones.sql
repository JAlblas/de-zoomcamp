CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_zones`
OPTIONS
(
  format = 'PARQUET',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/zones']
);