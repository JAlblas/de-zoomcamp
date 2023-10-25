CREATE OR REPLACE EXTERNAL TABLE `de-taxi-401414.trips_data_all.external_fhv_tripdata`
OPTIONS
(
  format = 'CSV',
  uris = ['gs://taxi_data_lake_de-taxi-401414/raw/fhv_tripdata_2019-*.csv.gz']
);


CREATE OR REPLACE TABLE `de-taxi-401414.trips_data_all.fhv_tripdata_nonpartitioned`
AS
SELECT *
FROM `trips_data_all
.external_fhv_tripdata`;


CREATE OR REPLACE TABLE `de-taxi-401414.trips_data_all.fhv_tripdata_partitioned`
PARTITION BY DATE
(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
(
  SELECT *
FROM `trips_data_all
.external_fhv_tripdata`);


--667 mb
SELECT count(*)
FROM `de
-taxi-401414.trips_data_all.fhv_tripdata_nonpartitioned`
WHERE DATE
(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN
('B00987', 'B02279', 'B02060');


--142 mb
SELECT count(*)
FROM `de
-taxi-401414.trips_data_all.fhv_tripdata_partitioned`
WHERE DATE
(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN
('B00987', 'B02279', 'B02060');