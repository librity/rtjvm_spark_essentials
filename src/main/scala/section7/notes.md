# Notes

## Big Data Set

New York City taxi ride data from 2009 to 2016: 35 GB as `.parquet`, 400 GB as uncompressed `.csv`

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Download with Transmission:

- https://academictorrents.com/details/4f465810b86c6b793d1c7556fe3936441081992e
- https://transmissionbt.com/download#unixdistros

Smaller Data Set at `src/main/resources/data/yellow_taxi_jan_25_2018`
Also download the taxi zone lookup table:

- https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

### Format

`nyc_taxi_2009_2016.parquet`:

```elixir
root
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- dropoff_latitude: float (nullable = true)
 |-- dropoff_longitude: float (nullable = true)
 |-- dropoff_taxizone_id: integer (nullable = true)
 |-- ehail_fee: float (nullable = true)
 |-- extra: float (nullable = true)
 |-- fare_amount: float (nullable = true)
 |-- improvement_surcharge: float (nullable = true)
 |-- mta_tax: float (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- pickup_latitude: float (nullable = true)
 |-- pickup_longitude: float (nullable = true)
 |-- pickup_taxizone_id: integer (nullable = true)
 |-- rate_code_id: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- tip_amount: float (nullable = true)
 |-- tolls_amount: float (nullable = true)
 |-- total_amount: float (nullable = true)
 |-- trip_distance: float (nullable = true)
 |-- trip_type: string (nullable = true)
 |-- vendor_id: string (nullable = true)
 |-- trip_id: long (nullable = true)
 
 Size: 1,382,375,998!
```

`yellow_taxi_jan_25_2018`:

```elixir
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)

Size: 331,893
```

`taxi_zones.csv`:

```elixir
root
 |-- LocationID: integer (nullable = true)
 |-- Borough: string (nullable = true)
 |-- Zone: string (nullable = true)
 |-- service_zone: string (nullable = true)

Size: 265
```

### Questions

1. Which zones have the most pickups/dropoffs overall?
2. What are the peak hours for taxi?
3. How are the trips distributed? Why are people taking the cab?
4. What are the peak hours for long/short trips?
5. What are the top 3 pickup/dropoff zones for long/short trips?
6. How are people paying for the ride, on long/short trips?
7. How is the payment type evolving with time?
8. Can we explore a ride-sharing opportunity by grouping close short trips?

## Spark

- https://sparkbyexamples.com/spark/spark-extract-hour-minute-and-second-from-timestamp/

## Run on AWS with `S3` and `EMR`

1. Save data and `spark-essentials.jar` in an S3 bucket
2. Create a Spark EMR (Elastic Map Reduce) cluster
3. Connect to cluster with`ssh`
4. Get `spark-essentials.jar` from S3

```bash
$ aws s3 cp s3://BUCKET_NAME/spark-essentials.jar .
```

5. Submit the job:

```bash
$ which spark-submit
$ spark-submit \
  --class section7.TaxiBigData \
  --supervise \
  --verbose \
  spark-essentials.jar \
  s3://BUCKET_NAME/nyc_taxi_2009_2016.parquet \
  s3://BUCKET_NAME/taxi_zones.csv \
  s3://BUCKET_NAME/taxi_big_data_results
```

6. Wait for it to finish
7. Terminate the cluster so you don't end up bankrupt

## Run on Azure with `Blob storage` and `HDInsight`

- https://learn.microsoft.com/en-us/azure/architecture/aws-professional/storage
- https://learn.microsoft.com/en-us/azure/architecture/aws-professional/services
- https://stackoverflow.com/questions/67395418/azure-equivalent-of-aws-athena-over-s3
- https://azure.microsoft.com/en-us/products/storage/blobs/
- https://azure.microsoft.com/en-us/products/hdinsight/#overview
- https://learn.microsoft.com/en-us/azure/?product=popular
- https://learn.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage
