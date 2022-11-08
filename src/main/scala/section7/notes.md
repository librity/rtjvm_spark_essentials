# Notes

## Big Data Set

New York City taxi data:
35 GB as `.parquet`, 400 GB as uncompressed `.csv`

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://academictorrents.com/details/4f465810b86c6b793d1c7556fe3936441081992e

Download with Transmission:

- https://transmissionbt.com/download#unixdistros

Smaller Data Set at `src/main/resources/data/yellow_taxi_jan_25_2018`
Also download the taxi zone lookup table:

- https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

### Format

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

Size: 331893
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
