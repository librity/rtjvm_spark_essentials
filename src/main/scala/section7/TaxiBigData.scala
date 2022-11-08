package section7

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TaxiBigData extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 7.1 - Taxi Big Data Application")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
   * Load Data Sets
   */

  //  val bigTaxiDF = spark.read.load("path/to/taxi/big/data")
  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  //  inspect(bigTaxiDF)
  //  inspect(taxiDF)
  //  inspect(taxiZonesDF)

  /**
   * Questions
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   *
   * Conclusion: Data is skewed towards Manhattan:
   * Proposal: Charge fares proportional to this skew.
   */

  val pickupsByZone = taxiDF
    .groupBy("PULocationID")
    .agg(count("*").as("total_trips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)
  //  pickupsByZone.show()

  /**
   * 1b. Group by borough
   */

  val pickupsByBorough = pickupsByZone
    .groupBy("Borough")
    .agg(sum("total_trips").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)
  //  pickupsByBorough.show()

  /**
   * 2. What are the peak hours for taxi?
   *
   * Conclusion: There are clear hours of peak demand (14:00 to 18:00).
   * Proposal: Increase the fare proportional to the demand per hour.
   */

  val ridesByTime = taxiDF
    .withColumn("pickup_hour", hour($"tpep_pickup_datetime"))
    .groupBy("pickup_hour")
    .agg(count("*").as("trips_per_hour"))
    .orderBy($"trips_per_hour".desc_nulls_last)
  //    .orderBy("pickup_hour")
  //  ridesByTime.show(24, false)

  /**
   * 3. How are the trips distributed by length? Why are people taking the cab?
   *
   * Conclusion: The mean trip is 2.7 miles, and most are less than 2 miles.
   * Proposal: Increase the fare for trips shorter than 2 miles
   */

  val tripDistances = taxiDF
    .select($"trip_distance".as("distance"))
  val longDistanceThreshold = 2 // miles

  val tripDistanceStats = tripDistances
    .select(
      count("*").as("count"),
      lit(longDistanceThreshold).as("threshold"),
      mean("distance").as("mean"),
      stddev("distance").as("stddev"),
      min("distance").as("min"),
      max("distance").as("max"),
    )
  //  tripDistanceStats.show()


  val tripsWithLength = taxiDF
    .withColumn("is_long", $"trip_distance" >= longDistanceThreshold)
  val tripsByLenght = tripsWithLength
    .groupBy("is_long")
    .count()
  //  tripsByLenght.show()


  /**
   * 4. What are the peak hours for long/short trips?
   *
   * Conclusion: The peak hour for short trips is 14:00.
   * The peak hour for long trips is 17:00
   * Proposal: Charge more for each type of trip
   * at its respective peak hours.
   */

  val ridesByTimeAndDistance = tripsWithLength
    .withColumn("pickup_hour", hour($"tpep_pickup_datetime"))
    .groupBy("pickup_hour", "is_long")
    .agg(count("*").as("trips_per_hour"))
    .orderBy($"trips_per_hour".desc_nulls_last)
  //    .orderBy("pickup_hour")
  //  ridesByTimeAndDistance.show(48, false)


  /**
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   *
   * Conclusion:
   *
   * The most popular short trip pickup/dropoff zones are
   * Upper East Side South, Upper East Side North and Midtown Center.
   *
   * The most popular long trip pickup/dropoff zones are
   * LaGuardia Airport, Times Sq/Theatre District and Midtown Center.
   *
   * There's a clear separation for long and short trips;
   * Short trips concentrate on wealthy zones (bars and restaurants).
   * Long trips are mostly for airport transfers.
   *
   * Proposal:
   *
   * - There's a demand for airport rapid transit
   * that could be supplied by the Government or private companies.
   * - Taxi companies can separate market segments and tailor services to each.
   * - Strike partnerships with bars/restaurants for pickup service.
   */

  def getPickupDropoffPopularity(predicate: Column) = tripsWithLength
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("total_trips"))

    .join(taxiZonesDF, $"PULocationID" === $"LocationID")
    .where($"Zone" !== "NV")
    .withColumnRenamed("Zone", "pickup_zone")
    .drop("LocationID", "Borough", "service_zone")

    .join(taxiZonesDF, $"DOLocationID" === $"LocationID")
    .where($"Zone" !== "NV")
    .withColumnRenamed("Zone", "dropoff_zone")
    .drop("LocationID", "Borough", "service_zone")

    .drop("PULocationID", "DOLocationID")
    .orderBy($"total_trips".desc_nulls_last)
    .limit(6)

  val shortPickupDropoffPopularity =
    getPickupDropoffPopularity(not($"is_long"))
  val longPickupDropoffPopularity =
    getPickupDropoffPopularity($"is_long")

  shortPickupDropoffPopularity.show(false)
  longPickupDropoffPopularity.show(false)

  /**
   * 6. How are people paying for the ride, on long/short trips?
   *
   * Conclusion:
   * Proposal:
   */


  /**
   * 7. How is the payment type evolving with time?
   *
   * Conclusion:
   * Proposal:
   */


  /**
   * 8. Can we explore a ride -sharing opportunity by grouping close short trips?
   *
   * Conclusion:
   * Proposal:
   */


  /**
   */


  /**
   */


  /**
   */


  /**
   */


  def inspect(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show()
    println(s"Size: ${dataFrame.count()}")
  }

}
