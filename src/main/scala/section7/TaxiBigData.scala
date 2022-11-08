package section7

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

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
   * Load Big Data Set
   *
   * You can download it with a bittorrent client here:
   * - https://academictorrents.com/details/4f465810b86c6b793d1c7556fe3936441081992e
   *
   * We can track keep track of the jobs' progress at:
   * http://localhost:4040
   */

  val taxiDF = spark.read
    .load("/media/lgeniole/hd1t1/big_data/nyc_taxi_2009_2016.parquet")
  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  //    inspectDF(taxiDF)
  //    inspectDF(taxiZonesDF)

  /**
   * Questions
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   *
   * Conclusion:
   * - Data is heavily skewed towards Manhattan
   * - Hottest pickup zones:
   * Upper East Side South, Midtown Center, East Village,
   * Times Sq/Theatre District, Midtown East, Upper East Side North,
   * Union Sq, Murray Hill, Clinton East, Penn Station/Madison Sq West,
   * Lincoln Square Eas, Gramercy, Midtown North, East Chelsea,
   * Upper West Side South, Lenox Hill West, Midtown South, West Village
   *
   * Proposal:
   * - Charge fares proportional to this skew.
   * - Charge extra at the hottest pickup zones.
   */

  val pickupsByZone = taxiDF
    .groupBy("pickup_taxizone_id")
    .agg(count("*").as("total_trips"))
    .join(
      taxiZonesDF,
      col("pickup_taxizone_id") === col("LocationID")
    )
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)

  //  pickupsByZone.show(false)
  //  saveDF(pickupsByZone, "pickups_by_zone")

  /**
   * 1b. Group by borough
   */

  val pickupsByBorough = pickupsByZone
    .groupBy("Borough")
    .agg(sum("total_trips").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

  //  pickupsByBorough.show(false)
  //  saveDF(pickupsByBorough, "pickups_by_borough")

  /**
   * 2. What are the peak hours for taxi?
   *
   * Conclusion:
   * - There are clear hours of peak demand (15:00 to 20:00).
   *
   * Proposal:
   * - Increase the fare proportional to the demand per hour.
   */

  val ridesByTime = taxiDF
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .groupBy("pickup_hour")
    .agg(count("*").as("trips_per_hour"))
    .orderBy($"trips_per_hour".desc_nulls_last)
  //    .orderBy("pickup_hour")

  //  ridesByTime.show(24, false)
  //  saveDF(ridesByTime, "rides_by_time")

  /**
   * 3. How are the trips distributed by length? Why are people taking the cab?
   *
   * Conclusion:
   * - The mean trip is 5.2 miles, and most are less than 2 miles.
   * - The Standard Deviation is 7291: ride distance for the sample is extremely diverse.
   *
   * Proposal:
   * - Increase the fare for trips shorter than 2 miles
   */

  val tripDistances = taxiDF
    .select($"trip_distance".as("distance"))
  val longDistanceThreshold = 2 // miles

  val tripDistanceStats = tripDistances
    .select(
      count("*").as("count"),
      lit(longDistanceThreshold).as("threshold"),
      avg("distance").as("average"),
      mean("distance").as("mean"),
      stddev("distance").as("stddev"),
      min("distance").as("min"),
      max("distance").as("max"),
    )

  //  tripDistanceStats.show(false)
  //  saveDF(tripDistanceStats, "trip_distance_stats")


  val tripsWithLength = taxiDF
    .withColumn("is_long", $"trip_distance" >= longDistanceThreshold)
  val tripsByLenght = tripsWithLength
    .groupBy("is_long")
    .count()

  //  tripsByLenght.show(false)
  //  saveDF(tripsByLenght, "trips_by_lenght")

  /**
   * 4. What are the peak hours for long/short trips?
   *
   * Conclusion:
   * - The peak hour for short trips is 16:00.
   * - The peak hour for long trips is 19:00
   *
   * Proposal:
   * - Charge more for each type of trip at its respective peak hours.
   */

  val ridesByTimeAndDistance = tripsWithLength
    .withColumn("pickup_hour", hour($"pickup_datetime"))
    .groupBy("pickup_hour", "is_long")
    .agg(count("*").as("trips_per_hour"))
    .orderBy($"trips_per_hour".desc_nulls_last)
  //    .orderBy("pickup_hour")

  //  ridesByTimeAndDistance.show(48, false)
  //  saveDF(ridesByTimeAndDistance, "rides_by_time_and_distance")


  /**
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   *
   * Conclusion:
   *
   * The most popular short trip pickup/dropoff zones are
   * Upper East Side and Midtown.
   *
   * The most popular long trip pickup/dropoff zones are
   * LaGuardia Airport, Times Sq/Theatre District and Midtown.
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
    .groupBy("pickup_taxizone_id", "dropoff_taxizone_id")
    .agg(count("*").as("total_trips"))

    .join(taxiZonesDF, $"pickup_taxizone_id" === $"LocationID")
    .where($"Zone" !== "NV")
    .withColumnRenamed("Zone", "pickup_zone")
    .drop("LocationID", "Borough", "service_zone")

    .join(taxiZonesDF, $"dropoff_taxizone_id" === $"LocationID")
    .where($"Zone" !== "NV")
    .withColumnRenamed("Zone", "dropoff_zone")
    .drop("LocationID", "Borough", "service_zone")

    .drop("pickup_taxizone_id", "dropoff_taxizone_id")
    .orderBy($"total_trips".desc_nulls_last)
    .limit(6)

  val shortPickupDropoffPopularity =
    getPickupDropoffPopularity(not($"is_long"))
  val longPickupDropoffPopularity =
    getPickupDropoffPopularity($"is_long")

  //    shortPickupDropoffPopularity.show(false)
  //    saveDF(shortPickupDropoffPopularity, "short_pickup_dropoff_popularity")

  //  longPickupDropoffPopularity.show(false)
  //  saveDF(longPickupDropoffPopularity, "long_pickup_dropoff_popularity")

  /**
   * 6. How are people paying for the ride, on long/short trips?
   *
   * Rate Code IDs:
   * 1 standard - 2 JFK - 3 Newark - 4 Nassau/Westchester - 5 negotiated
   *
   * Payment Types:
   * 1 credit card - 2 cash - 3 nocharge - 4 dispute - 5 unknown - 6 voided
   *
   * Conclusion:
   *
   * Cash and credit payments are evenly matched on the Big Data Set:
   * For short trips, Cash beats Credit by about 100,000,000 rides.
   * For long trips, Credit beats Cash by about 50,000,000 rides.
   *
   * The Big Dat Set contains older data
   * when credit card payments were less prevalent.
   *
   * Proposal:
   * - Filter out the older portion of the data set (say 2013)
   * - Gather and analyze more recent data on the payment habits of taxi riders
   */

  def getDistribution(predicate: Column, group: String) = tripsWithLength
    .where(predicate)
    .groupBy(group)
    .agg(count("*").as("total_trips"))
    .orderBy($"total_trips".desc_nulls_last)

  val shortRateCodeDistribution =
    getDistribution(not($"is_long"), "rate_code_id")
  val shortPaymentTypeDistribution =
    getDistribution(not($"is_long"), "payment_type")

  val longRateCodeDistribution =
    getDistribution($"is_long", "rate_code_id")
  val longPaymentTypeDistribution =
    getDistribution($"is_long", "payment_type")

  //  shortRateCodeDistribution.show(false)
  //  saveDF(shortRateCodeDistribution, "short_rate_code_distribution")
  //  longRateCodeDistribution.show(false)
  //  saveDF(longRateCodeDistribution, "long_rate_code_distribution")

  //  shortPaymentTypeDistribution.show(false)
  //  saveDF(shortPaymentTypeDistribution, "short_payment_type_distribution")
  //  longPaymentTypeDistribution.show(false)
  //  saveDF(longPaymentTypeDistribution, "long_payment_type_distribution")

  /**
   * 7. How is the payment type evolving with time?
   *
   * We can better visualize this by plotting it with pandas/d.
   *
   * Conclusion:
   *
   * - The payment_type column is very dirty,
   * with many different names for the same payment type.
   * - The resulting data set is so big
   * that it's hard to draw meaningful conclusions at a glance.
   *
   * Proposal:
   *
   * - Clean the payment_type column and run this again.
   * - Aggregate and plot by total trips per day
   * to better visualize it and predict demand.
   */

  val paymentTypeEvolution = taxiDF
    .groupBy(
      to_date($"pickup_datetime").as("pickup_date"),
      $"payment_type"
    )
    .agg(count("*").as("total_trips"))
    //    .orderBy("pickup_date")
    .orderBy("payment_type")

  //  paymentTypeEvolution.show(false)
  //  saveDF(paymentTypeEvolution, "payment_type_evolution")


  /**
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   * Conclusion:
   *
   * - Lots of close taxi rides in the same location and time.
   * - There's a massive ride sharing opportunity for East Village riders.
   * - These would mostly occur between 23:00 and 00:00.
   *
   * Proposal:
   * - Offer ride sharing and encourage it with discount.
   */
  val percentSharable = 0.872


  val sharableRides = taxiDF

    // Add five-minute window column
    .select(
      round(
        unix_timestamp($"pickup_datetime") / 300
      ).cast("integer").as("five_minute_id"),
      $"pickup_taxizone_id",
      $"total_amount",
    )

    // Group by time time window and location, then count and add
    .groupBy($"five_minute_id", $"pickup_taxizone_id")
    .agg(
      (count("*") * percentSharable).as("total_trips"),
      sum("total_amount").as("revenue"),
    )

    // Transform 5-minute windows back to human-readable time
    .withColumn(
      "approximate_datetime",
      from_unixtime($"five_minute_id" * 300)
    )
    .drop("five_minute_id")

    // Get Borough and Pickup Zone
    .join(taxiZonesDF, $"pickup_taxizone_id" === $"LocationID")
    .drop("pickup_taxizone_id", "LocationID", "service_zone")

    // Order by total trips
    .orderBy($"total_trips".desc_nulls_last)

  //  sharableRides.show(false)
  //  saveDF(sharableRides, "sharable_rides")


  /**
   * Calculate the economic impact if:
   * - 5% of taxi trips detected to be shared at any time
   * - 30% of people actually accept to be grouped
   * - $5 discount if you take a grouped ride
   * - $2 extra to take an individual ride (privacy/time)
   * - if two rides grouped, reducing cost by 60% of one average ride
   *
   * Conclusion:
   * - Estimate profit of $20 per pickup locations
   * - $139,367,639 profit for the 7-year data set
   * - $20,000,000 profit per year.
   */

  val percentShareAttempt = 0.05
  val percentAcceptSharing = 0.30
  val percentRejectSharing = 1 - percentAcceptSharing

  val shareDiscount = 5
  val noShareCost = 2

  val avgRideCost = taxiDF
    .select(avg("total_amount"))
    .as[Double]
    .take(1)(0)
  val avgReducedRideCost = avgRideCost * 0.60

  val rideSharingImpact = sharableRides
    .withColumn("shared_rides", $"total_trips" * percentShareAttempt)
    .withColumn(
      "accepted_shared_rides_impact",
      $"shared_rides" * percentAcceptSharing * (avgReducedRideCost - shareDiscount)
    )
    .withColumn(
      "rejected_shared_rides_impact",
      $"shared_rides" * percentRejectSharing * noShareCost
    )
    .withColumn(
      "total_impact",
      $"accepted_shared_rides_impact" + $"rejected_shared_rides_impact"
    )

  val totalProfitFromRideSharing = rideSharingImpact
    .select(sum("total_impact").as("total"))


  //  rideSharingImpact.show(100, true)
  //  saveDF(rideSharingImpact, "ride_sharing_impact")
  //  totalProfitFromRideSharing.show(false)
  //  saveDF(totalProfitFromRideSharing, "total_profit_from_ride_sharing")


  def inspectDF(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show(false)
    println(s"Size: ${dataFrame.count()}")
  }

  def saveDF(dataFrame: DataFrame, fileName: String): Unit = {
    dataFrame
      .limit(200)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(buildResultsPath(fileName))
  }

  def buildResultsPath(fileName: String) =
    s"src/main/resources/data/taxi_big_data_results/$fileName"

}
