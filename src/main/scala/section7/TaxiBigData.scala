package section7

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
   *
   * We can track keep track of the jobs' progress at:
   * http://localhost:4040
   */

  val taxiDF = spark.read.load("path/to/taxi/big/data")
  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

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

  //  shortPickupDropoffPopularity.show(false)
  //  longPickupDropoffPopularity.show(false)

  /**
   * 6. How are people paying for the ride, on long/short trips?
   *
   * Rate Code IDs: 1 (standard), 2 (JFK), 3 (Newark), 4 (Nassau/Westchester) or 5 (negotiated)
   * Payment Types: 1 (credit card), 2 (cash), 3 (no charge), 4 (dispute), 5 (unknown), 6 (voided)
   *
   * Conclusion:
   *
   * Most long and short trips have the standard rate and are payed with Credit Card.
   * Cash is a bit more prevalent on short trips, but it's definitely dying for this service.
   *
   * Proposal:
   *
   * - Make sure card payments work 24/7 and can handle the load.
   * - Accept more convenient forms of credit car payment
   * like NFC credit cards and phone payments.
   */

  def getDistribution(predicate: Column, group: String) = tripsWithLength
    .where(predicate)
    .groupBy(group)
    .agg(count("*").as("total_trips"))
    .orderBy($"total_trips".desc_nulls_last)

  val shortRateCodeDistribution =
    getDistribution(not($"is_long"), "RatecodeID")
  val shortPaymentTypeDistribution =
    getDistribution(not($"is_long"), "payment_type")

  val longRateCodeDistribution =
    getDistribution($"is_long", "RatecodeID")
  val longPaymentTypeDistribution =
    getDistribution($"is_long", "payment_type")

  //  shortRateCodeDistribution.show()
  //  longRateCodeDistribution.show()
  //
  //  shortPaymentTypeDistribution.show()
  //  longPaymentTypeDistribution.show()

  /**
   * 7. How is the payment type evolving with time?
   *
   * We can better visualize this by plotting it with pandas/whatever
   * over more data.
   *
   * Conclusion:
   * Given the small sample of the data, there was no significant change
   * in payment distribution between the two days.
   *
   * There was an sharp increase of total trips
   * from 2018-01-24 (Wednesday) to 2018-01-25 (Thursday).
   *
   * Proposal:
   *
   * - Get more data.
   * - We should aggregate and plot by total trips per day
   * so we can visualize and predict demand.
   */

  val paymentTypeEvolution = taxiDF
    .groupBy(
      to_date($"tpep_pickup_datetime").as("pickup_date"),
      $"payment_type"
    )
    .agg(count("*").as("total_trips"))
    //    .orderBy("pickup_date")
    .orderBy("payment_type")

  //  paymentTypeEvolution.show()


  /**
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   * Conclusion:
   *
   * - Lots of close taxi rides in the same location and time.
   * - There's massive ride sharing opportunities
   * concentrated in Upper East Side, Lincoln Square and Midtown.
   * - These would mostly occur around 4:00, 11:00 and 18:00.
   *
   * Proposal:
   * - Offer ride sharing and encourage it with discount.
   */
  val percentSharable = 0.872


  val sharableRides = taxiDF

    // Add five-minute window column
    .select(
      round(
        unix_timestamp($"tpep_pickup_datetime") / 300
      ).cast("integer").as("five_minute_id"),
      $"PULocationID",
      $"total_amount",
    )

    // Group by time time window and location, then count and add
    .groupBy($"five_minute_id", $"PULocationID")
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
    .join(taxiZonesDF, $"PULocationID" === $"LocationID")
    .drop("PULocationID", "LocationID", "service_zone")

    // Order by total trips
    .orderBy($"total_trips".desc_nulls_last)

  //  sharableRides.show(false)


  /**
   * Calculate the economic impact if:
   * - 5% of taxi trips detected to be shared at any time
   * - 30% of people actually accept to be grouped
   * - $5 discount if you take a grouped ride
   * - $2 extra to take an individual ride (privacy/time)
   * - if two rides grouped, reducing cost by 60% of one average ride
   *
   * Conclusion:
   * - Estimate profit of $10 per pickup locations
   * - $39,987 profit for the 2-day data set, or $20,000 per day.
   * - $7,300,000 profit per year.
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


  rideSharingImpact.show(100, true)
  totalProfitFromRideSharing.show()


  def inspectDF(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show()
    println(s"Size: ${dataFrame.count()}")
  }

}
