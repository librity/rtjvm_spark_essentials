package section7

import org.apache.spark.sql.{DataFrame, SparkSession}

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
  inspect(taxiDF)
  inspect(taxiZonesDF)

  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */


  def inspect(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show()
    println(s"Size: ${dataFrame.count()}")
  }

}
