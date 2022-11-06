package section3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import section3.ComplexTypesExercise.badDateMovies

object ComplexTypesExercise extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.1 - Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * 1. How do we deal with different format of dates in the same row?
   *
   */


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  val moviesWithRelease = moviesDF
    .where($"Release_Date".isNotNull)
    .select(
      $"*",
      to_date($"Release_Date", "dd-MMM-yy")
        .as("release")
    )
  //  inspect(moviesWithRelease)


  var badDateMovies = moviesWithRelease
    .where($"release".isNull)
  //  inspect(badDateMovies)

  /**
   * 1.1 Parse the data frame multiple times, then .union() the small Data Frames
   */


  //    Round 1

  var fixedDates1 = badDateMovies
    .drop($"release")
    .withColumn(
      "release",
      to_date($"Release_Date", "yyyy-mm-dd")
    )

  badDateMovies = fixedDates1
    .where($"release".isNull)

  fixedDates1 = fixedDates1
    .where($"release".isNotNull)
  //  inspect(fixedDates1)


  //    Round 2

  //  inspect(badDateMovies)

  var fixedDates2 = badDateMovies
    .drop($"release")
    .withColumn(
      "release",
      to_date($"Release_Date", "MMMMM, yyyy")
    )

  badDateMovies = fixedDates2
    .where($"release".isNull)

  fixedDates2 = fixedDates2
    .where($"release".isNotNull)

  //  inspect(fixedDates2)
  //  inspect(badDateMovies)


  //  Unions

  val fixedMovies = moviesWithRelease
    .union(fixedDates1)
    .union(fixedDates2)

  //  inspect(fixedMovies)

  /**
   * 1.2 Discard the Rows with bad dates.
   */

  val goodDateMovies = moviesWithRelease
    .where($"release".isNotNull)
  //    inspect(goodDateMovies)

  /**
   * 2. Read stocks.csv and parse the dates.
   */

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/resources/data/stocks.csv")
  //  stocksDF.printSchema()
  //    inspect(stocksDF)


  val stocksWithDates = stocksDF
    .withColumn(
      "parsed_date",
      to_date($"date", "MMM dd yyyy")
    )


  val badStockDates = stocksWithDates
    .where($"parsed_date".isNull)
  //    inspect(badStockDates)


  def inspect(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    println(s"Size: ${dataFrame.count()}")

  }
}


