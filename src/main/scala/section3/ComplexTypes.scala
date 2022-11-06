package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
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


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  /**
   * Dates
   */

  val moviesWithRelease = moviesDF
    .select(
      $"Title",
      to_date($"Release_Date", "dd-MMM-yy")
        .as("release")
    )


  moviesWithRelease
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
    .withColumn(
      "age",
      // Another option: date_add() and date_sub()
      datediff($"now", $"release") / 365)
  //    .show()


  //  Dates that can't be parsed by to_date() are set as null
  val differentFormatMovies = moviesWithRelease
    .select("*")
    .where($"release".isNull)
  //  differentFormatMovies.show()
  //  println(s"Total Bad Dates: ${differentFormatMovies.count()}")


  /**
   * Structures
   */

  val structMovies = moviesDF
    .select(
      $"Title",
      struct($"US_Gross", $"Worldwide_Gross")
        .as("profit")
    )
  //  structMovies.show()

  structMovies
    .select(
      $"Title",
      $"profit"
        .getField("US_Gross")
        .as("us_gross")
    )
  //    .show()


  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross"))
        .as("profit")
    )
    .select(
      col("Title"),
      col("profit")
        .getField("US_Gross")
        .as("us_gross")
    )
  //    .show()

  moviesDF
    .selectExpr(
      "Title",
      "(US_Gross, Worldwide_Gross) as profit"
    )
    .selectExpr(
      "Title",
      "profit.US_Gross as us_gross"
    )
  //    .show()


  /**
   * Arrays
   */


  val splitMovieTitles = moviesDF
    .select(
      $"Title",
      split($"Title",
        // RegEx
        " |,")
        .as("title_words")
    )
  //  splitMovieTitles.show()


  splitMovieTitles
    .select(
      $"Title",
      expr("title_words[0]"),
      size($"title_words"),
      array_contains($"title_words", "Love")
    )
  //    .show()


}
