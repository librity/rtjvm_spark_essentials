package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.1 - Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  /**
   * Adding a plain value to a Data Frame
   */

  moviesDF
    .select(
      col("Title"),
      lit(42).as("plain_value"),
      lit(true).as("bool_value"),
      lit("42").as("string_value"),
    )
  //    .show()


  /**
   * Booleans
   */

  moviesDF
    .select("Title")
    .where(
      col("Major_Genre") === "Drama"
    )
  //    .show()

  // Or: Create individual filters
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF
    .select("Title")
    .where(preferredFilter)
  //    .show()

  val preferredMovies = moviesDF
    .select(
      $"Title",
      preferredFilter.as("good_movie")
    )
  //  preferredMovies.show()

  preferredMovies
    .where("good_movie")
  //    .show()


  /**
   * Negations
   */

  preferredMovies
    .where(
      not(
        col("good_movie")
      )
    )
  //    .show()


  /**
   * Math Operators
   */


  val averageRating = moviesDF
    .select(
      col("Title"),
      ((
        col("Rotten_Tomatoes_Rating") / 10.0
          + col("IMDB_Rating")
        ) / 2.0
        )
    )
  //    .show()
  // WARNING: Operating on non-numerical values will crash Spark.

  /**
   * Correlation between Columns (Pearson)
   * - Between -1 and 1:
   * - 1: Both values increase in the same direction (Positive Correlated)
   * - -1: When one value increases the other decreases (Negative Correlated)
   * - Between -0.3 and 0.3: No significant correlation between values
   */

  val ratingsCorellation = moviesDF.stat
    // .corr() Is an action (data gets evaluated)
    .corr(
      "Rotten_Tomatoes_Rating",
      "IMDB_Rating")
  //  println(s"Ratings Correlation: $ratingsCorellation")


  /**
   * Strings
   */

  // Capitalize String Column
  carsDF
    .select(
      initcap($"Name")
    )
  //    .show()


  carsDF
    .select("*")
    .where($"Name".contains("volkswagen"))
  //    .show()

  /**
   * Regular Expression
   */

  val volkswagenRegEx = "volkswagen|vw"
  val volkswagens = carsDF
    .select(
      $"Name",
      regexp_extract(
        $"Name",
        volkswagenRegEx,
        0
      ).as("regex_extract")
    )
    .drop("regex_extract")
    .where($"regex_extract" =!= "")
  //  volkswagens.show()


  volkswagens
    .select(
      $"Name",
      regexp_replace(
        $"Name",
        volkswagenRegEx,
        "VW"
      ).as("regex_replace")
    )
//    .show()


}
