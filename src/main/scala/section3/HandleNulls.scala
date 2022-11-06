package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HandleNulls extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.3 - Handle Nulls")
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
   * coalesce()
   *
   * Select the first non-null rating (IMDB or Rotten Tomatoes)
   */

  moviesDF
    .select(
      $"Title",
      $"Rotten_Tomatoes_Rating",
      $"IMDB_Rating",
      coalesce(
        $"Rotten_Tomatoes_Rating",
        $"IMDB_Rating" * 10)
        .as("some_rating")

    )
  //    .show()

  /**
   * Checking for nulls
   */

  val nullCondition = $"Rotten_Tomatoes_Rating".isNull or $"IMDB_Rating".isNull
  moviesDF
    .select("*")
    .where(nullCondition)
  //    .show()


  /**
   * Nulls when ordering
   */

  moviesDF
    .orderBy(
      $"IMDB_Rating".desc_nulls_last)
  //    .show()


  /**
   * Remove nulls
   */

  moviesDF
    .select(
      $"Title",
      $"IMDB_Rating"
    )
    .na.drop()
  //    .show()


  /**
   * Replace nulls
   */

  moviesDF
    .na.fill(
    0,
    List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  //    .show()

  moviesDF
    .na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "unknown"
  ))
  //    .show()

  /**
   * Complex Expressions
   */


  moviesDF
    .selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",

      // Same as coalesce
      "ifNull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as if_null",

      // Same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl",

      // IF: both values are equal RETURNS null
      // ELSE: returns the first one
      // if (first == second) null else first
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as null_if",

      // if (first != null) second else third
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2",
    )
    .show()


  /**
   *
   */


  /**
   *
   */


  /**
   *
   */

}
