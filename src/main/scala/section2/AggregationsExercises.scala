package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AggregationsExercises extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.8 - Aggregations Exercises")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * Exercise 1
   *
   * - [x] Sum up ALL the profits of ALL movies in the Data Frame
   * - [x] Count how many distinct directors we have
   * - [x] Show the mean and standard deviation of US_Gross
   * - [x] Compute the average IMDB_Rating and the average US_Gross revenue PER DIRECTOR
   */

  val moviesDF = getMoviesDF
  val filledMovies = moviesDF.na.fill(0)


  // --------------------------------------------------------------------------------------------------
  val profits = filledMovies.selectExpr(
    "Worldwide_Gross + US_DVD_Sales - Production_Budget AS Profit"
  )
  profits.select(sum("Profit")).show()


  filledMovies.select((
    col(
      "Worldwide_Gross")
      + col("US_DVD_Sales"))
    .as("Revenue")
  )
    .select(sum("Revenue"))
    .show()


  // --------------------------------------------------------------------------------------------------
  val directorsCount = moviesDF.select(countDistinct(col("Director")))
  directorsCount.show()


  // --------------------------------------------------------------------------------------------------
  val usGrossStats = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross"))
  usGrossStats.show()

  val worldwideGrossStats = moviesDF.select(
    mean("Worldwide_Gross"),
    stddev("Worldwide_Gross"))
  worldwideGrossStats.show()

  val profitGrossStats = profits.select(
    mean("Profit"),
    stddev("Profit"))
  profitGrossStats.show()


  // --------------------------------------------------------------------------------------------------
  val bestDirectors = filledMovies
    .groupBy("Director")
    .avg("IMDB_Rating", "US_Gross")
    .orderBy(col("avg(IMDB_Rating)").desc)
  bestDirectors.show()

  val sellouts = filledMovies
    .withColumn(
      "Profit",
      expr("Worldwide_Gross + US_DVD_Sales - Production_Budget")
    )
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating"),
      avg("US_Gross"),
      avg("Worldwide_Gross").as("Avg_Worldwide_Gross"),
      avg("Profit"),
    )
    .orderBy(col("Avg_Worldwide_Gross").desc)
  sellouts.show()


  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      sum("US_Gross").as("Total_US_Gross"),
    )
    .orderBy(col("Avg_IMDB_Rating").desc_nulls_last)
    .show()


  // --------------------------------------------------------------------------------------------------
  def getMoviesDF = {
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")
    moviesDF.show()
    println(s"MOVIES COUNT: ${moviesDF.count()}")
    moviesDF
  }
}
