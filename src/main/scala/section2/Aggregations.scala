package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.8 - Aggregations")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.show()
  println(s"MOVIES COUNT: ${moviesDF.count()}")


  /**
   * Counting
   */


  // Count all non-null genre values
  val genresCount = moviesDF.select(count(col("Major_Genre")))
  genresCount.show()
  // Another way
  moviesDF.selectExpr("count(Major_Genre)").show()


  // Count total number of rows including nulls
  moviesDF.select(count("*")).show()


  // Count distinct Major_Genre values
  moviesDF.select(countDistinct("Major_Genre")).show()


  // Approximate count (for BIG data frames/sets)
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()



  //  Min and Max
  moviesDF.select(min(column("IMDB_Rating"))).show()
  moviesDF.select(max(column("IMDB_Rating"))).show()
  moviesDF.select(min(column("Rotten_Tomatoes_Rating"))).show()
  moviesDF.select(max(column("Rotten_Tomatoes_Rating"))).show()



  // Sum
  moviesDF.select(sum(moviesDF.col("US_Gross"))).show()
  moviesDF.select(sum(col("Worldwide_Gross"))).show()
  moviesDF.select(sum(col("US_DVD_Sales"))).show()
  moviesDF.selectExpr("sum(Production_Budget)").show()

  // Average
  moviesDF.select(avg(moviesDF.col("Rotten_Tomatoes_Rating"))).show()
  moviesDF.selectExpr("avg(IMDB_Rating)").show()


  //  Data Science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  ).show()
  moviesDF.select(
    mean(col("IMDB_Rating")),
    stddev(col("IMDB_Rating")),
  ).show()


  // Grouping
  // Equivalent to SQL: SELECT COUNT(*) FROM moviesDF GROUP BY 'Major_Genre'
  val countByGenre = moviesDF
    // Includes null
    .groupBy(col("Major_Genre"))
    .count()
  countByGenre.show()


  val avgRatingByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
  avgRatingByGenre.show()

  val aggregationsByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating"),
    )
    .orderBy("Avg_Rating")
  aggregationsByGenre.show()

}
