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
   * Exercise
   * - [ ] Sum up ALL the profits of ALL movies in the Data Frame
   * - [ ] Count how many distinct directors we have
   * - [ ] Show the mean and standard deviation of US_Gross
   * - [ ] Compute the average IMDB_Rating and the average US_Gross revenue PER DIRECTOR
   */

  val moviesDF = getMoviesDF

//  val profits = moviesDF.s


  def getMoviesDF = {
    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")
    moviesDF.show()
    println(s"MOVIES COUNT: ${moviesDF.count()}")

    moviesDF
  }
}
