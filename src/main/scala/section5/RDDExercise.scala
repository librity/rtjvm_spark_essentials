package section5

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDExercise extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * Exercise 1 - With RDD transformations!
   *
   * 1. Read movies.json as an RDD
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val cleanMovies = moviesDF
    .withColumnRenamed("Title", "title")
    .withColumnRenamed("Major_Genre", "genre")
    .withColumnRenamed("IMDB_Rating", "rating")
    .select("title", "genre", "rating")

  case class Movie(title: String, genre: Option[String], rating: Option[Double])

  val moviesDS = cleanMovies.as[Movie]
  val moviesRDD = moviesDS.rdd

  //  println(s"DF count: ${moviesDF.count()}")
  //  println(s"DS count: ${moviesDS.count()}")
  //  println(s"RDD count: ${moviesRDD.count()}")

  /**
   * 2. Show the distinct genres as an RDD
   */

  moviesRDD
    .filter(_.genre.isDefined)
    .map(_.genre)
    .distinct()
  //    .foreach(println(_))

  /**
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.0
   */


  val dramaDFCount = moviesDF
    .where("Major_Genre = 'Drama'")
    .count()


  val dramaRDDCount = moviesRDD
    .filter(_.genre == Some("Drama"))
    .count()

  //  println(s"Drama DF count: $dramaDFCount")
  //  println(s"Drama RDD count: $dramaRDDCount")


  /**
   * 4. Show the average rating of movies by genre.
   */

  moviesDF
    .where(col("Major_Genre").isNotNull)
    .groupBy("Major_Genre")
    .agg(avg("IMDB_Rating"))
    .show()


  def calcAverage(list: Iterable[Double]) = list.sum / list.size.toDouble

  moviesRDD
    .filter(movie => movie.genre.isDefined && movie.rating.isDefined)
    .groupBy(_.genre)
    .map(group => (
      group._1.getOrElse("Unknown"),
      group._2.map(row => row.rating.getOrElse(0.0))
    ))
    .foreach(group => println(
      s"${group._1} average rating: ${calcAverage(group._2)}"
    ))


}
