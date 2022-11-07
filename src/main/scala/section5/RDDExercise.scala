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
    .where(
      $"title".isNotNull
        and $"genre".isNotNull
        and $"rating".isNotNull)
    .select("title", "genre", "rating")

  case class Movie(title: String, genre: String, rating: Double)

  val moviesDS = cleanMovies.as[Movie]
  val moviesRDD = moviesDS.rdd

  println(s"DF count: ${cleanMovies.count()}")
  println(s"DS count: ${moviesDS.count()}")
  println(s"RDD count: ${moviesRDD.count()}")

  /**
   * 2. Show the distinct genres as an RDD
   */

  moviesRDD
    .map(_.genre)
    .distinct()
    //    .foreach(println(_))
    .toDF().show()

  /**
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.0
   */


  val dramaDF = moviesDF
    .where("Major_Genre = 'Drama' AND IMDB_Rating > 6.0")


  val dramaRDD = moviesRDD
    .filter(movie =>
      movie.genre == "Drama"
        && movie.rating > 6.0
    )

  dramaDF.show()
  println(s"Drama DF count: ${dramaDF.count()}")
  dramaRDD.toDF().show()
  println(s"Drama RDD count: ${dramaRDD.count()}")


  /**
   * 4. Show the average rating of movies by genre.
   */

  cleanMovies
    .groupBy("genre")
    .agg(avg("rating"))
    .show()


  moviesRDD
    .groupBy(_.genre)
    .map(tuple => (tuple._1, tuple._2.map(row => row.rating)))
    .foreach(tuple => println(
      s"${tuple._1}: ${calcAverage(tuple._2)}"
    ))

  def calcAverage(list: Iterable[Double]) = list.sum / list.size.toDouble

  /**
   * Daniel's Solution
   */

  case class AverageRating(genre: String, rating: Double)

  moviesRDD
    .groupBy(_.genre)
    .map {
      case (genre, movies) => AverageRating(genre,
        movies.map(_.rating).sum / movies.size)
    }
    .toDF().show()
}
