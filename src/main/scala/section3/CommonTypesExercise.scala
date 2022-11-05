package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypesExercise extends App {

  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.1 - Common Types Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")


  /**
   * Exercise
   *
   * Filter the carsDF by a list of car names obtained by an API call getCarNames()
   */

  def getCarNames: List[String] = ???

  /**
   * With .contains()
   */


  /**
   * With .regex_extract()
   */


}
