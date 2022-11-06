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

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")


  /**
   * With .regex_extract()
   */


  val namesRegEx = getCarNames
    .map(_.toLowerCase())
    //     .reduce((left, right) => s"$left|$right")
    .mkString("|")
  println(namesRegEx)

  carsDF
    .select(
      $"Name",
      regexp_extract(
        $"Name",
        namesRegEx,
        0
      ).as("regex_extract")
    )
    .where($"regex_extract" =!= "")
    .drop("regex_extract")
    .show()

  /**
   * With .contains()
   */

  val carNameFilter = getCarNames
    .map(_.toLowerCase())
    .map(
      carName => col("Name").contains(carName)
    )
  val bigFilter = carNameFilter
    .fold(lit(false))(
      (combined, next) => combined or next
    )

  carsDF
    .filter(bigFilter)
    .show()


}
