package section3

import org.apache.spark.sql.{Dataset, Encoder, Encoders, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object DataSets extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.4 - Data Sets")
    .config("spark.master", "local")
    .getOrCreate()


  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")


  /**
   * Data Frame to Data Set
   *
   * - Manipulate values as Scala Types and Objects (distributed collection of JVM objects)
   * - Type Safety
   * - Leverage Scala's Functional resources.
   * - Bad for performance: Spark can't optimize Data Set transformations.
   */

  numbersDF
    .where(col("numbers") < 100)
  //    .show()


  /**
   * Or: Transform it to a Data Set
   */


  //  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  //  val numbersDataSetV2 = numbersDF.as
  //  numbersDataSetV2
  //    .filter(_ < 100)
  //    .show()


  /**
   * Using Spark's implicits
   */

  import spark.implicits._

  val numbersDataSet: Dataset[Int] = numbersDF.as[Int]
  numbersDataSet
    .filter(_ < 100)
  //    .show()


  /**
   * For Data Frames with Multiple Columns and Complex Types
   *
   * 1. Define a case class (easiest)
   * with a field for each row of the Data Frame (like a schema)
   * 2. Transform the Data Frame with an (implicit) Encoder
   */


  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  // Horsepower: Either[Long, Null],
                  // Either[] doesn't work here because it tries to instantiate it.
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  //                  Year: Option[Date],
                  Origin: String,
                )

  val carsDF = getJsonDataFrame("cars")

  //  implicit val carEncoder: Encoder[Car] = Encoders.product[Car]
  val carsDataSet = carsDF.as[Car]


  /**
   * Collection functions
   *
   * - map, flatMap, fold, reduce, for comprehensions, etc.
   */

  carsDataSet
    .filter(_.Origin.matches("Europe|Japan"))
  //    .show()

  carsDataSet
    .map(_.Name.toUpperCase())
  //    .show()

  //  for (car <- carsDataSet) printf("%s, ", car.Origin)


  /**
   * Joins
   */

  case class
  Guitar(
          id: Long,
          model: String,
          make: String,
          guitarType: String,
        )

  val guitars = getJsonDataFrame("guitars").as[Guitar]

  case class
  Guitarist(
             id: Long,
             guitars: Seq[Long],
             name: String,
             band: Long,
           )

  val guitarists = getJsonDataFrame("guitarPlayers").as[Guitarist]


  case class
  Band(
        id: Long,
        name: String,
        hometown: String,
        year: Long,
      )

  val bands = getJsonDataFrame("bands").as[Band]


  /**
   * .join() Returns a Data Frame, looses type information.
   * .joinWith() Returns a tuple of Data Sets (maintains the objects separate).
   */


  val joinCondition = guitarists.col("band") === bands.col("id")
  val guitaristBands = guitarists
    .joinWith(bands,
      joinCondition,
      // Same join types as .join()
      "inner")
  guitaristBands.show()


  /**
   * Grouping
   *
   * .groupByKey()
   * .reduceGroups()
   * .mapValues()
   * .mapGroups()
   * .agg()
   * .cogroup()
   * .flatMapGroups()
   * .flatMapGroupsWithState()
   * (...)
   */

  val groupedCars = carsDataSet
    .groupByKey(_.Origin)
    .count()
  groupedCars.show()


  /**
   * Joins and Groups are Wide Transformations:
   * - They change the number of partition behind the Data Sets
   * - Require shuffles, bad for performance
   */


  def getJsonDataFrame(name: String): DataFrame = {
    spark.read
      // inferSchema parses all integers as Long
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$name.json")
  }
}
