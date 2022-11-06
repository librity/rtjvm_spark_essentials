package section3

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._


import java.sql.Date
import scala.math.Ordered.orderingToOrdered

object DataSetsExercise extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 3.4 - Data Sets Exercise")
    .config("spark.master", "local")
    .getOrCreate()


  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * Exercise 1
   *
   * 1. Create carsDataSet
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


  implicit val carEncoder: Encoder[Car] = Encoders.product[Car]
  val carsDataSet = carsDF.as[Car]


  /**
   * 2. Count how many cars we have
   */

  println(s"Cars Data Frame Count: ${carsDF.count()}")
  println(s"Cars Data Set Count: ${carsDataSet.count()}")


  /**
   * 3. Count how many POWERFUL cars we have (HP > 140)
   */

  val powerfulCarsDF = carsDF
    .where(col("Horsepower") > 140)

  val powerfulCarsDS = carsDataSet
    .filter(_.Horsepower match {
      case Some(hp) => hp > 140L
      case None => false
    })

  println(s"Powerful Cars Data Frame Count: ${powerfulCarsDF.count()}")
  println(s"Powerful Cars Data Set Count: ${powerfulCarsDS.count()}")

  /**
   * 4. Calculate the average HorsePower of the data set
   */


  val cleanCarsDS = carsDataSet
    .filter(_.Horsepower.isDefined)

  implicit val longEncoder: Encoder[Long] = Encoders.scalaLong
  implicit val doubleEncoder: Encoder[Double] = Encoders.scalaDouble
  val averageHPofDS = cleanCarsDS
    .map(_.Horsepower.getOrElse(0L).toDouble)
    .reduce(_ + _) / cleanCarsDS.count().toDouble

  println(s"Average Horse Power of Cars Data Frame:")
  carsDF
    .agg(avg("Horsepower"))
    .show()
  println(s"Average Horse Power of Cars Data Set: ${averageHPofDS}")


  /**
   * Daniel's Solution
   *
   * We can also use the Data Frame functions:
   * DataFrame  = Dataset[Row]
   */

  carsDataSet
    .select(avg("Horsepower"))
    .show()


  /**
   * Exercise 2
   */

  case class
  Guitar(
          id: Long,
          model: String,
          make: String,
          guitarType: String,
        )

  implicit val guitarEncoder = Encoders.product[Guitar]
  val guitars = getJsonDataFrame("guitars").as[Guitar]

  case class
  Guitarist(
             id: Long,
             guitars: Seq[Long],
             name: String,
             band: Long,
           )

  implicit val guitaristEncoder = Encoders.product[Guitarist]
  val guitarists = getJsonDataFrame("guitarPlayers").as[Guitarist]


  /**
   * 1. Join guitarists with guitars by guitarists.col("guitars")
   * - use array_contains() and outer join
   */

  val joinCondition = array_contains(
    guitarists.col("guitars"),
    guitars.col("id"))
  guitarists.joinWith(
    guitars,
    joinCondition,
    "outer",
  )
    .show()


  def getJsonDataFrame(name: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$name.json")
  }
}
