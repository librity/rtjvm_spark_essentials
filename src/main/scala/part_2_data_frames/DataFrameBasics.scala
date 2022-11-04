package part_2_data_frames

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App {

  //  Create spark session
  val spark = SparkSession.builder()
    .appName("Lesson 2.1 - Data Frames Basics")
    .config("spark.master", "local")
    .getOrCreate()
  val sparkContext = spark.sparkContext
  sparkContext.setLogLevel("WARN")

  //  Read a data frame
  val carsDataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")
  // Show data frame
  carsDataFrame.show()
  carsDataFrame.printSchema()

  //  Data Frame: A schema + rows conforming to that structure, distributed over a cluster.
  carsDataFrame.take(10).foreach(println)

  //  Spark Types: run-time types implemented as case objects
  val longType = LongType

  /**
   * Data Frame Schemas
   * Defining your own schemas => Good practice
   * Avoid inferSchema:
   * - Dates might get parsed wrong
   */
  val myCarsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))
  println(myCarsSchema)

  val carsDFSchema = carsDataFrame.schema
  println(carsDFSchema)


  var carsDataFrameMySchema = spark.read.format("json")
    .schema(myCarsSchema)
    .load("src/main/resources/data/cars.json")
  carsDataFrameMySchema.show()


  //  Manually creating Data Frames
  //  Create a row
  val myRow = Row(
    "amc rebel sst", 16.0, 8, 304.0, 150, 3433, 12.0, "1970-01-01", "USA"
  )

  //  Create a Data Frame from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
    ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
    ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
    ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
    ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
    ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
    ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
  )
  //  Schema auto-inferred from types
  val manualCarsDataFrame = spark.createDataFrame(cars)
  manualCarsDataFrame.show()
  //  Data Frames have Schemas, Rows do not


  //  Create Data Frames with implicits

  import spark.implicits._

  val manualCarsDataFrameWImplicits = cars.toDF("Name",
    "Miles_per_Gallon",
    "Cylinders",
    "Displacement",
    "Horsepower",
    "Weight_in_lbs",
    "Acceleration",
    "Year",
    "Origin")
  manualCarsDataFrame.printSchema()
  manualCarsDataFrameWImplicits.printSchema()
  
}
