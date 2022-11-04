package part_2_data_frames

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  val sparkSession = SparkSession.builder()
    .appName("Lesson 2.4 - Data Sources")
    .config("spark.master", "local")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("WARN")


  val carsSchema = StructType(Array(
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

  /**
   * Reading a Data Frame:
   * - Format
   * - Schema
   */
  val carsDataFrame = sparkSession.read
    .format("json")
    //    .option("inferSchema", "true")
    .schema(carsSchema)
    // failFast: will crash if any record doesn't match the schema when evaluated (action called)
    .option("mode", "failFast")
    // Other Options: dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDataFrame.show()

  val carsDFWithOptionsMap = sparkSession.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()
  carsDFWithOptionsMap.show()

  /**
   * Writing Data Frames
   * - format
   * - save mode: overwrite, append, ignore, errorIfExists
   * - path
   * - zero or more options
   */
  carsDataFrame.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()
}
