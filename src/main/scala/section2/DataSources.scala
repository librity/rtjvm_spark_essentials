package section2

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


  //  Load JSON
  val carsSchemaV2 = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val carsDFV2 = sparkSession.read
    .schema(carsSchemaV2)
    // dateFormat only works with enforced schema
    .option("dateFormat", "YYYY-MM-dd")
    // Dates that don't conform to the format will be null
    .option("allowSingleQuotes", "true")
    // compression: bzip2, gzip, lz4, snappy, deflate
    .option("compression", "uncompressed") // default
    .json("src/main/resources/data/cars.json")

  carsDFV2.printSchema()
  carsDFV2.show()


  //  Load CSV
  //  CSVs are tricky to load due to separators and formatting

  //  Allows parsing "MMM dd YYYY"
  sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  var stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType),
  ))

  val stocksDF = sparkSession.read
    .schema(stocksSchema)
    .options(Map(
      "dateFormat" -> "MMM dd YYYY",
      // Will validate column names based on your schema
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> "",
    ))
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.printSchema()
  stocksDF.show()


  /**
   * Save as Parquet
   * - Open-source column-oriented data format
   * - Efficient data storage and retrieval
   * - Up to 10 times less space than json
   * - Spark's default storage format for data frames
   * - Very predictable, doesn't require many options to load
   */

  carsDataFrame.write
    .mode(SaveMode.Overwrite)
    // .parquet("src/main/resources/data/cars.parquet")
    // Parquet is default.
    .save("src/main/resources/data/cars.parquet")
    // Default compression: snappy


  //  Load Text Files
  //  Each line becomes a row.
  val textDataFrame = sparkSession.read.text("src/main/resources/data/sample.txt")

  textDataFrame.printSchema()
  textDataFrame.show()


  //  Load from Remote Data Base
  val employeesDF = sparkSession.read
    // Java Database Connectivity
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

}
