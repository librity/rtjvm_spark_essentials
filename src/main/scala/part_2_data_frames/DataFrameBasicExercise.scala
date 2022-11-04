package part_2_data_frames

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object DataFrameBasicExercise extends App {

  val sparkSession = SparkSession.builder()
    .appName("Lesson 2.2 - Data Frames Basics Exercise")
    .config("spark.master", "local")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("WARN")

  /**
   * Exercise 1
   * - [x] Create a manual DF describing smartphones
   * - [x] make
   * - [x] model
   * - [x] screen size
   * - [x] camera megapixels
   * - [x] etc.
   * - [x] Print data frame and Schema.
   */
  val phonesSchema = StructType(
    Array(
      StructField("Make", StringType, nullable = false),
      StructField("Model", StringType, nullable = false),
      StructField("OS", StringType, nullable = false),
      StructField("Screen Size (inches)", DoubleType, nullable = false),
      StructField("Main Camera Megapixels", LongType, nullable = false),
      StructField("RAM GB", LongType, nullable = false),
      StructField("Battery mAh", IntegerType, nullable = false),
      StructField("Release Date", StringType, nullable = false),
    )
  )


  //  SOURCE: https://phonesdata.com/en/
  val phones = Seq(
    Row("Xiaomi", "Redmi 9T", "Android 10", 6.53, 48L, 4L, 6000, "2021/01/08"),
    Row("Samsung", "Galaxy A12", "Android 10", 6.5, 48L, 3L, 5000, "2020/11/24"),
    Row("Huawei", "P20 Pro", "Android 8.1", 6.1, 40L, 8L, 4000, "  2018/03/01"),
    Row("Apple", "iPhone 14", "iOS 16", 6.1, 12L, 6L, 3279, "2022/09/07"),
  )
  //  Data must match schema or it will error out when evaluated! (lazy)
  //  Transformation (map(), etc.) are only executed when actions are called (show(), count(), etc.)

  val phonesRows = sparkContext.parallelize(phones)
  val phonesDataFrame = sparkSession.createDataFrame(phonesRows, phonesSchema)

  println("=============== SCHEMA ===============")
  phonesDataFrame.printSchema()
  println("=============== DATA FRAME ===============")
  phonesDataFrame.show()

  /**
   * Exercise 2
   * - [x] Read another file from the "src/main/resources/data"
   * - [x] Manually define the schema (recommended)
   * - [x] print its schema
   * - [x] print the number of rows (.count())
   */


  val guitarsSchema = StructType(
    Array(
      StructField("id", IntegerType, nullable = false),
      StructField("model", StringType, nullable = false),
      StructField("make", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
    )
  )

  val guitarsDF = sparkSession.read
    .format("json")
    .schema(guitarsSchema)
    .load("src/main/resources/data/guitars.json")

  println("=============== SCHEMA ===============")
  guitarsDF.printSchema()
  println("=============== DATA FRAME ===============")
  guitarsDF.show()
  println(s"ROW COUNT: ${guitarsDF.count()}")
  println("======================================")

}
