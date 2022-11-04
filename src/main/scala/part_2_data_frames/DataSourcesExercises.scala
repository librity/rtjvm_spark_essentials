package part_2_data_frames

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourcesExercises extends App {
  val sparkSession = SparkSession.builder()
    .appName("Lesson 2.5 - Data Sources Exercise")
    .config("spark.master", "local")
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("WARN")


  /**
   * Exercise 1
   * - [x] Read movies.json as a Data Frame
   * - [x] Write it as a tab-separated values file
   * - [x] Write it as a snappy Parquet
   * - [x] Write it in the PostgreSQL DB as public.movies
   */

  val moviesSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", IntegerType),
    StructField("Worldwide_Gross", IntegerType),
    StructField("US_DVD_Sales", IntegerType),
    StructField("Production_Budget", IntegerType),
    StructField("Release_Date", DateType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min", IntegerType),
    StructField("Distributor", StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", IntegerType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", IntegerType),
  ))


  // Parse month names in English ("MMM")
  sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = sparkSession.read
    .schema(moviesSchema)
    .option("dateFormat", "dd-MMM-YY")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()
  println(s"Total Movies: ${moviesDF.count()}")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .option("nullValue", "")
    .csv("src/main/resources/data/movies.tsv")


  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")


  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()


}
