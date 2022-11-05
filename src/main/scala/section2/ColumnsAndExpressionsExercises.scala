package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._

object ColumnsAndExpressionsExercises extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.7 - Columns And Expressions Exercises")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
   * Exercise 1
   * - [x] Read movies.json as a Data Frame
   * - [x] Project 2 columns of your choice
   * - [x] Add a Revenue column with "Worldwide_Gross + US_DVD_Sales"
   * - [x] Add a Profit column with "Revenue - Production_Budget"
   * - [x] Select all movies with comedy as Major_Genre and an IMDB rating >= 6
   * - [x] Try and do each exercise in a different way (filter, where, symbols, interpolated strings, expressions, etc.)
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
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = spark.read
    .schema(moviesSchema)
    .option("dateFormat", "dd-MMM-YY")
    .json("src/main/resources/data/movies.json")
  moviesDF.show()

  import spark.implicits._

  moviesDF.selectExpr("Distributor", "Director").show()
  moviesDF.select($"Title", 'US_DVD_Sales).show()
  moviesDF.select(moviesDF.col("US_Gross"), column("Creative_Type")).show()


  val filledMovies = moviesDF.na.fill(0)

  val financialReport = filledMovies
    .withColumn(
      "Revenue",
      expr("Worldwide_Gross + US_DVD_Sales")
    )
    .withColumn(
      "Profit",
      col("Revenue") - col("Production_Budget")
    )
  financialReport.show()

  filledMovies.select(
    filledMovies.col("Title"),
    col("Worldwide_Gross"),
    column("US_Gross"),
    $"US_DVD_Sales",
    (col("Worldwide_Gross") + col("US_DVD_Sales")).as("Revenue"),
    'Production_Budget,
    expr("Worldwide_Gross + US_DVD_Sales - Production_Budget AS Profit")
  ).show()

  filledMovies.selectExpr(
    "Title",
    "Worldwide_Gross + US_DVD_Sales AS Revenue",
    "Production_Budget AS Cost",
    "Worldwide_Gross + US_DVD_Sales - Production_Budget AS Profit",
  ).show()


  val goodComedies = moviesDF
    .where("Major_Genre = 'Comedy'")
    .filter(moviesDF.col("IMDB_Rating") >= 6.0)
  goodComedies.show()

  moviesDF
    .select($"Title", 'IMDB_Rating)
    .where(
      col("Major_Genre") === "Comedy"
        and expr("IMDB_Rating >= 6.0"))
    .show()

  moviesDF
    .select($"Title", col("Rotten_Tomatoes_Rating"))
    .where("Major_Genre = 'Comedy' and IMDB_Rating >= 6.0")
    .show()
}
