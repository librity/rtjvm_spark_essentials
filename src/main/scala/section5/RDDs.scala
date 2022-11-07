package section5

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  /**
   * Spark Context:
   *
   * Entry point for low-level RDD APIs
   */

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
   * RDD by parallelizing an existing collection
   */

  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)
  //  numbersRDD.foreach(println(_))


  /**
   * CSV to RDD
   */

  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(fileName: String) =
    Source
      .fromFile(s"src/main/resources/data/$fileName.csv")
      .getLines()
      // Drop the header
      .drop(1)
      .map(_.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksList = readStocks("stocks")
  val stocksRDD = sc.parallelize(stocksList)
  //  stocksRDD.foreach(println(_))


  /**
   * Another way:
   */


  val stocksRDDV2 = sc.textFile("src/main/resources/data/stocks.csv")
    // Drop the header
    .filter(_ != "symbol,date,price")
    .map(_.split(","))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
  //  stocksRDDV2.foreach(println(_))


  /**
   * Data Frame to RDD
   */

  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")
  val stocksDS = stocksDF.as[StockValue]

  /**
   * RDD[StockValue] Maintain type information
   */
  val stocksRDDV3 = stocksDS.rdd

  /**
   * RDD[Row] Lose type information
   */
  val stocksRDDV4 = stocksDF.rdd


  /**
   * RDD to Data Frame
   *
   * Lose type information
   */
  val numbersDF = numbersRDD.toDF("numbers")

  /**
   * RDD to Data Set
   *
   * Maintain type information
   */
  val numbersDS = spark.createDataset(numbersRDD)


  /**
   * RDD vs. Data Set
   *
   * Things in common:
   * - Collection API: .map(), .flatMap(), .reduce(), .take(), .filter(), etc.
   * - .union(), .count(), .distinct()
   * - .groupBy(), .sortBy()
   *
   * RDDs over Datasets
   * - partition control: .repartition(), .coalesce(), .partitioner(), .zipPartitions(), .mapPartitions()
   * - operation control: .checkpoint(), .isCheckpointed(), .localCheckpoint(), .cache()
   * - storage control: .cache(), .()getStorageLevel, .persist()
   *
   * Datasets over RDDs
   * - .select() and .join()!
   * - Spark planning/optimization before running code
   */

  /**
   * Transformations
   */

  /**
   * Counting
   *
   * .filter() is lazy
   * .count() is eager
   */

  val msRDD = stocksRDD
    .filter(_.symbol == "MSFT")
  val msCount = msRDD
    .count()
  //  println(s"Microsoft Count: $msCount")


  /**
   * .distinct() is lazy
   */

  val companyNamesRDD = stocksRDD
    .map(_.symbol)
    .distinct()
  //    .foreach(println(_))


  /**
   * Min and Max
   *
   * Both are actions since they reduce the RDD to a single value.
   */

  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue](
      (left: StockValue, right: StockValue) => left.price < right.price)
  //  println(s"Microsoft Min and Max: ${msRDD.min().price}, ${msRDD.max().price}")


  /**
   * Reduce
   */

  //  println(s"Numbers Sum: ${numbersRDD.reduce(_ + _)}")


  /**
   * Grouping
   *
   * Very expensive for Data Frames, Sets and RDDs
   */

  val groupedStocksRDD = stocksRDD
    .groupBy(_.symbol)
  //  groupedStocksRDD.foreach(println(_))


  /**
   * Repartitioning
   *
   * Involves Shuffling, Very Expensive.
   * Best practice: repartition before processing, as EARLY as possible.
   * Optimal size of a partition should be between 10-100MB in size.
   */

  val repartitionedRDD = stocksRDD.repartition(30)

  // Will create 30 partition parquets.
  //  repartitionedRDD.toDF().write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks30")


  /**
   * Coalesce
   *
   * Reduces the number of partitions.
   * Does not necessarily involve shuffling.
   */

  val coalescedRDD = repartitionedRDD.coalesce(15)
  coalescedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")


}
