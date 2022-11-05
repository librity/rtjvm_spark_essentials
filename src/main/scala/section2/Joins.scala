package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.9 - Joins")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")
  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  val guitaristDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  bandsDF.show()
  guitarsDF.show()
  guitaristDF.show()


  /**
   * Inner Join
   */

  // Isolate Expressions and Coditions: Good practice
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")

  // guitaristDF: left table
  // bandsDF: right table
  val guitaristBands = guitaristDF.join(
    bandsDF,
    joinCondition,
    // Inner Join: discards rows that don't match the condition (The Beatles and Eric Clapton).
    "inner"
    // Default value.
  )
  guitaristBands.show()


  /**
   * Left Outer Join
   */
  guitaristDF.join(
    bandsDF,
    joinCondition,
    // Contains everything in the Inner Join
    // + All the rows in the Left Table (bandsDF) with nulls for missing data.
    "left_outer"
  ).show()

  /**
   * Right Outer Join
   */
  guitaristDF.join(
    bandsDF,
    joinCondition,
    // Contains everything in the Inner Join
    // + All the rows in the Right Table (guitaristDF) with nulls for missing data.
    "right_outer"
  ).show()

  /**
   * Outer Join
   */
  guitaristDF.join(
    bandsDF,
    joinCondition,
    // Contains everything in the Inner Join
    // + All the rows in the both Tables with nulls for missing data.
    "outer"
  ).show()


  /**
   * Left Semi-Join
   */
  guitaristDF.join(
    bandsDF,
    joinCondition,
    // Discards the rows of the Left Table (guitaristDF) that don't satisfy the condition.
    "left_semi"
  ).show()


  /**
   * Left Anti-Join
   */
  guitaristDF.join(
    bandsDF,
    joinCondition,
    // Keeps the rows of the Left Table (guitaristDF) that don't satisfy the condition.
    "left_anti"
  ).show()

  /**
   * There's no Right Semi-Join or Anti-Join in Spark
   */


  /**
   * Joins might return a Data Frame with two columns with the same name:
   */
  // guitaristBands.select("id", "band").show()
  // Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.;

  // Option 1: Rename column
  guitaristDF.join(
    bandsDF.withColumnRenamed("id", "band_id"),
    expr("id = band_id"),
  )
    .select("id", "band")
    .show()




  // Option 2: Drop duplicate column
  guitaristBands
    .drop(bandsDF.col("id"))
    .select("id", "band")
    .show()
  // Spark keeps an internal identifier of each column.


  // Option 3: Rename the colum and keep the data
  val renamedBands = bandsDF
    .withColumnRenamed("id", "band_id")
    .withColumnRenamed("name", "band_name")
  val renamedJoinCond = guitaristDF.col("band") === renamedBands.col("band_id")
  val renamedGuitaristBands = guitaristDF
    .join(
      renamedBands,
      renamedJoinCond,
      "inner"
    )

  renamedGuitaristBands
    .select("id", "band_id")
    .show()

  renamedGuitaristBands
    .select("name", "band_name")
    .show()


  /**
   * Joins with Complex Types
   *
   * You can join on an array and other types with expressions:
   */

  val renamedGuitars = guitarsDF.withColumnRenamed("id", "guitar_id")
  guitaristDF.join(
    renamedGuitars,
    expr("array_contains(guitars, guitar_id)")
  ).show()

  guitaristDF.join(
    renamedGuitars,
    array_contains(
      guitaristDF.col("guitars"),
      renamedGuitars.col("guitar_id"))
  ).show()


}
