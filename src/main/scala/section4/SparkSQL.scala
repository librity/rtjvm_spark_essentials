package section4

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSQL extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 4.2 - Spark SQL")
    .config("spark.master", "local")
    // Set SparkSQL's warehouse folder:
    .config("spark.sql.warehouse.dir", "src/main/resources/spark-warehouse")
    // Deprecated in Spark 3.0.0
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")


  /**
   * .select().where()
   */
  carsDF
    .select(col("Name"))
    .where(col("Origin") === "USA")
  //    .show()


  /**
   * With Spark SQL:
   *
   * Create an alias for the Data Frame so we can refer to as a table
   */
  carsDF
    .createOrReplaceTempView("cars")

  /**
   * Query it as you would any other SQL table
   */
  spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)
  //    .show()


  /**
   * Will create a spark-warehouse folder in the execution directory
   */

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")

  val databases = spark.sql("show databases")
  databases.show()


  /**
   * Transfer and save tables from a Data Base to Spark Tables;
   */


  val employees = getTable("employees")

  //  Delete src/main/resources/spark-warehouse before running.
  //  employees.write
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("employees")

  //  transferTables(List(
  //    "departments",
  //    "dept_emp",
  //    "dept_manager",
  //    "employees",
  //    "movies",
  //    "salaries",
  //    "titles",
  //  ))


  /**
   * Read Data Frame from a Loaded Table
   */

  employees.createOrReplaceTempView("employees")
  val employeesV2 = spark.read.table("employees")
  //  employeesV2.show()


  def transferTables(tableNames: List[String]): Unit = tableNames.foreach { tableName =>
    val table = getTable(tableName)
    table.createOrReplaceTempView(tableName)

    table.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)

  }

  def getTable(name: String): DataFrame = {
    val postgresConfig = Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
    )

    val table = spark.read
      .format("jdbc")
      .option("driver", postgresConfig("driver"))
      .option("url", postgresConfig("url"))
      .option("user", postgresConfig("user"))
      .option("password", postgresConfig("password"))
      .option("dbtable", s"public.$name")
      .load()

    //    table.show()
    //    println(s"Size: ${table.count()}")

    table
  }

}
