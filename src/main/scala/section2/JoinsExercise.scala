package section2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JoinsExercise extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.10 - Joins Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * Exercises
   *
   * - [x] Read tables from the Postgres DB: employees, salaries, titles and dept_managers
   * - [x] Show all employees and their max salary
   * - [x] Show all employees who were never managers
   * - [x] Find the latest job title of the top 10 paid employees
   */


  /** ***************************************************************************\
   * Read tables from the Postgres DB: employees, salaries, titles and dept_managers
   * ************************************************************************** */

  val employeesDF = getTable("employees")
  val salariesDF = getTable("salaries")
  val deptManagersDF = getTable("dept_manager")
  val titlesDF = getTable("titles")


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

    table.show()
    println(s"Size: ${table.count()}")

    table
  }


  /** ***************************************************************************\
   * Show all employees and their max salary
   * ************************************************************************** */


  var joinCond = employeesDF.col("emp_no") === salariesDF.col("emp_no")
  val maxSalaries = employeesDF.join(salariesDF, joinCond)
    .groupBy(employeesDF.col("emp_no"))
    .max("salary")


  joinCond = employeesDF.col("emp_no") === maxSalaries.col("emp_no")
  val employeesByMaxSalary = employeesDF.join(maxSalaries, joinCond)
    .drop(maxSalaries.col("emp_no"))
    .select("emp_no", "first_name", "last_name", "max(salary)")
  employeesByMaxSalary.show()


  /**
   * Daniel's Solution
   */

  val maxSalariesPerEmp = salariesDF
    .groupBy("emp_no")
    .max("salary")
  val employeesSalaries = employeesDF
    .join(
      maxSalariesPerEmp,
      "emp_no",
    )
  employeesSalaries.show()


  /** ***************************************************************************\
   * Show all employees who were never managers
   * ************************************************************************** */


  //  Get manager ids.
  deptManagersDF.show()
  employeesDF
    .join(
      deptManagersDF,
      "emp_no"
    )
    .select("emp_no", "first_name", "last_name")
    .show()


  joinCond = employeesDF.col("emp_no") === deptManagersDF.col("emp_no")
  employeesDF.join(
    deptManagersDF,
    joinCond,
    "left_anti",
  )
    .select("emp_no", "first_name", "last_name")
    .show()


  /** ***************************************************************************\
   * Find the latest job title of the top 10 paid employees
   * ************************************************************************** */


  val topTen = employeesByMaxSalary
    .orderBy(col("max(salary)").desc)
    .limit(10)

  joinCond = topTen.col("emp_no") === titlesDF.col("emp_no")
  topTen.join(titlesDF, joinCond)
    .drop(titlesDF.col("emp_no"))
    .orderBy($"to_date".desc)
    .limit(10)
    .select("emp_no", "last_name", "first_name", "title", "max(salary)")
    .show()


  /**
   * Daniel's Solution
   */

  val mostRecentTitles = titlesDF
    .groupBy("emp_no", "title")
    .agg(max("to_date"))
  mostRecentTitles.show()

  val tenBestPaid = employeesSalaries
    .orderBy($"max(salary)".desc)
    .limit(10)
  tenBestPaid.show()

  val bestPaidJobs = tenBestPaid
    .join(
      mostRecentTitles,
      "emp_no"
    )
  bestPaidJobs.show()


}


