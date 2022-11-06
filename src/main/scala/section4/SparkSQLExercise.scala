package section4

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Date

object SparkSQLExercise extends App {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 4.2 - Spark SQL Exercise")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/spark-warehouse")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
   * Load and Expose Tables
   */

  spark.sql("CREATE DATABASE rtjvm")
  spark.sql("USE rtjvm")

  val employees = getTable("employees")
  val departments = getTable("departments")
  val dept_emp = getTable("dept_emp")
  val salaries = getTable("salaries")

  //  spark.sql("SHOW TABLES").show()

  /**
   * HINT: SQL Aliases
   */

  //  spark.sql(
  //    """
  //      |select *
  //      |from employees e, dept_emp d
  //      |where e.emp_no = d.emp_no
  //  """.stripMargin
  //  )


  /**
   * Exercise 1 - With SparkSQL!!!
   *
   * 1. Read movies.json and store it as a Spark Table in the rtjvm database.
   */


  val databases = spark.sql("show databases")
  //  databases.show()


  val movies = getJsonDataFrame("movies")
  movies.createOrReplaceTempView("movies")

  //  movies.write
  //    .mode(SaveMode.Overwrite)
  //    .saveAsTable("movies")


  /**
   * 2. Count how many employees were hired between January 1st 1999 and January 1st 2001
   */


  spark.sql(
    """
      |SELECT *
      |   FROM employees emp
      |   ORDER BY emp.hire_date DESC;
    """.stripMargin)
  //    .show()

  val targetEmployees = spark.sql(
    """
      |SELECT *
      |   FROM employees emp
      |   WHERE
      |     emp.hire_date >= date('1999-01-01')
      |     AND emp.hire_date <= date('2001-01-01')
      |   ORDER BY emp.hire_date DESC;
    """.stripMargin)
  //    targetEmployees.show()
  targetEmployees.createOrReplaceTempView("target_employees")

  spark.sql(
    """
      |SELECT count(*)
      |   FROM target_employees
    """.stripMargin)
  //    .show()

  val employeeDFCount = employees
    .where(col("hire_date") >= Date.valueOf("1999-01-01"))
    .where(col("hire_date") <= Date.valueOf("2001-01-01"))
    .count()
  //  println(s"Employee Data Frame Count: $employeeDFCount")


  /**
   * 3. Show the average salaries for the employees above grouped by department number
   */


  val targetDepartments = spark.sql(
    """
      |SELECT dept_no, avg(sal.salary) AS avg_salary
      |    FROM target_employees emp
      |    INNER JOIN salaries sal
      |        ON emp.emp_no = sal.emp_no
      |    INNER JOIN dept_emp
      |        ON emp.emp_no = dept_emp.emp_no
      |    GROUP BY dept_no
      |    ORDER BY dept_no
    """.stripMargin)
  targetDepartments.createOrReplaceTempView("target_deps")

  //  targetDepartments.show()

  /**
   * 4. Show the name of the best-paying department for employees hired between those dates
   */

  spark.sql(
    """
      |SELECT tdep.dept_no, dept_name, avg_salary
      |    FROM target_deps tdep
      |    INNER JOIN departments dep
      |        ON tdep.dept_no = dep.dept_no
      |    ORDER BY avg_salary DESC
    """.stripMargin)
    .show()

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
    table.createOrReplaceTempView(name)

    table
  }

  def getJsonDataFrame(name: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$name.json")
  }
}
