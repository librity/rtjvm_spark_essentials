package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("Lesson 2.6 - Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.show()


  /**
   * Create columns
   */

  //  Get the names column
  val namesColumn = carsDF.col("Name")
  //  Project (Select) the column
  val carNamesDF = carsDF.select(namesColumn)
  //  Data Frames are immutable (functional programming and RDD):
  //  We always get a new data frame.
  carNamesDF.show()



  //  Project multiple columns

  import spark.implicits._

  val reducedCarsDF = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Horsepower"),
    'Year, // Scala Symbol (like ruby), auto-convert to column
    $"Weight_in_lbs", // Interpolated string
    expr("Origin") // Expression
  )
  reducedCarsDF.show()

  val reducedCarsDFV2 = carsDF.select("Name", "Horsepower")
  reducedCarsDFV2.show()

  /**
   * Spark Expressions
   * - Like `eval()` but for manipulating data.
   */


  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carWeights = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    // Very common:
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_v2"),
  )
  carWeights.show()

  //  Shorthand: selecExpr()
  val carWeightsV2 = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 AS Weight_in_kg",
  )
  carWeightsV2.show()


  /**
   * Data Frame Processing
   */

  // Add a Column
  val carsWithKg = carsDF.withColumn(
    "Weight_in_kg",
    col("Weight_in_lbs") / 2.2
  )
  carsWithKg.show()

  // Rename a Column
  val carsRenamed = carsDF.withColumnRenamed(
    "Weight_in_lbs",
    "Weight in pounds"
  )
  carsRenamed.show()

  // Columns names with spaces or hyphens are not recommended
  carsRenamed.selectExpr("`Weight in pounds`").show()

  // Remove a Column
  val carsRemoved = carsDF.drop("Cylinders", "Displacement")
  carsRemoved.show()



  //  Filtering

  val foreignCars = carsDF.filter(col("Origin") =!= "USA")
  foreignCars.show()
  val foreignCarsV2 = carsDF.where(col("Origin") =!= "USA")
  foreignCarsV2.show()

  val americanCars = carsDF.filter("Origin = 'USA'")
  americanCars.show()
  val americanCarsV2 = carsDF.filter(col("Origin") === "USA")
  americanCarsV2.show()


  val muscleCars = carsDF.filter(col("Origin") === "USA").where(col("Horsepower") > 150)
  muscleCars.show()
  val muscleCarsV2 = carsDF.filter(
    col("Origin") === "USA"
      and col("Horsepower") > 150
  )
  muscleCarsV2.show()
  val muscleCarsV3 = carsDF.where("Origin = 'USA' and Horsepower > 150")
  muscleCarsV3.show()


  // Unioning: Adding rows to Data Frame
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  // Data frams must have the same schema;
  val allCarsDF = carsDF.union(moreCarsDF)
  allCarsDF.show()


  //  Distinct Values
  val allCountries = allCarsDF.select("Origin").distinct()
  allCountries.show()
}
