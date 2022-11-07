package section6

import org.apache.spark.sql.{SaveMode, SparkSession}

object DeployApp {
  def main(args: Array[String]): Unit = {

    /**
     * With args(0) = movies.json and args(1) = good_comedies.json:
     *
     * 1. Read file args(0)
     * 2. Select all comedies with rating > 6.5
     * 3. Save as args(1)
     */

    if (args.length != 2) {
      println("ERROR: Need input and output file paths.")
      System.exit(1)
    }

    /**
     * Boilerplate
     */

    val spark = SparkSession.builder()
      .appName("Lesson 6.2 - Deploy a Spark App")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    /**
     * Read input Data Frame, filter and save.
     */

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComedies = moviesDF.select(
      $"Title".as("title"),
      $"IMDB_Rating".as("rating"),
      $"release_date".as("release"),
    )
      .where(
        $"Major_Genre" === "Comedy"
          and $"IMDB_Rating" > 6.5
      )
      .orderBy($"rating".desc_nulls_last)
    goodComedies.show()

    goodComedies.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))

  }
}
