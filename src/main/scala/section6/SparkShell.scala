package section6

import org.apache.spark.sql.SparkSession

object SparkShell {
  /**
   * Boilerplate
   */

  val spark = SparkSession.builder()
    .appName("Lesson 6.1 - Spark Shell")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
   * Run this in the master node's Spark Shell: /spark/bin/spark-shell
   */

  val rdd1 = sc.parallelize(1 to 1000000)

  /**
   * There should be a new job in http://localhost:4040/jobs
   *
   * Jobs have Stages, Stages have Tasks
   * - Stage = A set of computations between shuffles
   * - Task = A unit of computation per RDD's partition
   */
  rdd1.count()
  rdd1.getNumPartitions


  /**
   * .map() is Lazy
   * .count() is Eager
   *
   * Directed Acyclical Graph (DAG) maps dependencies between RDDs
   */
  val rdd2 = rdd1.map(_ * 2)
  rdd2.count()


  /**
   * Forcing a Full Shuffle
   */
  rdd1.repartition(23).count()


  /**
   * RDD.toDF()
   *
   * .toDF() is lazy
   * .show() is an action
   *
   *
   */
  rdd1.toDF().show()
  rdd1.toDF().count()

  rdd1.repartition(42).toDF.show()
  rdd1.repartition(42).toDF.count()

  /**
   * You lose control of the computations on your data
   * when using Spark's High-level APIs
   *
   * spark.range() is lazy
   */
  val ds1 = spark.range(1, 1000000, 2)
  ds1.show()

  /**
   * .explain() Prints the Physical Plan,
   * or all the operations that spark will do to compute the data set.
   */
  ds1.explain()

  /**
   * With a shuffle:
   */
  val ds2 = spark.range(1, 1000000, 5)
  val ds3 = ds1.repartition(7)
  ds2.explain()
  ds3.explain()

  val ds4 = ds2.repartition(9)
  ds4.explain()


  /**
   * With a select:
   */
  val ds5 = ds3.selectExpr("id * 5 as id")
  ds5.explain()

  /**
   * With a join:
   */
  val joined = ds5.join(ds4, "id")
  joined.explain()


  /**
   * With $"sum()"
   *
   * Aggregation functions like $"sum()" shuffle: HashAggregate
   */
  val sum = joined.selectExpr("sum(id)")
  sum.explain()
  sum.show()


  /**
   * .explain(extended = true) Prints all the Plans:
   * - Parsed Logical Plan: initial version
   * - Analyzed Logical Plan: Resolves expression functions and arguments
   * - Optimized Logical Plan: Optimizes wherever it can
   * - Physical Plan: The actual set of operations executed on the cluster
   */
  sum.explain(true)

  /**
   * With Data Frames
   *
   * We can see Spark skipping many stages in the Web View
   */
  val df1 = sc.parallelize(1 to 1000000).toDF().repartition(7)
  val df2 = sc.parallelize(1 to 100000).toDF().repartition(9)
  val df3 = df1.selectExpr("value * 5 as value")

  val joined2 = df3.join(df2, "value")
  val sum2 = joined2.selectExpr("sum(value)")

  sum2.show()
  sum2.explain(true)


}

