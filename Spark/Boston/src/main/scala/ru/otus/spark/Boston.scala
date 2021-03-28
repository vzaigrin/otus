package ru.otus.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Boston {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage Boston <file1> <file2")
      sys.exit(-1)
    }

    val spark = SparkSession.builder
      .appName("Boston")
      .getOrCreate()

    import spark.implicits._

    val data1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    data1.printSchema()
    data1.select(percentile_approx($"OFFENSE_CODE", lit(0.95), lit(1))).show

    val data2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    data2.printSchema()

    spark.stop()
  }
}
