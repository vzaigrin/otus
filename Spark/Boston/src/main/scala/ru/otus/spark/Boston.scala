package ru.otus.spark

import org.apache.spark.sql.SparkSession

object Boston {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage Boston <file1> <file2")
      sys.exit(-1)
    }

    val spark = SparkSession.builder
      .appName("Boston")
      .getOrCreate()

    val data1 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    data1.printSchema()

    val data2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    data2.printSchema()

    spark.stop()
  }
}
