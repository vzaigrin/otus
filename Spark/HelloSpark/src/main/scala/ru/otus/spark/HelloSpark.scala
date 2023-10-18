package ru.otus.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time._
import java.sql.Timestamp
import scala.io.StdIn

case class Person(name: String, age: Long)

object HelloSpark {
  def main(args: Array[String]): Unit = {
    // Создаём SparkSession
    val spark = SparkSession
      .builder()
      .appName("HelloSpark")
      .getOrCreate()

    import spark.implicits._

    println(s"spark.version == ${spark.version}")

    // RDD
    val rdd = spark.sparkContext.parallelize(
      List(
        (1, 2.0, "string1", LocalDate.of(2000, 1, 1), LocalDateTime.of(2000, 1, 1, 12, 0)),
        (2, 3.0, "string2", LocalDate.of(2000, 2, 1), LocalDateTime.of(2000, 1, 2, 12, 0)),
        (3, 4.0, "string3", LocalDate.of(2000, 3, 1), LocalDateTime.of(2000, 1, 3, 12, 0))
      )
    )

    rdd.foreach(println)

    for (r <- rdd.take(2))
      println(r)

    // DataFrame
    val df = List(
      (1L, 2.0, "string1", LocalDate.of(2000, 1, 1), Timestamp.valueOf("2000-01-01 12:00:00")),
      (2L, 3.0, "string2", LocalDate.of(2000, 2, 1), Timestamp.valueOf("2000-02-01 12:00:00")),
      (3L, 4.0, "string3", LocalDate.of(2000, 3, 1), Timestamp.valueOf("2000-03-01 12:00:00"))
    ).toDF("a", "b", "c", "d", "e")

    df.show()
    df.printSchema()

    df.withColumn("upper_c", upper($"c")).show()
    df.select(col("c")).show()
    df.filter($"a" === 1).show()

    val df2 = List(
      ("red", "banana", 1, 10),
      ("blue", "banana", 2, 20),
      ("red", "carrot", 3, 30),
      ("blue", "grape", 4, 40),
      ("red", "carrot", 5, 50),
      ("black", "carrot", 6, 60),
      ("red", "banana", 7, 70),
      ("red", "grape", 8, 80)
    ).toDF("color", "fruit", "v1", "v2")

    df2.show()
    df2.groupBy($"color").avg().show()

    // SQL
    df2.createOrReplaceTempView("tableA")
    spark.sql("SELECT count(*) from tableA").show()

    // DataSet
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.show

    val caseClassDS = Seq(Person("Andy", 32), Person("Mike", 25), Person("Mary", 18)).toDS()

    caseClassDS.show
    caseClassDS.filter(_.age > 20).show
    caseClassDS.select(col("name")).show

    // Stop
    spark.stop()
  }
}
