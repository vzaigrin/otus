package ru.otus.spark

import org.apache.spark.sql.SparkSession

object MyTypedAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MyTypedAverage")
      .getOrCreate()

    import spark.implicits._

    val ds = Seq(
      Employee("Michael", 3000),
      Employee("Andy", 4500),
      Employee("Justin", 3500),
      Employee("Berta", 4000)
    ).toDS()
    ds.show()

    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()

    spark.stop()
  }
}
