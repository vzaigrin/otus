package ru.otus.spark

import org.apache.spark.sql.{Dataset, SparkSession, TypedColumn}

object MyTypedAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MyTypedAverage")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[Employee] = Seq(
      Employee("Michael", 3000),
      Employee("Andy", 4500),
      Employee("Justin", 3500),
      Employee("Berta", 4000)
    ).toDS()
    ds.show()

    val averageSalary: TypedColumn[Employee, Double] = MyAverage.toColumn.name("average_salary")
    val result: Dataset[Double] = ds.select(averageSalary)
    result.show()

    spark.stop()
  }
}
