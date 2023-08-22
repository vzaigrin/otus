package ru.otus.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.{DataFrame, SparkSession}

object MyUntypedAverage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MyUntypedAverage")
      .getOrCreate()

    import spark.implicits._

    val data: DataFrame = Seq(("Michael", 3000), ("Andy", 4500), ("Justin", 3500), ("Berta", 4000))
      .toDF("name", "salary")
    data.show()

    val myAverageUdf: UserDefinedFunction = udaf(new MyAverage)

    val result: DataFrame = data.agg(myAverageUdf($"salary").as("average_salary"))
    result.show()

    spark.stop()
  }
}
