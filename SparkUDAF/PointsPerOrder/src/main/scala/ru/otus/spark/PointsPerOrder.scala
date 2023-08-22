package ru.otus.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PointsPerOrder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PointsPerOrder")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Jean-Georges", "Perrin", "NC", 1, 300, 1551903533),
      ("Jean-Georges", "Perrin", "NC", 2, 120, 1551903567),
      ("Jean-Georges", "Perrin", "CA", 4, 75, 1551903599),
      ("Holden", "Karau", "CA", 6, 37, 1551904299),
      ("Ginni", "Rometty", "NY", 7, 91, 1551916792),
      ("Holden", "Karau", "CA", 4, 153, 1552876129)
    ).toDF("firstName", "lastName", "state", "quantity", "revenue", "timestamp")
    data.show()

    val pointAttribution = new PointAttribution
    val pointAttributionUdf = udaf(pointAttribution)

    val pointDf = data
      .groupBy($"firstName", $"lastName", $"state")
      .agg(sum("quantity"), pointAttributionUdf($"quantity").as("point"))
    pointDf.show()

    spark.stop()
  }
}
