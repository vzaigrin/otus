package ru.otus.spark

import org.apache.spark.sql.Row

case class TripsByYMBorough(
    pickup_month: Int,
    Bronx: Double,
    Brooklyn: Double,
    EWR: Double,
    Manhattan: Double,
    Queens: Double,
    Staten_Island: Double
) {
  override def toString: String =
    f"$pickup_month\t$Bronx%.2f\t$Brooklyn%.2f\t$EWR%.2f\t$Manhattan%.2f\t$Queens%.2f\t$Staten_Island%.2f"
}

object TripsByYMBorough {
  def apply(r: Row): TripsByYMBorough =
    TripsByYMBorough(
      r.getInt(0),
      r.getDouble(1),
      r.getDouble(2),
      r.getDouble(3),
      r.getDouble(4),
      r.getDouble(5),
      r.getDouble(6)
    )
}
