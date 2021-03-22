package ru.otus.spark

import org.apache.spark.sql.Row

case class TripsGrouped(
    pickup_year: Int,
    pickup_month: Int,
    PUBorough: String,
    count: Long
) {
  override def toString: String = f"$pickup_year\t$pickup_month\t$PUBorough%15s\t$count"
}

object TripsGrouped {
  def apply(r: Row): TripsGrouped =
    TripsGrouped(
      r.getInt(0),
      r.getInt(1),
      r.getString(2),
      r.getLong(3)
    )
}
