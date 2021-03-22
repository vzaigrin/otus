package ru.otus.spark

import org.apache.spark.sql.Row

case class TripsByYM(pickup_month: Int, y2019: Long, y2020: Long) {
  override def toString: String = s"$pickup_month\t$y2019\t$y2020"
}

object TripsByYM {
  def apply(r: Row): TripsByYM = TripsByYM(r.getInt(0), r.getLong(1), r.getLong(2))
}
