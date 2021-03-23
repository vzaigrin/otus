package ru.otus.spark

import org.apache.spark.sql.Row

case class Wine(
    id: Option[Int],
    country: Option[String],
    description: Option[String],
    designation: Option[String],
    points: Option[Double],
    price: Option[Double],
    province: Option[String],
    region_1: Option[String],
    region_2: Option[String],
    taster_name: Option[String],
    taster_twitter_handle: Option[String],
    title: Option[String],
    variety: Option[String],
    winery: Option[String]
) {
  override def toString: String =
    f"${id.getOrElse("")}\t$country\t$description\t$designation\t${points.getOrElse(Double.NaN)}%.2f\t${price
      .getOrElse(Double.NaN)}%.2f\t$province\t$region_1\t$region_2\t$taster_name\t$taster_twitter_handle\t$title\t$variety\t$winery"
}

object Wine {
  def apply(r: Row): Wine =
    Wine(
      try {
        if (r.get(1) != null) Some(r.getString(0).toInt) else None
      } catch {
        case _: Throwable => None
      },
      if (r.get(1) != null) Some(r.getString(1)) else None,
      if (r.get(2) != null) Some(r.getString(2)) else None,
      if (r.get(3) != null) Some(r.getString(3)) else None,
      try {
        if (r.get(4) != null) Some(r.getString(4).toDouble) else None
      } catch {
        case _: Throwable => None
      },
      try {
        if (r.get(5) != null) Some(r.getString(5).toDouble) else None
      } catch {
        case _: Throwable => None
      },
      if (r.get(6) != null) Some(r.getString(6)) else None,
      if (r.get(7) != null) Some(r.getString(7)) else None,
      if (r.get(8) != null) Some(r.getString(8)) else None,
      if (r.get(9) != null) Some(r.getString(9)) else None,
      if (r.get(10) != null) Some(r.getString(10)) else None,
      if (r.get(11) != null) Some(r.getString(11)) else None,
      if (r.get(12) != null) Some(r.getString(12)) else None,
      if (r.get(13) != null) Some(r.getString(13)) else None
    )
}
