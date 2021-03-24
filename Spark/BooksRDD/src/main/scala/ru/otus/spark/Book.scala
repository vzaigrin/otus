package ru.otus.spark

import org.apache.commons.csv.CSVRecord

case class Book(
    name: String,
    author: String,
    userRating: Float,
    reviews: Long,
    price: Int,
    year: Int,
    genre: String
)

object Book {
  def apply(r: CSVRecord): Book =
    Book(
      r.get(0),
      r.get(1),
      r.get(2).toFloat,
      r.get(3).toLong,
      r.get(4).toInt,
      r.get(5).toInt,
      r.get(6)
    )
}
