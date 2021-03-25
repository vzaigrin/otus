package ru.otus.spark

case class Book(
    name: String,
    author: String,
    userRating: Float,
    reviews: Long,
    price: Int,
    year: Int,
    genre: String
)
