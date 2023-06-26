package ru.otus.spark

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class MyAverage extends Aggregator[Long, Average, Double] {
  // Начальное значение. Должно соответствовать свойству: любое b + zero = b
  def zero: Average = Average(0L, 0L)
  // Объединение двух значений в новое значение.
  // Для повышения производительности функция может изменять `buffer` и
  // возвращать его вместо создания нового объекта.
  def reduce(buffer: Average, data: Long): Average = {
    buffer.sum += data
    buffer.count += 1
    buffer
  }
  // Объединение двух промежуточных значения
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Преобразование выходных данных
  def finish(reduction: Average): Double =
    reduction.sum.toDouble / reduction.count
  // Кодировщик для типа промежуточного значения
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Кодировщик для типа выходного значения
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
