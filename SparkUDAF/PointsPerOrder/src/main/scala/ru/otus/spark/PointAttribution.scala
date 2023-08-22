package ru.otus.spark

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

class PointAttribution extends Aggregator[Int, Buffer, Int] {
  val MAX_POINT_PER_ORDER = 3
  // Начальное значение. Должно соответствовать свойству: любое b + zero = b
  def zero: Buffer = Buffer(0)
  // Объединение двух значений в новое значение.
  // Для повышения производительности функция может изменять `buffer` и
  // возвращать его вместо создания нового объекта.
  def reduce(buffer: Buffer, data: Int): Buffer = {
    val outputValue = if (data < MAX_POINT_PER_ORDER) data else MAX_POINT_PER_ORDER
    buffer.value += outputValue
    buffer
  }
  // Объединение двух промежуточных значения
  def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.value += b2.value
    b1
  }
  // Преобразование выходных данных
  def finish(reduction: Buffer): Int = reduction.value
  // Кодировщик для типа промежуточного значения
  def bufferEncoder: Encoder[Buffer] = Encoders.product
  // Кодировщик для типа выходного значения
  def outputEncoder: Encoder[Int] = Encoders.scalaInt
}
