package ru.otus.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object WinMag {
  def main(args: Array[String]): Unit = {
    // Проверяем аргументы вызова
    if (args.length != 1) {
      println("Usage: WineMag <data>")
      sys.exit(-1)
    }

    // Создаём SparkSession
    val spark = SparkSession
      .builder()
      .appName("WineMag")
      .getOrCreate()

    import spark.implicits._

    try {
      // Читаем данные
      val wine: Dataset[Wine] = spark.read
        .option("header", "true")
        .csv(args(0))
        .map(Wine(_))

      // Фильтруем непустые значения названия страны и цены
      // Находим для каждой страны максимальную цену
      // Выводим первые 10 стран с самой большой максимальной ценой
      wine
        .filter(_.country.nonEmpty)
        .filter(_.price.nonEmpty)
        .select(col("country").as[String], col("price").as[Double])
        .groupBy(col("country"))
        .agg(max("price").as("max_price"))
        .as[(String, Double)]
        .orderBy($"max_price".desc)
        .show(10, truncate = false)

    } catch {
      // Если что-то пошло не так
      case e: Throwable =>
        println(s"Error: ${e.getLocalizedMessage}")
        sys.exit(-1)
    } finally {
      // Останавливаем SparkContext
      spark.stop()
    }
  }
}
