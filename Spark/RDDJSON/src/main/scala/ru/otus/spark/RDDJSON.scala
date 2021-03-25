package ru.otus.spark

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.spark.{SparkConf, SparkContext}

object RDDJSON {
  def main(args: Array[String]): Unit = {
    // Проверяем наличия аргумента вызова
    if (args.length != 1) {
      println("Usage: RDDJSON <filename>")
      sys.exit(-1)
    }

    // Создаём SparkConf и SparkContext
    val conf: SparkConf  = new SparkConf().setAppName("RDDJSON")
    val sc: SparkContext = new SparkContext(conf)

    try {
      // Читаем файл, переданный аргументом вызова
      val file = sc
        .textFile(args(0))
        // Разбираем JSON
        .map { line =>
          decode[Book](line) match {
            case Right(value) => value
            case Left(e)      => println(e.getLocalizedMessage)
          }
        }

      // Выводим результат на экран
      file.foreach(println)
    } catch {
      // Если что-то пошло не так
      case e: Throwable =>
        println(s"Error: ${e.getLocalizedMessage}")
        sys.exit(-1)
    } finally {
      // Останавливаем SparkContext
      sc.stop()
    }
  }
}
