package ru.otus.spark

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object Books {
  def main(args: Array[String]): Unit = {
    // Проверяем наличия аргумента вызова
    if (args.length != 1) {
      println("Usage: Books <filename>")
      sys.exit(-1)
    }

    // Encoder для Book
    implicit val bookEncoder: Encoder[Book] = deriveEncoder[Book]

    // Создаём SparkConf и SparkContext
    val conf: SparkConf  = new SparkConf().setAppName("Books")
    val sc: SparkContext = new SparkContext(conf)

    try {
      // Читаем файл, переданный аргументом вызова
      val file: RDD[String] = sc
        .textFile(args(0))
        // Пропускаем первую строку
        .zipWithIndex
        .filter(_._2 > 0)
        .map(_._1)
        // Разбираем CSV
        .map { line =>
          val parser            = CSVParser.parse(line, CSVFormat.RFC4180)
          val record: CSVRecord = parser.getRecords.asScala.toList.head
          // Конвертируем в JSON
          Book(record).asJson.noSpaces
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
