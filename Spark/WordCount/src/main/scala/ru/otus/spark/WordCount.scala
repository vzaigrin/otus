package ru.otus.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // Проверяем наличия аргумента вызова
    if (args.length != 1) {
      println("Usage: WordCount <filename>")
      sys.exit(-1)
    }

    // Создаём SparkConf и SparkContext
    val conf: SparkConf  = new SparkConf().setAppName("wordCount")
    val sc: SparkContext = new SparkContext(conf)

    try {
      // Читаем файл, переданный аргументом вызова
      val file: RDD[String] = sc.textFile(args(0))

      // Убираем специальные символы, собираем пробелы
      // разбиваем на слова, создаём кортеж (слово, 1)
      // группируем по словам, суммируем счётчик
      val counts: RDD[(String, Int)] = file
        .map(line => line.replaceAll("\\W", " ").replaceAll("\\s+", " ").trim)
        .flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      // Выводим результат на экран
      counts
        .sortBy(_._1)
        .foreach { kv => println(s"${kv._1}:\t${kv._2}") }
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
