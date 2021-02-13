package ru.otus.sparkmlstreaming

import com.typesafe.config._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

object MLStructuredStreaming {
  def main(args: Array[String]): Unit = {
    // Читаем конфигурационный файл
    val config                 = ConfigFactory.load()
    val inputBootstrapServers  = config.getString("input.bootstrap.servers")
    val inputTopic             = config.getString("input.topic")
    val outputBootstrapServers = config.getString("output.bootstrap.servers")
    val outputTopic            = config.getString("output.topic")
    val path2model             = config.getString("path2model")
    val checkpointLocation     = config.getString("checkpointLocation")

    // Загружаем модель
    val model = PipelineModel.load(path2model)

    // Создаём SparkSession
    val spark = SparkSession.builder
      .appName("MLStructuredStreaming")
      .getOrCreate()

    import spark.implicits._

    // Читаем входной поток
    val input = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.replace("\"", "").trim.split(","))
      .map { a =>
        Data(
          a(0).toInt,
          a(1),
          a(2).toInt,
          a(3),
          a(4).toInt,
          a(5),
          a(6),
          a(7),
          a(8),
          a(9).toInt,
          a(10).toInt,
          a(11).toInt,
          a(12).toInt,
          a(13).toDouble,
          a(14).toInt,
          a(15).toDouble,
          a(16).toDouble,
          a(17).toInt,
          a(18).toInt,
          a(19).toDouble,
          a(20).toDouble,
          a(21).toDouble,
          a(22).toDouble
        )
      }

    // Применяем модель к входным данным
    val prediction = model.transform(input)

    // Выводим результат
    val query = prediction
      .select(concat_ws(",", $"CLIENTNUM", $"prediction").as("value"))
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", outputBootstrapServers)
      .option("topic", outputTopic)
      .start()

    query.awaitTermination()
  }
}
