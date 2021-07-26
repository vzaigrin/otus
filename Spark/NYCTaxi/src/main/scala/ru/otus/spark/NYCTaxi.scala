package ru.otus.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object NYCTaxi {
  def main(args: Array[String]): Unit = {
    // Проверяем аргументы вызова
    if (args.length != 3) {
      println("Usage: NYCTaxi <trips> <payments> <zones>")
      sys.exit(-1)
    }

    // Создаём SparkSession
    val spark = SparkSession
      .builder()
      .appName("NYCTaxi")
      .getOrCreate()

    import spark.implicits._

    try {
      // Читаем данные о поездках
      val tripsRaw = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args(0))

      // Отфильтровываем некорректные данные, удаляем лишние колонки, преобразуем время
      val trips = tripsRaw
        .filter($"trip_distance" > 0)
        .filter($"passenger_count" > 0)
        .drop(
          "VendorID",
          "RatecodeID",
          "store_and_fwd_flag",
          "fare_amount",
          "extra",
          "mta_tax",
          "improvement_surcharge",
          "congestion_surcharge"
        )
        .withColumn("pickup_dt", to_timestamp($"tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dropoff_dt", to_timestamp($"tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
        .filter($"dropoff_dt" > $"pickup_dt")
        .withColumn("pickup_year", year($"pickup_dt"))
        .withColumn("pickup_month", month($"pickup_dt"))
        .withColumn("pickup_day", dayofmonth($"pickup_dt"))
        .withColumn("pickup_dayofweek", dayofweek($"pickup_dt"))
        .withColumn("pickup_weekofyear", weekofyear($"pickup_dt"))
        .withColumn("pickup_hour", hour($"pickup_dt"))
        .withColumn("dropoff_year", year($"dropoff_dt"))
        .withColumn("dropoff_month", month($"dropoff_dt"))
        .withColumn("dropoff_day", dayofmonth($"dropoff_dt"))
        .withColumn("dropoff_dayofweek", dayofweek($"dropoff_dt"))
        .withColumn("dropoff_weekofyear", weekofyear($"dropoff_dt"))
        .withColumn("dropoff_hour", hour($"dropoff_dt"))
        .filter($"pickup_year" > 2018 && $"pickup_year" < 2021)
        .filter($"dropoff_year" > 2018 && $"dropoff_year" < 2021)

      // Загружаем типы платежей
      val paymentType =
        spark.read.option("header", "true").option("inferSchema", "true").csv(args(1))

      // Загружаем информацию о зонах
      val zoneLookup = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args(2))
        .drop("service_zone")

      // Формируем наборы данных
      // "Базовый" набор
      val data = trips
        .join(paymentType, trips("payment_type") === paymentType("type_id"))
        .drop("payment_type", "type_id")
        .withColumnRenamed("type", "payment_type")
        .join(zoneLookup, trips("PULocationID") === zoneLookup("LocationID"))
        .drop("LocationID")
        .withColumnRenamed("Borough", "PUBorough")
        .withColumnRenamed("Zone", "PUZone")
        .join(zoneLookup, trips("DOLocationID") === zoneLookup("LocationID"))
        .drop("LocationID")
        .withColumnRenamed("Borough", "DOBorough")
        .withColumnRenamed("Zone", "DOZone")
        .cache()

      // Набор с часовой агрегацией поездок по зоне посадки
      val hourly = data
        .groupBy(
          "pickup_year",
          "pickup_month",
          "pickup_day",
          "pickup_dayofweek",
          "pickup_weekofyear",
          "pickup_hour",
          "PULocationID",
          "PUZone",
          "PUBorough"
        )
        .agg(sum("passenger_count").as("passengers"), count("pickup_dt").as("trips"))
        .cache()

      // Набор с месячной агрегацией поездок по зоне посадки
      val monthly = hourly
        .groupBy("pickup_year", "pickup_month", "PULocationID", "PUZone", "PUBorough")
        .agg(sum("passengers").as("passengers"), sum("trips").as("trips"))
        .cache()

      // Набор с месячной агрегацией поездок по району посадки
      val monthlyB = data
        .groupBy($"pickup_year".as("year"), $"pickup_month".as("month"), $"PUBorough")
        .agg(count("pickup_dt").as("trips"))
        .cache()

      // Анализ
      // Топ-5 зон по количеству посадок для каждого часа
      val windowH = Window
        .partitionBy("pickup_year", "pickup_month", "pickup_day", "pickup_hour")
        .orderBy($"trips".desc)

      val top5H = hourly
        .withColumn("rn", row_number().over(windowH))
        .filter($"rn" < 6)
        .drop("rn")
        .select(
          $"pickup_year",
          $"pickup_month",
          $"pickup_day",
          $"pickup_hour",
          $"PULocationID",
          $"PUZone",
          $"PUBorough",
          $"trips"
        )
        .orderBy($"pickup_year", $"pickup_month", $"pickup_day", $"pickup_hour", $"trips".desc)

      println
      println("Топ-5 зон по количеству посадок для каждого часа")
      top5H.show(numRows = 20, truncate = false)
      println

      // Топ-5 зон по количеству посадок для каждого месяца
      val windowW = Window
        .partitionBy("pickup_year", "pickup_month")
        .orderBy($"trips".desc)

      val top5M = monthly
        .withColumn("rn", row_number().over(windowW))
        .filter($"rn" < 6)
        .drop("rn")
        .select($"pickup_year", $"pickup_month", $"PULocationID", $"PUZone", $"PUBorough", $"trips")
        .orderBy($"pickup_year", $"pickup_month", $"trips".desc)

      println("Топ-5 зон по количеству посадок для каждого месяца")
      top5M.show(numRows = 20, truncate = false)
      println

      // Топ-5 пар (посадка - высадка) по количеству посадок для каждого месяца
      val top5MP = data
        .groupBy(
          "pickup_year",
          "pickup_month",
          "PULocationID",
          "PUZone",
          "PUBorough",
          "DOLocationID",
          "DOZone",
          "DOBorough"
        )
        .agg(count("pickup_dt").as("trips"))
        .withColumn("rn", row_number().over(windowW))
        .filter($"rn" < 6)
        .drop("rn")
        .select(
          "pickup_year",
          "pickup_month",
          "PULocationID",
          "PUZone",
          "PUBorough",
          "DOLocationID",
          "DOZone",
          "DOBorough",
          "trips"
        )
        .orderBy($"pickup_year", $"pickup_month", $"trips".desc)

      println("Топ-5 пар (посадка - высадка) по количеству посадок для каждого месяца")
      top5MP.show(numRows = 20, truncate = false)
      println

      // Суммарное количество поездок в каждый месяц 2019 и 2020 годов
      val tripsByYM = monthlyB
        .groupBy("month")
        .pivot("year")
        .sum("trips")

      println("Суммарное количество поездок в каждый месяц 2019 и 2020 годов")
      tripsByYM.orderBy("month").show(numRows = 12, truncate = false)
      println

      // Изменение количества поездок в каждый месяц 2019 и 2020 годов по районам
      val tripsByYMBorough = monthlyB
        .groupBy("month")
        .pivot("PUBorough")
        .agg(stddev("trips"))
        .orderBy("month")

      val tripsByYMBoroughDev = tripsByYMBorough.select(
        Array($"month") ++ tripsByYMBorough.columns.tail.map(col).map(c => round(c, 2)): _*
      )

      println("Изменение количества поездок в каждый месяц 2019 и 2020 годов по районам")

      tripsByYMBoroughDev
        .toDF(
          Array("month") ++ tripsByYMBoroughDev.columns.tail.map(s =>
            s.substring(4, s.length - 1)
          ): _*
        )
        .show(numRows = 12, truncate = false)

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
