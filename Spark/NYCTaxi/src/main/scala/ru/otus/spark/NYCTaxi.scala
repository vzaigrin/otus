package ru.otus.spark

import org.apache.spark.sql.SparkSession
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
        .filter($"trip_distance" > 1)
        .filter($"passenger_count" > 0)
        .drop(
          "VendorID",
          "RatecodeID",
          "store_and_fwd_flag",
          "fare_amount",
          "extra",
          "mta_tax",
          "tip_amount",
          "tolls_amount",
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
        .withColumn("pickup_weekofyear", weekofyear($"pickup_dt"))
        .withColumn("pickup_hour", hour($"pickup_dt"))
        .withColumn("dropoff_year", year($"dropoff_dt"))
        .withColumn("dropoff_month", month($"dropoff_dt"))
        .withColumn("dropoff_day", dayofmonth($"dropoff_dt"))
        .withColumn("dropoff_weekofyear", weekofyear($"dropoff_dt"))
        .withColumn("dropoff_hour", hour($"dropoff_dt"))
        .filter($"pickup_year" > 2018 && $"pickup_year" < 2021)
        .filter($"dropoff_year" > 2018 && $"dropoff_year" < 2021)

      // Загружаем типы платежей
      val paymentType =
        spark.read.option("header", "true").option("inferSchema", "true").csv(args(1))

      // Создаём Broadcast для paymentType
      val paymentBC = spark.sparkContext.broadcast(paymentType)

      // Загружаем информацию о зонах
      val zoneLookup = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args(2))
        .drop("service_zone")

      // Создаём Broadcast для zoneLookup
      val zoneBC = spark.sparkContext.broadcast(zoneLookup)

      // Объединяем поездки с типами платежей и с зонами посадки и высадки
      val data = trips
        .join(paymentBC.value, trips("payment_type") === paymentType("type_id"))
        .drop("payment_type", "type_id")
        .withColumnRenamed("type", "payment_type")
        .join(zoneBC.value, trips("PULocationID") === zoneLookup("LocationID"))
        .drop("PULocationID", "LocationID")
        .withColumnRenamed("Borough", "PUBorough")
        .withColumnRenamed("Zone", "PUZone")
        .join(zoneBC.value, trips("DOLocationID") === zoneLookup("LocationID"))
        .drop("DOLocationID", "LocationID")
        .withColumnRenamed("Borough", "DOBorough")
        .withColumnRenamed("Zone", "DOZone")
        .filter($"PUBorough" =!= "Unknown")
        .filter($"DOBorough" =!= "Unknown")

      // Группируем данные по году, месяцу и району посадки
      val tripsGrouped = data
        .groupBy("pickup_year", "pickup_month", "PUBorough")
        .count
        .cache

      // Выгружаем сгруппированные данные
      val tripsGroupedC: Array[TripsGrouped] = tripsGrouped.collect.map { r => TripsGrouped(r) }

      // Выводим количество поездок по году, месяцу и району посадки
      println
      println("Количество посадок в районах по году и месяцу")
      println("Год\tМесяц\t          Район\tКол-во")
      tripsGroupedC
        .sortBy(_.pickup_year)
        .sortBy(_.pickup_month)
        .sortBy(_.PUBorough)
        .foreach(println)

      // Группируем количество поездок по году и месяцу
      val tripsByYM = tripsGrouped
        .groupBy("pickup_month")
        .pivot("pickup_year")
        .sum("count")

      // Выгружаем сгруппированные данные
      val tripsByYMC: Array[TripsByYM] = tripsByYM.collect.map { r => TripsByYM(r) }

      // Выводим количество поездок по году и месяцу
      println
      println("Количество посадок по году и месяцу")
      println(s"Месяц\t2019\t2020")
      tripsByYMC.sortBy(_.pickup_month).foreach(println)

      // Вычисляем средние отклонения количества посадок по месяцам и районам по годам
      val tripsByYMBorough = tripsGrouped
        .groupBy("pickup_month")
        .pivot("PUBorough")
        .agg(stddev("count"))

      // Выгружаем сгруппированные данные
      val tripsByYMBoroughC = tripsByYMBorough.collect.map { r => TripsByYMBorough(r) }

      // Выводим средние отклонения
      println
      println("Средние отклонения количества посадок по месяцам и районам по годам")
      println(s"Месяц\tBronx\tBrooklyn\tEWR\tManhattan\tQueens\tStaten Island")
      tripsByYMBoroughC.sortBy(_.pickup_month).foreach(println)

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
