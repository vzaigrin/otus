import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    # Проверяем аргументы вызова
    if len(sys.argv) != 4:
        print("Usage: NYCTaxi <trips> <payments> <zones>", file=sys.stderr)
        sys.exit(-1)

    # Создаём SparkSession
    spark = SparkSession \
        .builder \
        .appName("PyNYCTaxi") \
        .getOrCreate()

    # Читаем данные о поездках
    tripsRaw = spark.read.option("header", "true").option("inferSchema", "true").csv(sys.argv[1])

    # Отфильтровываем некорректные данные, удаляем лишние колонки, преобразуем время
    tripsWithColumn1 = tripsRaw \
        .drop("VendorID", "RatecodeID", "store_and_fwd_flag", "fare_amount", "extra", "mta_tax",
              "improvement_surcharge", "congestion_surcharge") \
        .withColumn('pickup_dt', to_timestamp(tripsRaw['tpep_pickup_datetime'], 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('dropoff_dt', to_timestamp(tripsRaw['tpep_dropoff_datetime'], 'yyyy-MM-dd HH:mm:ss')) \
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

    tripsWithColumn = tripsWithColumn1 \
        .withColumn("pickup_year", year(tripsWithColumn1['pickup_dt'])) \
        .withColumn("pickup_month", month(tripsWithColumn1['pickup_dt'])) \
        .withColumn("pickup_day", dayofmonth(tripsWithColumn1['pickup_dt'])) \
        .withColumn("pickup_dayofweek", dayofweek(tripsWithColumn1['pickup_dt'])) \
        .withColumn("pickup_weekofyear", weekofyear(tripsWithColumn1['pickup_dt'])) \
        .withColumn("pickup_hour", hour(tripsWithColumn1['pickup_dt'])) \
        .withColumn("dropoff_year", year(tripsWithColumn1['dropoff_dt'])) \
        .withColumn("dropoff_month", month(tripsWithColumn1['dropoff_dt'])) \
        .withColumn("dropoff_day", dayofmonth(tripsWithColumn1['dropoff_dt'])) \
        .withColumn("dropoff_dayofweek", dayofweek(tripsWithColumn1['dropoff_dt'])) \
        .withColumn("dropoff_weekofyear", weekofyear(tripsWithColumn1['dropoff_dt'])) \
        .withColumn("dropoff_hour", hour(tripsWithColumn1['dropoff_dt']))

    trips = tripsWithColumn \
        .filter(tripsWithColumn.trip_distance > 0) \
        .filter(tripsWithColumn.passenger_count > 0) \
        .filter(tripsWithColumn.dropoff_dt > tripsWithColumn.pickup_dt) \
        .filter((tripsWithColumn.pickup_year > 2018) & (tripsWithColumn.pickup_year < 2021)) \
        .filter((tripsWithColumn.dropoff_year > 2018) & (tripsWithColumn.dropoff_year < 2021))

    # Загружаем типы платежей
    paymentType = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[2])

    # Загружаем информацию о зонах
    zoneLookup = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sys.argv[3]) \
        .drop("service_zone")

    # Формируем наборы данных
    # "Базовый" набор
    data = trips \
        .join(paymentType, trips['payment_type'] == paymentType['type_id']) \
        .drop("payment_type", "type_id") \
        .withColumnRenamed("type", "payment_type") \
        .join(zoneLookup, trips['PULocationID'] == zoneLookup['LocationID']) \
        .drop("LocationID") \
        .withColumnRenamed("Borough", "PUBorough") \
        .withColumnRenamed("Zone", "PUZone") \
        .join(zoneLookup, trips['DOLocationID'] == zoneLookup['LocationID']) \
        .drop("LocationID") \
        .withColumnRenamed("Borough", "DOBorough") \
        .withColumnRenamed("Zone", "DOZone") \
        .cache()

    # Набор с часовой агрегацией поездок по зоне посадки
    hourly = data \
        .groupBy('pickup_year', 'pickup_month', 'pickup_day', 'pickup_dayofweek', 'pickup_weekofyear', 'pickup_hour',
                 'PULocationID', 'PUZone', 'PUBorough') \
        .agg(sum(col('passenger_count')).alias("passengers"), count('pickup_dt').alias("trips")) \
        .cache()

    # Набор с месячной агрегацией поездок по зоне посадки
    monthly = hourly \
        .groupBy("pickup_year", "pickup_month", "PULocationID", "PUZone", "PUBorough") \
        .agg(sum(col("passengers")).alias("passengers"), sum(col("trips")).alias("trips")) \
        .cache()

    # Набор с месячной агрегацией поездок по району посадки
    monthlyB = data \
        .groupBy(col("pickup_year").alias("year"), col("pickup_month").alias("month"), col("PUBorough")) \
        .agg(count(col("pickup_dt")).alias("trips")) \
        .cache()

    # Анализ
    # Топ-5 зон по количеству посадок для каждого часа
    windowH = Window \
        .partitionBy("pickup_year", "pickup_month", "pickup_day", "pickup_hour") \
        .orderBy(col("trips").desc())

    top5H = hourly \
        .withColumn("rn", row_number().over(windowH)) \
        .filter(col("rn") < 6) \
        .drop("rn") \
        .select("pickup_year", "pickup_month", "pickup_day", "pickup_hour", "PULocationID", "PUZone", "PUBorough",
                "trips") \
        .orderBy(col("pickup_year"), col("pickup_month"), col("pickup_day"), col("pickup_hour"), col("trips").desc())

    print()
    print("Топ-5 зон по количеству посадок для каждого часа")
    top5H.show(20, truncate=False)
    print()

    # Топ-5 зон по количеству посадок для каждого месяца
    windowW = Window \
        .partitionBy("pickup_year", "pickup_month") \
        .orderBy(col("trips").desc())

    top5M = monthly \
        .withColumn("rn", row_number().over(windowW)) \
        .filter(col("rn") < 6) \
        .drop("rn") \
        .select("pickup_year", "pickup_month", "PULocationID", "PUZone", "PUBorough", "trips") \
        .orderBy(col("pickup_year"), col("pickup_month"), col("trips").desc())

    print("Топ-5 зон по количеству посадок для каждого месяца")
    top5M.show(20, truncate=False)
    print()

    # Топ-5 пар (посадка - высадка) по количеству посадок для каждого месяца
    top5MP = data \
        .groupBy("pickup_year", "pickup_month", "PULocationID", "PUZone", "PUBorough", "DOLocationID", "DOZone",
                 "DOBorough") \
        .agg(count(col("pickup_dt")).alias("trips")) \
        .withColumn("rn", row_number().over(windowW)) \
        .filter(col("rn") < 6) \
        .drop("rn") \
        .select("pickup_year", "pickup_month", "PULocationID", "PUZone", "PUBorough", "DOLocationID", "DOZone",
                "DOBorough", "trips") \
        .orderBy(col("pickup_year"), col("pickup_month"), col("trips").desc())

    print("Топ-5 пар (посадка - высадка) по количеству посадок для каждого месяца")
    top5MP.show(20, truncate=False)
    print()

    # Суммарное количество поездок в каждый месяц 2019 и 2020 годов
    tripsByYM = monthlyB \
        .groupBy("month") \
        .pivot("year") \
        .sum("trips")

    print("Суммарное количество поездок в каждый месяц 2019 и 2020 годов")
    tripsByYM.orderBy("month").show(12, truncate=False)
    print()

    # Изменение количества поездок в каждый месяц 2019 и 2020 годов по районам
    tripsByYMBorough = monthlyB \
        .groupBy("month") \
        .pivot("PUBorough") \
        .agg(stddev("trips")) \
        .orderBy("month")

    head, *tail = tripsByYMBorough.columns
    columns = [col(head)] + list(map(lambda x: round(x, 2), tail))

    tripsByYMBoroughDev = tripsByYMBorough.select(*columns).toDF(*tripsByYMBorough.columns)

    print("Изменение количества поездок в каждый месяц 2019 и 2020 годов по районам")
    tripsByYMBoroughDev.show(12, truncate=False)

    spark.stop()
