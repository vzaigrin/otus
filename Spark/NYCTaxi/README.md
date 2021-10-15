# NYC Taxi records

Анализ данных о поездках в "жёлтом" такси Нью-Йорка

## Источник данных
Данные находятся в открытом доступе [здесь](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Каждый файл представляет собой записи о поездках за месяц.
Описание данных находится [здесь](https://www1.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) и [здесь](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
В записях используются идентификаторы зон посадок и высадок. Данные о зонах находятся [здесь](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv).

## Запуск

*spark-submit --driver-memory 2G --executor-memory 2G target/scala-2.12/NYCTaxi-assembly-2.0.jar data/trips data/payment_type.csv data/taxi_zone_lookup.csv*
