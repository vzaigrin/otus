# Structured Network Word Count Windowed

Пример простого приложения Structured Spark Streaming

## Запуск

* В первом терминале запускаем *nc -lk 9999* и вводить слова, разделённые пробелом
* Во втором терминале запускаем *spark-submit StructuredNetworkWordCountWindowed-assembly-1.0.jar localhost 9999 10 5*
