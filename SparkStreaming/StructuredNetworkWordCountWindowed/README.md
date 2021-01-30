# Structured Network Word Count Windowed

Пример простого приложения Structured Spark Streaming

## Запуск

* В первом терминале запустить *nc -lk 9999* и вводить слова, разделённые пробелом
* Во втором терминале запустит *spark-submit StructuredNetworkWordCountWindowed-assembly-1.0.jar localhost 9999 10 5*
