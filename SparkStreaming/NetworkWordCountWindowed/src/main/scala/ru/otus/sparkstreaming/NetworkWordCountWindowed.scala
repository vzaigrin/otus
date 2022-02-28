package ru.otus.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountWindowed {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCountWindowed <hostname> <port>")
      System.exit(1)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCountWindowed")
    val ssc       = new StreamingContext(sparkConf, Seconds(1))

    // Create a socket stream on target ip:port
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    // Reduce last 30 seconds of data, every 10 seconds
    val windowedWordCounts =
      pairs.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(10))

    windowedWordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
