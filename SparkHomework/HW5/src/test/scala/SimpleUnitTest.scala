import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import Task2.processtaxiRDD
import Task2.TaxiZoneFact

class SimpleUnitTest extends AnyFlatSpec {


  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  it should "upload and process data" in {
    val taxiDF = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiRDD = taxiDF.rdd


    val actualResult = processtaxiRDD(taxiRDD)
      .take(1)

    assert(actualResult === Array(("19", 22121)))

  }

  spark.close()

}
