package lesson2

import lesson2.OtusFragmentedByMethod.{processTaxiData, readCSV, readParquet}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SimpleUnitTest extends AnyFlatSpec {


  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()

  it should "upload and process data" in {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiData(taxiZonesDF2, taxiDF2)
      .collectAsList()
      .get(0)

    assert(actualDistribution.get(0) == "Manhattan")
    assert(actualDistribution.get(1) == 304266)
    assert(actualDistribution.get(2) == 0.0)
    assert(actualDistribution.get(3) == 2.23)
    assert(actualDistribution.get(4) == 66.0)
  }

}
