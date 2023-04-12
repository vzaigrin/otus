package lesson2

import lesson2.OtusFragmentedByMethod.{processTaxiData, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.funsuite.AnyFunSuite


class TestSharedSparkSession extends SharedSparkSession {
  import testImplicits._

  test("join - join using") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")

    checkAnswer(
      df.join(df2, "int"),
      Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "5") :: Nil)
  }

  test("join - processTaxiData") {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiData(taxiZonesDF2, taxiDF2)

    checkAnswer(
      actualDistribution,
      Row("Manhattan",30426,0.0,2.23,66.0)  ::
        Row("Queens",17712,0.0,11.14,53.5) ::
        Row("Unknown",6644,0.0,2.34,42.8) ::
        Row("Brooklyn",3037,0.0,3.28,27.37) ::
        Row("Bronx",211,0.0,2.99,20.09) ::
        Row("EWR",19,0.0,3.46,17.3) ::
        Row("Staten Island",4,0.0,0.2,0.5) :: Nil
    )

  }

}

