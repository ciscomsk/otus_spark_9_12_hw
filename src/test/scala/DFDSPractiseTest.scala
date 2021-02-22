import DFPractice.{readCSV, readParquet, processing => procDF}
import DSPractice.{Distance, Mart, processing => procDS}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.catalyst.encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.test.SharedSparkSession

class DFDSPractiseTest extends SharedSparkSession {

  test("DFPractice - processing") {

    val zones: DataFrame = readCSV("src/test/resources/taxi_zones.csv", Seq("LocationID", "Borough"))()
    val data: DataFrame = readParquet("src/test/resources/yellow_taxi_jan_25_2018", Seq("PULocationID"))
    val actualDF: DataFrame = procDF(data, zones, "PULocationID","LocationID")

    val expectedDF: Seq[Row] = Row("Manhattan", 304266) ::
      Row("Queens", 17712) ::
      Row("Unknown", 6644) ::
      Row("Brooklyn", 3037) ::
      Row("Bronx", 211) ::
      Row("EWR", 19) ::
      Row("Staten Island", 4) ::
      Nil

    checkAnswer(actualDF, expectedDF)
  }

  test("DSPractice - processing") {

    val encoder: ExpressionEncoder[Distance] = encoders.ExpressionEncoder[Distance]

    val dataDS: Dataset[Distance] = readParquet("src/main/resources/yellow_taxi_jan_25_2018", Seq("trip_distance")).as(encoder)
    val actualDS: Dataset[Mart] = procDS(dataDS)

    val expectedDF: Seq[Row] = Row(331893, 2.718, 3.485, 66.0, 0.0) :: Nil

    checkAnswer(actualDS.toDF, expectedDF)
  }

}
