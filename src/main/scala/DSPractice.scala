import org.apache.spark.sql.{ColumnName, Dataset, SaveMode}
import DFPractice.readParquet
import org.apache.spark.sql.functions.{avg, count, max, min, round, stddev_pop}

object DSPractice extends App with SparkSessionWrapper {

  import spark.implicits._

  case class Distance(trip_distance: Double)
  case class Mart(
                  total_trips: Int,
                  average_distance: Double,
                  population_standard_deviation: Double,
                  maximum_distance: Double,
                  minimum_distance: Double,
                 )

  def processing(ds: Dataset[Distance]): Dataset[Mart] = {
    val tripDistCol: ColumnName = $"trip_distance"

    ds
      .select(
        count(tripDistCol) as "total_trips",
        round(avg(tripDistCol), 3) as "average_distance",
        round(stddev_pop(tripDistCol), 3) as "population_standard_deviation",
        max(tripDistCol) as "maximum_distance",
        min(tripDistCol) as "minimum_distance"
      )
      .withColumn("total_trips", $"total_trips".cast("int"))
      .as[Mart]

  }

  val dataDS: Dataset[Distance] = readParquet("src/main/resources/yellow_taxi_jan_25_2018", Seq("trip_distance")).as[Distance]
  val resDS: Dataset[Mart] = processing(dataDS)

  resDS.show

  val dbUrl: String = "jdbc:postgresql://localhost:5432/otus"
  val dbTable: String = "public.distance_mart"
  val dbUser: String = "docker"
  val dbPassword: String = "docker"

  resDS.write
    .format("jdbc")
    .option("url", dbUrl)
    .option("dbtable", dbTable)
    .option("user", dbUser)
    .option("password", dbPassword)
    .mode(SaveMode.Overwrite)
    .save

}
