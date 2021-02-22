import org.apache.spark.sql.functions.{broadcast, col, count}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DFPractice extends App with SparkSessionWrapper {

  import spark.implicits._

  def readCSV(path: String, cols2Read: Seq[String] = Seq("*"))(header: Boolean = true)(implicit spark: SparkSession): DataFrame =
    spark
      .read
      .option("header", header)
      .option("inferSchema", "true")
      .csv(path)
      .select(cols2Read.map(col):_ *)

  def readParquet(path: String, cols2Read: Seq[String] = Seq("*"))(implicit spark: SparkSession): DataFrame =
    spark
      .read
      .parquet(path)
      .select(cols2Read.map(col):_ *)

  def processing(df1: DataFrame, df2: DataFrame, df1JoinCol: String, df2JoinCol: String): DataFrame = df1
    .join(
      broadcast(df2),
      col(df1JoinCol) === col(df2JoinCol)
    )
    .drop(df1JoinCol, df2JoinCol)
    .groupBy("Borough")
    .agg(count("Borough").as("total_orders"))
    .orderBy($"total_orders".desc)


  // Предобработка файла с зонами осуществляться не будет, хотя в нем и присутствуют дубли, т.к.
  // при последующем соединении могут быть потеряны данные, а дубли схлопнутся при дальнейшей агрегации.
  val zones: DataFrame = readCSV("src/main/resources/taxi_zones.csv", Seq("LocationID", "Borough"))()

  // В задаче четко не определяется, что считать "районом для заказа" => были выбраны районы посадки
  val data: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018", Seq("PULocationID"))

  val boroughRating: DataFrame = processing(data, zones, "PULocationID","LocationID")

  boroughRating.show

  boroughRating
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/borough_rating")

}
