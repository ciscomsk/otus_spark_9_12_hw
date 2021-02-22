import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


trait SparkSessionTestWrapper extends Serializable {

  lazy val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("spark test session")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)

}
