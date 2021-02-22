import org.apache.spark.rdd.RDD
import DFPractice.readParquet
import org.apache.spark.sql.Row
import org.joda.time.DateTime

import java.sql.Timestamp

object RDDPractice extends App with SparkSessionWrapper {

  def getTimeDistributionByHours(rdd :RDD[Row]): RDD[(Int, Int)] = rdd
    // - 3600000 => приводим к GMT +03:00
    .map(row => new Timestamp(row.getTimestamp(1).getTime - 360000))
    .map(ts => new DateTime(ts).hourOfDay.get)
    .map(hour => (hour, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)

  val dataRDD: RDD[Row] =
    readParquet("src/main/resources/yellow_taxi_jan_25_2018").rdd

  val resRDD: RDD[(Int, Int)] = getTimeDistributionByHours(dataRDD)

  resRDD.foreach(println)

  resRDD
    .map(el => s"${el._1} ${el._2}")
    .saveAsTextFile("src/main/resources/rdd")





}
