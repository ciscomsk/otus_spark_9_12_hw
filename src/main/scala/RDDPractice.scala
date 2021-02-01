import org.apache.spark.rdd.RDD
import DFPractice.readParquet
import org.apache.spark.SparkContext

import java.nio.file.{Files, Path, Paths}
import java.sql.Timestamp

object RDDPractice extends App with SparkSessionWrapper {

  def getTiming(rdd :RDD[Timestamp]): RDD[(String, Int)] = {

    val sc: SparkContext = spark.sparkContext

    val t01 = sc.longAccumulator("00:00-01:00")
    val t12 = sc.longAccumulator("01:00-02:00")
    val t23 = sc.longAccumulator("02:00-03:00")
    val t34 = sc.longAccumulator("03:00-04:00")
    val t45 = sc.longAccumulator("04:00-05:00")
    val t56 = sc.longAccumulator("05:00-06:00")
    val t67 = sc.longAccumulator("06:00-07:00")
    val t78 = sc.longAccumulator("07:00-08:00")
    val t89 = sc.longAccumulator("08:00-09:00")
    val t910 = sc.longAccumulator("09:00-10:00")
    val t1011 = sc.longAccumulator("10:00-11:00")
    val t1112 = sc.longAccumulator("11:00-12:00")
    val t1213 = sc.longAccumulator("12:00-13:00")
    val t1314 = sc.longAccumulator("13:00-14:00")
    val t1415 = sc.longAccumulator("14:00-15:00")
    val t1516 = sc.longAccumulator("15:00-16:00")
    val t1617 = sc.longAccumulator("16:00-17:00")
    val t1718 = sc.longAccumulator("17:00-18:00")
    val t1819 = sc.longAccumulator("18:00-19:00")
    val t1920 = sc.longAccumulator("19:00-20:00")
    val t2021 = sc.longAccumulator("20:00-21:00")
    val t2122 = sc.longAccumulator("21:00-22:00")
    val t2223 = sc.longAccumulator("22:00-23:00")
    val t230 = sc.longAccumulator("23:00-00:00")


    rdd
      .foreach {
        case el if el.getTime >= 1516827600000L && el.getTime < 1516831200000L => t01.add(1)
        case el if el.getTime >= 1516831200000L && el.getTime < 1516834800000L => t12.add(1)
        case el if el.getTime >= 1516834800000L && el.getTime < 1516838400000L => t23.add(1)
        case el if el.getTime >= 1516838400000L && el.getTime < 1516842000000L => t34.add(1)
        case el if el.getTime >= 1516842000000L && el.getTime < 1516845600000L => t45.add(1)
        case el if el.getTime >= 1516845600000L && el.getTime < 1516849200000L => t56.add(1)
        case el if el.getTime >= 1516849200000L && el.getTime < 1516852800000L => t67.add(1)
        case el if el.getTime >= 1516852800000L && el.getTime < 1516856400000L => t78.add(1)
        case el if el.getTime >= 1516856400000L && el.getTime < 1516860000000L => t89.add(1)
        case el if el.getTime >= 1516860000000L && el.getTime < 1516863600000L => t910.add(1)
        case el if el.getTime >= 1516863600000L && el.getTime < 1516867200000L => t1011.add(1)
        case el if el.getTime >= 1516867200000L && el.getTime < 1516870800000L => t1112.add(1)
        case el if el.getTime >= 1516870800000L && el.getTime < 1516874400000L => t1213.add(1)
        case el if el.getTime >= 1516874400000L && el.getTime < 1516878000000L => t1314.add(1)
        case el if el.getTime >= 1516878000000L && el.getTime < 1516881600000L => t1415.add(1)
        case el if el.getTime >= 1516881600000L && el.getTime < 1516885200000L => t1516.add(1)
        case el if el.getTime >= 1516885200000L && el.getTime < 1516888800000L => t1617.add(1)
        case el if el.getTime >= 1516888800000L && el.getTime < 1516892400000L => t1718.add(1)
        case el if el.getTime >= 1516892400000L && el.getTime < 1516896000000L => t1819.add(1)
        case el if el.getTime >= 1516896000000L && el.getTime < 1516899600000L => t1920.add(1)
        case el if el.getTime >= 1516899600000L && el.getTime < 1516903200000L => t2021.add(1)
        case el if el.getTime >= 1516903200000L && el.getTime < 1516906800000L => t2122.add(1)
        case el if el.getTime >= 1516906800000L && el.getTime < 1516910400000L => t2223.add(1)
        case el if el.getTime >= 1516910400000L && el.getTime < 1516914000000L => t230.add(1)
        case _ =>
      }

    val resRDD: RDD[(String, Int)] = sc
      .parallelize(
        Seq(
          (t01.name.get, t01.value.toInt),
          (t12.name.get, t12.value.toInt),
          (t23.name.get, t23.value.toInt),
          (t34.name.get, t34.value.toInt),
          (t45.name.get, t45.value.toInt),
          (t56.name.get, t56.value.toInt),
          (t67.name.get, t67.value.toInt),
          (t78.name.get, t78.value.toInt),
          (t89.name.get, t89.value.toInt),
          (t910.name.get, t910.value.toInt),
          (t1011.name.get, t1011.value.toInt),
          (t1112.name.get, t1112.value.toInt),
          (t1213.name.get, t1213.value.toInt),
          (t1314.name.get, t1314.value.toInt),
          (t1415.name.get, t1415.value.toInt),
          (t1516.name.get, t1516.value.toInt),
          (t1617.name.get, t1617.value.toInt),
          (t1718.name.get, t1718.value.toInt),
          (t1819.name.get, t1819.value.toInt),
          (t1920.name.get, t1920.value.toInt),
          (t2021.name.get, t2021.value.toInt),
          (t2122.name.get, t2122.value.toInt),
          (t2223.name.get, t2223.value.toInt),
          (t230.name.get, t230.value.toInt),
        )
      )

    resRDD
  }

  val dataRDD: RDD[Timestamp] =
    readParquet("src/main/resources/yellow_taxi_jan_25_2018")
      .rdd
      // - 360000 => приводим к GMT +03:00
      .map(row => new Timestamp(row.getTimestamp(1).getTime - 3600000))

  val resRDD: RDD[(String, Int)] = getTiming(dataRDD).sortBy(_._2, ascending = false)

  resRDD.foreach(println)

  resRDD
    .map(el => s"${el._1} ${el._2}")
    .saveAsTextFile("src/main/resources/rdd")





}
