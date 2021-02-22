import DFPractice.readParquet
import RDDPractice.getTimeDistributionByHours
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec

class RDDPractiseTest extends AnyFlatSpec with SparkSessionTestWrapper {

  it should "successfully calculate the distribution of taxi rides by hour" in {

    val dataRDD: RDD[Row] = readParquet("src/test/resources/yellow_taxi_jan_25_2018").rdd
    val actualResRDD: RDD[(Int, Int)] = getTimeDistributionByHours(dataRDD)

    val expectedResData: Seq[(Int, Int)] = Seq(
      (18,22121), (19,21598), (21,20884), (20,20318), (22,19528), (8,18867),
      (17,18664), (15,17843), (14,17483), (9,16840), (16,16160), (13,16082),
      (12,16001), (11,15564), (7,15445), (10,15348), (23,14652), (6,8600),
      (0,7050), (1,3978), (5,3133), (2,2538), (3,1610), (4,1586)
    )
    val expectedResRDD: RDD[(Int, Int)] = spark.sparkContext.parallelize(expectedResData)

    assert(actualResRDD.collect sameElements expectedResRDD.collect)
  }

}
