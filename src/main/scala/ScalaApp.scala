import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-11.
 */
object ScalaApp {

  def say(name: String) = s"hello, $name"

  def main(args: Array[String]) {
    val n = Random.nextInt()
    val conf = new SparkConf()
      //      .setMaster("spark://ubuntu-107:7077")
      //      .setMaster("local[4]")
      .setAppName("First Spark App -- " + n)
    //      .setJars(List("target/scala-2.10/sparkspark_2.10-1.0.jar"))
    val sc = new SparkContext(conf)
    val data = sc.textFile("/tmp/userhistory.data")
      .map(line => line.split(","))
      .map(
        purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2))
      )
    //    println(data.count() + "************************************************")
    println(data.count())
    val numberPurchase = data.count()
    val uniqueUsers = data.map { case (user, product, price) => user }.distinct().count()
    val totalRevenue = data.map { case (user, product, price) => price.toDouble }.sum()
    val productsByPopularity = data
      .map { case (user, product, price) => (product, 1) }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)
    val mostPopular = productsByPopularity(0)
    println(mostPopular + " ----------------------------------- ")
  }
}
