import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-21.
 */
object AmazonApp {

  def main(args: Array[String]) {
    val n = Random.nextInt()
    val conf = new SparkConf().setAppName("First Spark App -- " + n)
    val sc = new SparkContext(conf)

    val path = "/Users/omar/Downloads"
    val movie_data = sc.textFile(path + "/ml-100k/u.item")

  }
}
