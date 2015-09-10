package chapter2

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-13.
 */
object RatingExploring {
  def main(args: Array[String]) {
    val path = "/Users/omar/Downloads"
    val n = Random.nextInt()
    val conf = new SparkConf().setAppName("First Spark App -- " + n)
    val sc = new SparkContext(conf)

    val rating_data_raw = sc.textFile(path + "/ml-100k/u.data")
    println(rating_data_raw.first())
    val num_ratings = rating_data_raw.count()
    println("ratings : " + num_ratings)

    val rating_data = rating_data_raw.map(_.split("\t"))
    val ratings = rating_data.map(_(2).toInt)
    val max_rating = ratings.reduce(Math.max(_, _))
    val min_rating = ratings.reduce(Math.min(_, _))
    val mean_rating = ratings.reduce(_ + _) / num_ratings.toDouble

    val median_rating = ratings.countByValue().toArray.sortBy(-_._2).head._1

    println(s"max_rating: $max_rating, min_rating: $min_rating, mean_rating: $mean_rating, median_rating: $median_rating")

    val count_by_rating = ratings.countByValue()
    count_by_rating.foreach(println)

    val user_ratings_groups = rating_data.map(line => (line(0), line(2))).groupByKey()

    val user_ratings_byuser = user_ratings_groups.map{case (userId, votings) => (userId, votings.size)}

    user_ratings_byuser.take(5).foreach(println)


  }
}
