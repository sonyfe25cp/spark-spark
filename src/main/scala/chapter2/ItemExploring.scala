package chapter2

import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-13.
 */
object ItemExploring {

  def main(args: Array[String]) {
    val path = "/Users/omar/Downloads"
    val n = Random.nextInt()
    val conf = new SparkConf().setAppName("First Spark App -- " + n)
    val sc = new SparkContext(conf)

    val movie_data = sc.textFile(path + "/ml-100k/u.item")
    println(movie_data.first())
    val num_movies = movie_data.count()
    println("movies : " + num_movies)

    val movie_fields = movie_data.map(_.split("\\|"))

    val years = movie_fields.map(_(2)).map(convert_year(_))

    val years_filtered = years.filter(_ != 1900)

    println(s"years: ${years.count()}, years_filtered: ${years_filtered.count()}")

    //    years_filtered.foreach(println)

    println(years_filtered.getClass.getName)

    val movie_ages = years_filtered.map(1998 - _).countByValue()
    movie_ages.foreach(println)

  }

  def convert_year(year: String): Int = {
    if (year.length() >= 4)
      year.substring(year.length() - 4).toInt
    else
      1900
  }

}
