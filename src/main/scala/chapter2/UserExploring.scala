package chapter2

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-13.
 */
object UserExploring {

  def main(args: Array[String]) {
    val path = "/Users/omar/Downloads"
    val n = Random.nextInt()
    val conf = new SparkConf().setAppName("First Spark App -- " + n)
    val sc = new SparkContext(conf)

    val user_data = sc.textFile(path + "/ml-100k/u.user")

    println("first line:" + user_data.first())
    // 1|24|M|technician|85711

    val user_fields = user_data.map(line => line.split("\\|"))

//    user_fields.map{line => line(2)}.foreach(println)

    val num_users = user_fields.map(fields => fields(0)).distinct().count()

    val num_ages = user_fields.map(fields => fields(1)).distinct().count()

    val num_genders = user_fields.map(fields => fields(2)).distinct().count()

    val num_occupations = user_fields.map(fields => fields(3)).distinct().count()

    val num_zipcodes = user_fields.map(fields => fields(4)).distinct().count()

    println("Users: " + num_users + " genders: " + num_genders + " ages: "+ num_ages +
      ", occupations: " + num_occupations + ", ZIP codes: " + num_zipcodes)

    val total = user_fields.map(ages => Integer.parseInt(ages(1))).collect().sum

    val average = total / num_users

    println("total : "+ total +" average : " + average)


    println("the distribution of occ")
    user_fields.map(occ=> (occ(3), 1))
      .reduceByKey(_+_)
      .collect()
      .sortBy(_._2)
      .foreach(println)

    println("the distribution of age")
    user_fields.map(occ=> (occ(1), 1))
      .reduceByKey(_+_)
      .collect()
      .sortBy(_._2)
      .foreach(println)
  }
}
