import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

//从kafka中取内容并计算词
object KafkaWordCount {
  def main(args: Array[String]) {
    //    if (args.length < 4) {
    //      System.err.println("Usage: KafkaWordCount <zkQuorum>     <group> <topics> <numThreads>")
    //      System.exit(1)
    //    }

    //    StreamingExamples.setStreamingLogLevels()

    //val Array(zkQuorum, group, topics, numThreads) = args
    val zkQuorum = "10.1.0.171:2181"
    val group = "1"
    val topics = "test"
    val numThreads = 2

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_, numThreads)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)

    //val wordCounts = words.map(x => (x, 1L))
    //  .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
