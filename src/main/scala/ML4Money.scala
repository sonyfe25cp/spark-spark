import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by OmarTech on 15-8-21.
 */
object ML4Money {

  def main(args: Array[String]) {
    val n = Random.nextInt()
    val conf = new SparkConf().setAppName("First Spark App -- " + n)
    val sc = new SparkContext(conf)

    val datasource = first1000(sc)
    //    val datasource = crossValidation(sc)

    val trainingData = datasource(0)
    trainingData.cache()
    println("training instances number: " + trainingData.count())
    trainingData.map(_.label).countByValue().foreach(println)


    //    val testData = trainTestSplit(1)
    val testData = datasource(1)
    testData.cache()
    println("test instances number: " + testData.count())
    testData.map(_.label).countByValue().foreach(println)

    //feature standardization
    val vectors = trainingData.map(_.features)
    val matrix = new RowMatrix(vectors)
    val matrixSummary = matrix.computeColumnSummaryStatistics()

    val scaler = new StandardScaler(withMean = false, withStd = true).fit(vectors)
    val scaledData = trainingData.map(point => LabeledPoint(point.label, scaler.transform(point.features)))
    scaledData.cache()

    //train different models
    doExperiments(trainingData, testData)
    doExperiments(scaledData, testData)

  }

  def doExperiments(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    println("=======================================================================")
    val numIterations = 200
    val maxTreeDepth = 5


    val iterResults = Seq(1, 10, 100, 200, 500, 1000).map {
      iteration =>
        Seq(10.0, 1.0, 0.1, 0.01).map {
          step =>
            println("*****************************************************************")
            println(s"Iteration : $iteration")
            println(s"step : $step")
            val lrModel = LogisticRegressionWithSGD.train(trainingData, iteration, step)
            var accuracy = evaluateClassificationModel(lrModel, testData)
            (iteration, step, accuracy)
        }
    }
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    iterResults.sortBy(_(2)).foreach(println)

    val svmModel = SVMWithSGD.train(trainingData, numIterations)
    evaluateClassificationModel(svmModel, testData)

    //    val nbModel = NaiveBayes.train(trainingData)

    //    val dtModel = DecisionTree.train(trainingData, Algo.Classification, Entropy, maxTreeDepth)

    //    evaluateClassificationModel(nbModel, testData)

    //    check(lrModel, testData, 5)

  }

  def prepareDataSet(path: String, sc: SparkContext): RDD[LabeledPoint] = {
    val m_data_raw = sc.textFile(path)

    val m_data = m_data_raw.map(line => line.split("\t"))

    val data = m_data.map {
      r =>
        val label = r(1).toInt
        val features = r.slice(2, r.size).map(_.toDouble)
        LabeledPoint(label, Vectors.dense(features))
    }
    println(s"feature length : ${data.first().features.size}")
    data
  }

  def evaluateClassificationModel(model: ClassificationModel, testData: RDD[LabeledPoint]): Double = {

    val totalCorrect = testData.map {
      point =>
        if (model.predict(point.features) == point.label) 1 else 0
    }.sum

    println(s"${model.getClass.getSimpleName} correct number: $totalCorrect")

    val accuracy = totalCorrect / testData.count()

    println(s"${model.getClass.getSimpleName} accuracy: $accuracy")

    accuracy
  }

  def check(model: ClassificationModel, testData: RDD[LabeledPoint], n: Int) = {
    println("*****************************************************************")
    testData.take(n).map {
      point => (
        point.label,
        model.predict(point.features),
        point.features(0)
        )
    }.foreach(println)
  }

  val first1000Traing = "first1000"
  val crossTraing = "crossTraing"

  def first1000(sc: SparkContext): Array[RDD[LabeledPoint]] = {

    val last300Path = "/tmp/test.data"
    val first1000Path = "/tmp/training.data"

    val training = prepareDataSet(first1000Path, sc)
    val test = prepareDataSet(last300Path, sc)
    Array(training, test)
  }

  def crossValidation(sc: SparkContext): Array[RDD[LabeledPoint]] = {
    val trainingPath = "/Users/omar/Downloads/data.v7.0821.txt"
    val rawData = prepareDataSet(trainingPath, sc)
    val trainTestSplit = rawData.randomSplit(Array(0.8, 0.2), 101)
    trainTestSplit
  }

}
