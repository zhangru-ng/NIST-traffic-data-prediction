package edu.uf.ds.cleaning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.rdd.RDD
/**
 *
 * @author mebin
 */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val filePath = "/home/mebin/Downloads/outputjoin.csv";
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]");
    val spark = new SparkContext(conf);
    ///home/mebin/Documents/spark-1.5.0/data/mllib/ridge-data/lpsa.data
    val dataSetPath = "/home/mebin/Downloads/clean_classifier/part-00000"
    val data = spark.textFile(dataSetPath)
    println("The data is " + data)
    println(data)
    val filteredData = data.filter { x => (x.split(",")(x.split(",").length-1)).toInt == 1  } // filter according to class label
    val parsedData = filteredData.map { line =>
      val parts = line.split(',')
        val classLabel = parts(parts.length - 1).toInt
        var point: Array[String] = null
        point = parts.slice(0, parts.length - 1)
        LabeledPoint(classLabel, Vectors.dense(point.map(_.toDouble)))
    }.cache()
    val numIterations = 100
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize = 0.001)
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      println("features " + point.features)
      println("label " + point.label)
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map {
      case (v, p) =>
        println("p val is " + p)
        println("v val is " + v)
        math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
    // Save and load model
    model.save(spark, "/home/mebin/Documents/models")
    val sameModel = LinearRegressionModel.load(spark, "/home/mebin/Documents/models")
  }
}