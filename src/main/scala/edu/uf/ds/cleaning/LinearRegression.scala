package edu.uf.ds.cleaning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import java.io.FileInputStream
import org.apache.spark.mllib.util.Loader
import net.razorvine.pyro.IOUtil
import org.apache.spark.mllib.util.Loader
import net.razorvine.pyro.IOUtil
import sun.nio.ch.IOUtil
import org.apache.spark.mllib.util.Loader

/**
 *
 * @author mebin
 */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val OUTPUT_SEPERATOR = "\t"
    val conf = new SparkConf().setAppName("Linear Regression").setMaster("local[2]");
    val spark:SparkContext = new SparkContext(conf);
    val data = spark.textFile(Configuration.classifierOutput)
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-1)) == "1"  } // filter according to class label
    val parsedData = filteredData.map { line =>
      val parts = line.split(',')
        val classLabel = parts(3).toInt
        val date = parts(1)
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
        val month:Double = format.parse(date).getMonth
        val year:Double = format.parse(date).getYear
        val day:Double = format.parse(date).getDay
        val longitude:Double = parts(17).toDouble
        val latitude:Double = parts(16).toDouble
        //hout min sec
        val timeFormat = new java.text.SimpleDateFormat("hh:mm:ss")
        val hour:Double = format.parse(date).getHours
        val min:Double = format.parse(date).getMinutes
        val sec:Double = format.parse(date).getSeconds
        val point:Array[Double] = Array(month, year, day, longitude, latitude, hour, min, sec)
        LabeledPoint(classLabel, Vectors.dense(point))
    }.cache()
    val numIterations = 10000
    val modelTrained = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize = 0.001)
    // Evaluate model on training examples and compute training error
    // build up the pmml evaluator
    val valuesAndPreds = parsedData.map { point =>
      val prediction = modelTrained.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map {
      case (v, p) =>
        math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
    // Save model
    modelTrained.save(spark, Configuration.modelFilePath)
  }
}