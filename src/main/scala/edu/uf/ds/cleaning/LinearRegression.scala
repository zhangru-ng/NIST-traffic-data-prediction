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
    val conf = new SparkConf().setAppName("Linear Regression").setMaster("local[4]");
    val spark:SparkContext = new SparkContext(conf);
    val data = spark.textFile(Configuration.classifierOutput)
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-1)) == "1"  } // filter according to class label
    val max_flow = filteredData.map { line => line.split(',')(3).toDouble} max
    val parsedData = filteredData.map { line =>
      val parts = line.split(',')
        val classLabel = parts(3).toDouble
        val date = parts(1)
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val longitude:Double = parts(17).toDouble
        val latitude:Double = parts(16).toDouble
        val time = (format.parse(date).getTime)/(format.parse("2016-01-01 00:00:00").getTime)
        val point:Array[Double] = Array(time, longitude/180, latitude/90)
        LabeledPoint(classLabel/max_flow, Vectors.dense(point))
    }.persist()
    val numIterations = 10000
    val iw = Vectors.dense(0.5, 2, 3, 2, 6, 7)
    val modelTrained = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize = 1/*,miniBatchFraction=0.2,initialWeights=iw*/ )
    
    // Evaluate model on training examples and compute training error
    // build up the pmml evaluator
    val valuesAndPreds = parsedData.map { point =>
      val prediction = modelTrained.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map {
      case (v, p) =>
        println("v and p are - ", v, " nd ",p)
        math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
    // Save model
    println(modelTrained.toPMML())
    modelTrained.save(spark, Configuration.modelFilePath)
  }
}