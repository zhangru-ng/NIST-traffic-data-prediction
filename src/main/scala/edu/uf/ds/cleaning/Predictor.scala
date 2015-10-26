package edu.uf.ds.cleaning

import scala.beans.BeanInfo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel

/**
 * @author mebin
 */
object Predictor {
  def main(args: Array[String]): Unit = {
    val OUTPUT_SEPERATOR = "\t"
    val modelFilePath = Configuration.modelFilePath
    val conf = new SparkConf().setAppName("Linear Regression Predictor").setMaster("local[4]")
    val spark = new SparkContext(conf);
    val sameModel = LinearRegressionModel.load(spark, modelFilePath)
    val dataSetPath = Configuration.classifierOutput
    val data = spark.textFile(dataSetPath)
    val filteredData_1 = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-1)) == "1"  } // filter according to class label
    val max_flow = filteredData_1.map { line => line.split(',')(3).toDouble} max
    //Get all wront values
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-2)) == "0"  } // filter according to class label
    val regressionModel = LinearRegressionModel.load(spark, modelFilePath)
//    filteredData.foreach(x => println(x))
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
    println("before starting!!")
//    val numIterations = 100
    val valuesAndPreds = parsedData.map { point =>
      val prediction = regressionModel.predict(point.features)
      (point.label, prediction)
    }
    valuesAndPreds.foreach(x => println(x))
  }
}