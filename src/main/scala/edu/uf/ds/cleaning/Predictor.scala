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
    val modelFilePath = "LinearRegModel"
    val conf = new SparkConf().setAppName("Linear Regression Predictor").setMaster("local[2]");
    val spark = new SparkContext(conf);
    val sameModel = LinearRegressionModel.load(spark, modelFilePath)
    val dataSetPath = "/home/mebin/Downloads/clean_classifier/part-00000"
    val data = spark.textFile(dataSetPath)
    //Get all wront values
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-2)) == "0"  } // filter according to class label
    val regressionModel = LinearRegressionModel.load(spark, modelFilePath)
    
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
        //hour min sec
        val timeFormat = new java.text.SimpleDateFormat("hh:mm:ss")
        val hour:Double = format.parse(date).getHours
        val min:Double = format.parse(date).getMinutes
        val sec:Double = format.parse(date).getSeconds
        val point:Array[Double] = Array(month, year, day, longitude, latitude, hour, min, sec)
        LabeledPoint(classLabel, Vectors.dense(point))
    }.cache()
    println("before starting!!")
    val numIterations = 100
    val valuesAndPreds = parsedData.map { point =>
      val prediction = regressionModel.predict(point.features)
      (point.label, prediction)
    }
    valuesAndPreds.foreach(x => println(x))
  }
}