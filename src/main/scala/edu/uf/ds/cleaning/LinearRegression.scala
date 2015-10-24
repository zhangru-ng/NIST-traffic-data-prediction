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
    val OUTPUT_SEPERATOR = "\t"
    val filePath = "/home/mebin/Downloads/outputjoin.csv";
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]");
    val spark = new SparkContext(conf);
    ///home/mebin/Documents/spark-1.5.0/data/mllib/ridge-data/lpsa.data
    val dataSetPath = "/home/mebin/Downloads/clean_classifier/part-00000"
    val data = spark.textFile(dataSetPath)
    println("The data is " + data)
    println(data)
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length-1)) == "1"  } // filter according to class label
    val parsedData = filteredData.map { line =>
      val parts = line.split(',')
        val classLabel = parts(3).toInt
//        var point: Array[String] = null
//        point = parts.slice(0, parts.length - 1)
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