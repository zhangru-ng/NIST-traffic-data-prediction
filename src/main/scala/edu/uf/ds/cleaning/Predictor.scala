package edu.uf.ds.cleaning

import scala.beans.BeanInfo
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import java.util.Date
import java.io.File

/**
 * @author mebin
 */
object Predictor {
  def main(args: Array[String]): Unit = {
    val OUTPUT_SEPERATOR = "\t"
    val modelFilePath = Configuration.modelFilePath
    val conf = new SparkConf().setAppName("Linear Regression Predictor").setMaster("local[4]")
    val spark = new SparkContext(conf);
    val dataSetPath = Configuration.classifierOutput
    getListOfSubDirectories(dataSetPath).foreach(x => {
      val data = spark.textFile(dataSetPath)
      val max_flow = scala.io.Source.fromFile("max_flow").mkString.trim.toDouble
      //Get all wront values
      val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR)(x.split(OUTPUT_SEPERATOR).length - 2)) == "0" } // filter according to class label
      val regressionModel = LinearRegressionModel.load(spark, modelFilePath)
      val parsedData = filteredData.map { line =>
        val parts = line.split(',')
        val classLabel = parts(3).toDouble
        val date = parts(1)
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val longitude: Double = parts(17).toDouble
        val latitude: Double = parts(16).toDouble
        val time = (format.parse(date).getTime) / (new Date().getTime)
        val point: Array[Double] = Array(time, (longitude + 180) / 360, (latitude + 90) / 180)
        //        LabeledPoint(classLabel/max_flow, Vectors.dense(point))
        Array(line, LabeledPoint(classLabel / max_flow, Vectors.dense(point)))
      }.persist()
      println("before starting!!")
      /*val valuesAndPreds = parsedData.map { point =>
      val prediction = regressionModel.predict(point.features)
      (point.label * max_flow, prediction * max_flow)
    }*/
      val valuesAndPreds = parsedData.map { point =>
        val prediction = regressionModel.predict(point(1).asInstanceOf[LabeledPoint].features)
        (point(0).asInstanceOf[String], prediction * max_flow)
        point(0).asInstanceOf[String] + OUTPUT_SEPERATOR + prediction * max_flow
      }
      valuesAndPreds.saveAsTextFile(Configuration.predictorOutput + "/" + x)
    })
  }
  def getListOfSubDirectories(directoryName: String): Array[String] = {
    (new File(directoryName)).listFiles.filter(_.isDirectory).map(_.getName)
  }
}