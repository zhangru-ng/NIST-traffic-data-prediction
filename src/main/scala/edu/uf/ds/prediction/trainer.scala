package edu.uf.ds.prediction

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import edu.uf.ds.cleaning.Configuration
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.util.Date

/**
 * @author mebin
 */
object trainer {
  
  def writeToFile(p: String, s: String): Unit = {
    val pw = new java.io.PrintWriter(new File(p))
    try pw.write(s) finally pw.close()
  }
  def main(args: Array[String]): Unit = {
    val OUTPUT_SEPERATOR = "\t"
    val conf = new SparkConf().setAppName("Linear Regression").setMaster("local[4]") //remove the local thing
    val spark:SparkContext = new SparkContext(conf);
    val data = spark.textFile(Configuration.classifierOutput)
    val filteredData = data.filter { x => (x.split(OUTPUT_SEPERATOR))(1) == "1"  } // filter according to class label
    val max_flow:Double = (filteredData.map { line => line.split(',')(3).toDouble} max).asInstanceOf[Double]
    writeToFile("max_flow", max_flow.toString)
    val parsedData = filteredData.map { line =>
      val parts = line.split(OUTPUT_SEPERATOR)(0)split(',')
        if(parts.length > 7){
        val classLabel = parts(3).toDouble
        val date = parts(1)
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val longitude:Double = parts(8).toDouble
        val latitude:Double = parts(7).toDouble
        val time = (format.parse(date).getTime)/(new Date().getTime)
        val point:Array[Double] = Array(time, (longitude+180)/360, (latitude+90)/180)
        LabeledPoint(classLabel/max_flow, Vectors.dense(point))
        }
        else{
          LabeledPoint(0, Vectors.dense(Array(0.0, 0.0, 0.0)))
        }
    }.persist()
    val numIterations = 100000000
    val modelTrained = LinearRegressionWithSGD.train(parsedData, numIterations)
    
    // Evaluate model on training examples and compute training error
    // build up the pmml evaluator
    val valuesAndPreds = parsedData.map { point =>
      val prediction = modelTrained.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map {
      case (v, p) =>
//        println("v and p are - ", v, " nd ",p)
        math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)
    // Save model
    modelTrained.save(spark, Configuration.modelFilePath)
  }

  
}