package edu.uf.ds.cleaning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

/**
 * @author mebin
 */

object Cleaner {
  val OUTPUT_SEPERATOR = ","
/*  def redIterable(list: Iterable[String]): String = {
    var reason = ""
    for (value <- list) {
      reason = value
      var description: String = null
      if (value.split(",").length > 0 && value.split(",")(0).toInt < 0) description = "speed is negative"
      if (value.split(",").length > 1 && value.split(",")(1).toInt < 0)
        if (description != null) description += OUTPUT_SEPERATOR + "flow is negative" else description = "flow is negative"
      if (value.split(",").length > 2 && value.split(",")(2).toInt < 0)
        if (description != null) description += OUTPUT_SEPERATOR + "occupancy is negative" else description = "occupancy is negative"
      if (null != description && !description.isEmpty())
        reason += OUTPUT_SEPERATOR + description + OUTPUT_SEPERATOR + "0"
      else
        reason += OUTPUT_SEPERATOR + "1"
    }
    reason.mkString
  }
*/
  def reduce(listOfLines: Iterable[String]): String = {
    var result = ""
    var mean = 0
    var count = 0;
    
    for (line <- listOfLines) {
      var flow = line.split(",")(3).toInt
    }
    for (line <- listOfLines) {
      var flow = line.split(",")(3).toInt
      if (flow < 0) {
        result += line + OUTPUT_SEPERATOR + "0" + OUTPUT_SEPERATOR + "Flow is negative"
      } else {
        mean += line.split(",")(3).toInt
        count += 1
        result += line + OUTPUT_SEPERATOR + "1"
      }
    }
    mean = mean/count
    result.mkString
  }
  def main(args: Array[String]): Unit = {
    val filePath = "/home/mebin/Downloads/outputjoin.csv";
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]");
    val spark = new SparkContext(conf);
    val textFile: RDD[String] = spark.textFile(filePath)
    val format = new java.text.SimpleDateFormat("hh:mm") //07:05:08-05
    //one minutes
    val mapOutput = textFile.map(line => (line.split(",")(6) + line.split(",")(1).split(" ")(0) + ((format.parse(line.split(",")(1).split(" ")(1)).getHours * 60) + format.parse(line.split(",")(1).split(" ")(1)).getMinutes) / 1,
      line))
      .groupByKey().mapValues(reduce)
    mapOutput.map(x => x._1 + "," + x._2).saveAsTextFile("/home/mebin/Downloads/clean_classifier")

    //TODO : Put file with correct label in different file and update linear regression
  }

}
