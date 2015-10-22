package edu.uf.ds.cleaning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author mebin
 */
object Cleaner {
  def main(args: Array[String]): Unit = {
    val filePath = "/home/mebin/Downloads/outputjoin.csv";
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]");
    val spark = new SparkContext(conf);
    val textFile = spark.textFile(filePath)
    val counts = textFile.flatMap(line => line.split(","))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    counts.saveAsTextFile("/home/mebin/Downloads/count.txt")
  }  
}
