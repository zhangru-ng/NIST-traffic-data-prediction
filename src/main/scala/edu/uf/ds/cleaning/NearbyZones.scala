package edu.uf.ds.cleaning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.io.Source
/**
 * @author sudeep
 */
object NearbyZones {
  /*
   * 
   * 
   * Emitter function used in Map
   */
  //Check if row has 1 value in the correct/incorrect field
  //Send current zone's flow id to the neighbours only once per period 
  def emitNeighbourFlows(line:String, neighbourMap:scala.collection.immutable.Map[Int, Array[Int]]): List[(String,String)] = {
    
      var result = new ListBuffer[(String,String)]
      val row = line.split("\t")(0)
      val prediction = line.split("\t")(1)
      val splitted = row.split(",")
      val zone_id = splitted(6).toInt
      val flow = splitted(3).toInt
      val format = new java.text.SimpleDateFormat("hh:mm") //07:05:08-05
      //Zone-id + Date + minutes
      val abc = splitted(6) + splitted(1).split(" ")(0) + 
            ((format.parse(splitted(1).split(" ")(1)).getHours * 60) + 
                format.parse(splitted(1).split(" ")(1)).getMinutes)
                
        //Add the zone id of current line and send entire row
        result+= ((abc,line))
        
        if(prediction.toInt >0) {
          //println("Prediction vaule 1" + row)
        //Lane-number
            if(splitted(7).toInt == 1) {
               neighbourMap(zone_id).foreach{
               x => {
                val abc = x + splitted(1).split(" ")(0) + 
                  ((format.parse(splitted(1).split(" ")(1)).getHours * 60) + 
                      format.parse(splitted(1).split(" ")(1)).getMinutes)
            
                result+= ((abc,flow.toString()))
                }
            }
          }
        }
      result.toList   
  }

  val OUTPUT_SEPERATOR = "\t"
  
  
  /*
   * 
   * Reduce Function
   */
   def reduce(listOfLines: Iterable[String]): String = {
    
    
    var linesBuffer = new ListBuffer[String]

    //List of neighbours flow values
    val result = new ListBuffer[Int]
    
    
    var res = new ListBuffer[String]
    

    //var newLine = new String
    for (resultLine <- listOfLines) {
              //println(line)
      
              //Only flow value present in the string
         if(resultLine.split(",").size ==1) {
            result+= resultLine.toInt
			    } else {
			        //Output of previous join should be 1
			        if(resultLine.split("\t")(1).toInt > 0 ) {
			          //println("Prediction value correct"+resultLine)
			          result+= resultLine.split(",")(3).toInt
			          linesBuffer.append(resultLine)
			        } else {
			          res+=(resultLine)    
			        }
				      
				      
				 }
    }
    var isNext = false

    var mean:Double = 0
    
    if(result.size == 0) {
      return res.mkString
    }
    mean = result.sum/result.size
    val devs = result.map(score => (score - mean) * (score - mean))
    val stddev = Math.sqrt(devs.sum / result.size)
    
    
    for(newLine <- linesBuffer) {
      val flow = newLine.split(",")(3).toInt
      //if(isNext){
      //   res += "\n"
      //}
      if(newLine.split("\t")(1).toInt == 0) {
        res+= newLine			        
        
      } else if (Math.abs(flow-mean) <= 3.0 * stddev ) {
         //println("correct value " + newLine.split("\t")(0) + "mean: " + mean.toString() + "stddev: " + stddev.toString())

          res+= newLine.split("\t")(0) + OUTPUT_SEPERATOR + "1" + OUTPUT_SEPERATOR + "Correct"
      } else {
          println("Incorrect value " + newLine + "mean: " + mean.toString() + "stddev: " + stddev.toString())
          res+= newLine.split("\t")(0) + OUTPUT_SEPERATOR + "0" + OUTPUT_SEPERATOR + "InCorrect nearby zone flows not similar"
      }
      
            isNext = true

    }
    return res.mkString
    
        //result
  }
  def main(args: Array[String]): Unit = {
    
    val filePath = "/home/sudeepgaddam/data_science/labs/nist/data/clean_classifier/";
        val conf = new SparkConf().setAppName("NearbyGroups").setMaster("local[2]");
    val spark = new SparkContext(conf);

    val textFile: RDD[String] = spark.textFile(filePath)

    val nearbyPath = "/home/sudeepgaddam/data_science/labs/nist/data/nearby.csv";
    /*for(line <- Source.fromFile(nearbyPath).getLines()){
        println(line)
    }*/
    val keyValuePairs = scala.io.Source.fromFile(nearbyPath, "UTF8")
       .getLines.map(_.stripLineEnd.split("\t", -1))
       
       //.map(fields => fields(0) -> fields(1)).toList
    .map(fields => fields(0).toInt -> fields(1).drop(1).dropRight(1).split(", ").map(_.toInt)).toList
    val neighbourMap = Map(keyValuePairs : _*)
   // neighbourMap foreach ( (t2) => println (t2._1 + "-->" + t2._2.foreach (x => print(x+","))))
    
    neighbourMap(3483).foreach{
      x => println(x+",")
    }
    val mapOutput = textFile.flatMap(line => (emitNeighbourFlows(line,neighbourMap))).groupByKey().mapValues(reduce)
    mapOutput.map(x => x._2).saveAsTextFile("/home/sudeepgaddam/data_science/labs/nist/data/nearby")

    
    //val cache = collection.mutable.Map[Int, Int]()
  
  }
}
/*
	*  [lane_id,
 measurement_start,
 speed,
 flow,
 occupancy,
 quality,
 zone_id,
 lane_number,
 name,
 state,
 road,
 direction,
 location_description,
 lane_type,
 organization,
 detector_type,
 latitude,
 longitude,
 bearing,
 default_speed,
 interval]
 */
 
