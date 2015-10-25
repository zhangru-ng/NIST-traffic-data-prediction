package edu.uf.ds.cleaning

import com.typesafe.config.ConfigFactory

/**
 * @author mebin
 */
object Configuration {
  val conf = ConfigFactory.load()
  val modelFilePath = conf.getString("modelPath")
  val joinInputFilePath = conf.getString("joinInputFile")
  val classifierOutput = conf.getString("classifierOutput")
}