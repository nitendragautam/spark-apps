package com.nitendratech.sparkgeneral.utils

import org.apache.spark.sql.SparkSession

/**
 * Created by @author nitendratech on 1/18/21
 */
object SparkUtils {


  def withSpark(func: SparkSession => Unit):Unit= {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Test")
      .config("spark.ui.enabled","false")
      .getOrCreate()
    sparkSession.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func (sparkSession)
    } finally {
      sparkSession.stop()
      System.out.println("spark.driver.port")
    }
  }
}
