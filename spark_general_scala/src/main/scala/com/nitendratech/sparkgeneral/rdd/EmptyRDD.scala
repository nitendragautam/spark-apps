package com.nitendratech.sparkgeneral.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by @author nitendratech on 5/16/21
 */
object EmptyRDD {

  def main(args: Array[String]): Unit = {

    //Set the log Level to print only Errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession: SparkSession = SparkSession
      .builder().master("local[*]")
      .appName("RDDParallelize").getOrCreate()


    //Create Empty String RDD
    val emptyStringRDD = sparkSession.sparkContext.parallelize(Seq.empty[String])

    //Create Empty Numerical RDD
    val emptyNumericalRDD = sparkSession.sparkContext.parallelize(Seq.empty[Int])

  }
}
