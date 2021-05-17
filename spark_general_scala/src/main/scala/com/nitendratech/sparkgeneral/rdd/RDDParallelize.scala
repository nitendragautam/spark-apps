package com.nitendratech.sparkgeneral.rdd


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by @author nitendratech on 5/16/21
 */

object RDDParallelize {

  def main(args: Array[String]): Unit = {

    //Set the log Level to print only Errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession: SparkSession = SparkSession
                      .builder().master("local[*]")
                        .appName("RDDParallelize").getOrCreate()

    val numList =List(1,2,3,4,5)
    val numericalRDD:RDD[Int] = sparkSession.sparkContext.parallelize(numList)
      println("Number of RDD Partitions: "+numericalRDD.getNumPartitions)
    println("Print First Element from RDD: "+numericalRDD.first() )
    // Print the Numerical Elements in the RDD
    numericalRDD.collect().foreach(println)

    val fruitList = List("apple", "orange","banana","pear", "apple","berry","mango","blueberry")
    val stringRDD:RDD[String] = sparkSession.sparkContext.parallelize(fruitList)
    println("Number of RDD Partitions: "+stringRDD.getNumPartitions)
    println("Print First Element from RDD: "+stringRDD.first() )
    // Print the String Elements in the RDD
    stringRDD.collect().foreach(println)

    //Get the fruits RDD without apple, Creating RDD from RDD
    val noAppleRDD =
      stringRDD.filter( stringRDD => !stringRDD.equals("apple"))

    println(" No Apple RDD\n")

    noAppleRDD.collect().foreach(println)



  }

}
