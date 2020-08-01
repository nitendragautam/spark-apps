package com.nitendratech.scalaspark.sparksql

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession


/**
  * Data Frames Example
  */
object DataFrames {

  def main(args: Array[String]): Unit = {

    //Set the Log Levek to print Errors

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a Spark Session Interace in Spark 2.0
    val sparkSession = SparkSession.builder()
        .appName("DataFrame")
        .master("local[*]")
        .getOrCreate()

    // Convert our csv file to a Data Frame using our Person Case
    val fakeFriendsDF = sparkSession.read
      .option("header" ,true)
      .csv("datasets/fakefriends.csv")

    //Print the SQL SChema
    fakeFriendsDF.printSchema()

    //Select the Name Column
    println("Let's select the name column:")
    fakeFriendsDF.select("name").show()

    println("Filter out anyone over 21:")
    fakeFriendsDF.filter(fakeFriendsDF("age") < 21).show()

    println("Group By Age:")
    fakeFriendsDF.groupBy("age").count().show()

    println(" Make everyone 10 years older: ")
    fakeFriendsDF.select(fakeFriendsDF("name") ,fakeFriendsDF("age") + 10).show()

    sparkSession.stop()

  }
}
