package com.nitendratech.scalaspark.sparksql

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._


/**
  * Spark SQL to manipulate Fake Friends CSV Data
  */
object FakeFriends {

  case class Person(ID:Int, name: String ,age:Int ,numFriends: Int)


  def lineMapper(line: String): Person = {

    //Split the CSV file according to comma

    val fields = line.split(",")

   Person(fields(0).toInt,fields(1),fields(2).toInt ,fields(3).toInt)
  }
  def main(args:Array[String]): Unit ={


    //Set the log Level to print only Errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Use Spark Session interface in Spark 2.0
    val sparkSession = SparkSession
        .builder()
        .appName("FakeFriendsApp")
        .master("local[*]")
        .getOrCreate()


//Gives a Data Frame from FakeFriendsData ,
    //Read a CSV with header enabled
val fakeFriendsDF = sparkSession.read
  .option("header",true)
  .csv("datasets/fakefriends.csv")


    //Prints the Schema of the CSV File
    //Infer the Schema and Register the Data Sets as Table
fakeFriendsDF.printSchema()


//Create Temp View called "FakeFrinds"

    fakeFriendsDF.createTempView("FakeFriends")

//SQL Query can be run over Data DataDrames that have been registered as a Table.

    //Print all Data
    val selectAll = sparkSession.sql("SELECT * FROM FakeFriends")

val allData = selectAll.collect()

    allData.foreach(item =>{
      println(item)
    })

//Get Teenagers Results as Data Frames
    val teenagers = sparkSession.sql("SELECT * FROM FakeFriends WHERE age >= 13 AND age <=19")


   val teenagersData = teenagers.collect()

    println(" Teenagers Data ")
    teenagersData.foreach(item => println(item))



    //Stop SPark Session
    sparkSession.stop()




  }
}
