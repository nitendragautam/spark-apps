package com.nitendratech.sparkgeneral.blog


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
 * Created by @author nitendratech on 5/24/20
 */
object SparksesionExample {

  def main(args: Array[String]): Unit = {

    //Setting Log Level as Error
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder()
                    .config("spark.executor.memory", "2g")
                      .appName("SparkExample")
                      .master("local[2]")
                      .getOrCreate()

    println(sparkSession.version)
    println("Getting Spark session State ")
    println(sparkSession.sessionState.conf)

  }
}
