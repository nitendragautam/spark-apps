package com.nitendragautam

import com.nitendratech.scalapractice.SparkRestAPIService


/**
  * Created by nitendragautam on 4/29/2018.
  */
object SparkSubmittest {


  def main(args: Array[String]): Unit ={

    val sparkRestService = new SparkRestAPIService

    val jsonPayLoad = sparkRestService.getSparkSubmitJsonPayload("/home/hduser/NDSBatchApp/input",
      "/home/hduser/NDSBatchApp/output/",
      "/home/hduser/sparkbatchapp.jar",
      "com.nitendragautam.sparkbatchapp.main.Boot",
    "spark://192.168.133.128:7077")

    println(jsonPayLoad)

    val sparkHost ="192.168.133.128:6066"
    val sparkSubmitRestUrl = "http://" + sparkHost + "/v1/submissions/create"

    //print(sparkSubmitRestUrl)
  }



}
