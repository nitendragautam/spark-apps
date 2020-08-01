package com.nitendratech.scalapractice

import java.io.DataOutputStream
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import org.json.JSONObject



class SparkRestAPIService {


  /*
    Submits the spark batch Jobs to spark cluster
    Rest API
     */
  def submitSparkJob(sparkHost:String,jsonPayLoad:String): Unit = {
    val sparkSubmitRestUrl = "http://" + sparkHost + "/v1/submissions/create"


    val postURL = new URL(sparkSubmitRestUrl)
    val connection = postURL.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    val wr = new DataOutputStream(connection.getOutputStream)
    wr.writeBytes(jsonPayLoad)
    wr.flush()
    wr.close()
    //if(connection.getResponseCode==HttpURLConnection.HTTP_OK){
      //Get the Job Id from JSON File}




  }


  /**
    * Gets the Json PayLoad for Submitting Spark Job
    * @param inputPath
    * @param outputPath
    * @param sparkJarPath
    * @param sparkMainClass
    * @param sparkMaster
    * @return
    */

  def getSparkSubmitJsonPayload(inputPath:String,
                        outputPath:String,
                        sparkJarPath:String ,
                        sparkMainClass:String,
                       sparkMaster:String) ={
    val jsonObject = new JSONObject()
      .put("action","CreateSubmissionRequest")
      .put("appArgs",Array(inputPath,outputPath)) //Arguments for Spark
      .put("appResource",sparkJarPath)
      .put("clientSparkVersion","2.0.1")
      .put("environmentVariables",new JSONObject().put("SPARK_ENV_LOADED","1"))
      .put("mainClass",sparkMainClass)
      .put("sparkProperties",new JSONObject()
        .put("spark.jars",sparkJarPath)
        .put("spark.driver.supervise","true")
        .put("spark.app.name","Spark REST API"+getTodaysLatestTime())
        .put("spark.eventLog.enabled","false")
        .put("spark.submit.deployMode","cluster")
        .put("spark.master",sparkMaster)
        .put("spark.executor.memory","4g")
        .put("spark.driver.memory","4g")
        .put("spark.driver.cores","2")



      )
    jsonObject.toString
  }






  def getTodaysLatestTime() :String ={
    val gmt = TimeZone.getTimeZone("GMT")
    val format = new SimpleDateFormat("yyyyMMddHHmmsss")
    format.setTimeZone(gmt)
    val date = Calendar.getInstance().getTime()
    format.format(date)
  }


  /**
    * Submits Spark Job in Spark Cluster
    */
  def submitSParkJob(): Unit ={
    val jsonPayLoad = getSparkSubmitJsonPayload("/home/hduser/NDSBatchApp/input",
      "/home/hduser/NDSBatchApp/output/",
      "/home/hduser/sparkbatchapp.jar",
      "com.nitendragautam.sparkbatchapp.main.Boot",
      "spark://192.168.133.128:7077")



    val sparkHost ="192.168.133.128:6066"

    //Submit Spark Job
    //submitSparkJob(sparkHost,jsonPayLoad)

    //print(sparkSubmitRestUrl)
  }
}
