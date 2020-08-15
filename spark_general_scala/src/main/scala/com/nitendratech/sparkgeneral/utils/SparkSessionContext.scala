package com.nitendratech.sparkgeneral.utils

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
 * Created by @author nitendratech on 8/10/20
 */
trait SparkSessionContext {

  lazy val sparkSession = SparkSession.builder()
    .appName("Learn Spark")
    .master("local[*]")
    .config("spark.cores.max","2")
    .getOrCreate();


}
