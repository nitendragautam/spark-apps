package com.nitendratech.sparkgeneral.dataframes

import com.nitendratech.sparkgeneral.utils.SparkSessionContext

/**
 * Created by @author nitendratech on 8/10/20
 */
object DataframeTut extends App  with SparkSessionContext {


  /**
   * Create a DataFrame from reading a CSV file
   * Use Sparksession ad read method to read the CSV file convert into Dataframe
   *
   */

  val tagsDataframe = sparkSession.read
    .option("header", "true") // Get the Schema from the Header
    .option("inferSchema","true")
    .csv("datasets/stack/questions_tags_10k.csv")
    .toDF("id","tag")


  tagsDataframe.show(10) //Print 10 line items to the console

}
