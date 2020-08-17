package com.nitendratech.sparkgeneral.movieslensTutorials

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.zookeeper.Op.Create

/**
 * Created by @author nitendratech on 8/15/20
 */

/**
 * Tutorials for DataFrame based on Movies Data
 */
object DataFrameSQLTutorials {



  def main(args: Array[String]): Unit = {


    val getMoviesRelatedData = new GetMoviesRelatedData


    /**
     * To connect to a Spark cluster, we need to create a spark session
     */
    // Get the Spark Session
    val sparkSession = SparkSession.builder()
                      .master("local[*]")
                      .appName("MovieLensData")
                      .getOrCreate()


    //getMoviesRelatedData.getRatingDataset(sparkSession).show();

    //getMoviesRelatedData.getTagsDataset(sparkSession).show();


    //getMoviesRelatedData.getLinksDataset(sparkSession).show();

    // Create DataSet from CSV File File

    val moviesDataLoad = getMoviesRelatedData.getMoviesDataset(sparkSession)

    /** Output of moviesDataLoad.show(10)
     *
     * +-------+--------------------+--------------------+
     * |movieId|               title|              genres|
     * +-------+--------------------+--------------------+
     * |      1|    Toy Story (1995)|Adventure|Animati...|
     * |      2|      Jumanji (1995)|Adventure|Childre...|
     * |      3|Grumpier Old Men ...|      Comedy|Romance|
     * |      4|Waiting to Exhale...|Comedy|Drama|Romance|
     * |      5|Father of the Bri...|              Comedy|
     * |      6|         Heat (1995)|Action|Crime|Thri...|
     * |      7|      Sabrina (1995)|      Comedy|Romance|
     * |      8| Tom and Huck (1995)|  Adventure|Children|
     * |      9| Sudden Death (1995)|              Action|
     * |     10|    GoldenEye (1995)|Action|Adventure|...|
     * +-------+--------------------+--------------------+
     *
     */


    /* Print the Schema of the Dataframe
     We can call the method printSchema() on the dataframe moviesDataLoad to show the dataframe schema which was used to create
     Dataframe
     */


    moviesDataLoad.printSchema()

    /**
     * We defined the schema through StructType Method
     * root
     * |-- movieId: integer (nullable = true)
     * |-- title: string (nullable = true)
     * |-- genres: string (nullable = true)
     */


    /**
     * DataFrame Query: select columns from a Dataframe
     * We can use the select() method to select specific columns from Dataframe.
     * We need to pass in the columns which we want to select
     */


    // Query dataframe: select columns from a dataframe
    moviesDataLoad.select("movieId","genres").show(10)

    /**
     * We should see some data like below.
     * +-------+--------------------+
     * |movieId|              genres|
     * +-------+--------------------+
     * |      1|Adventure|Animati...|
     * |      2|Adventure|Childre...|
     * |      3|      Comedy|Romance|
     * |      4|Comedy|Drama|Romance|
     * |      5|              Comedy|
     * |      6|Action|Crime|Thri...|
     * |      7|      Comedy|Romance|
     * |      8|  Adventure|Children|
     * |      9|              Action|
     * |     10|Action|Adventure|...|
     * +-------+--------------------+
     */


    /**
     * DataFrame Query: filter by column value of a Dataframe
     *
     * We can use the Filter filter() method  to find all rows matching a specific column value
     *  For example, let's find all rows where the genre column has a value of Action.
     */


    // DataFrame Query: filter by column value of a Dataframe
    moviesDataLoad.filter("genres === 'Action'")


  }

}
