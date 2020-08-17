package com.nitendratech.sparkgeneral.movieslensTutorials

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, TimestampType}

/**
 * Created by @author nitendratech on 8/15/20
 */
class GetMoviesRelatedData {

  def getMoviesDataset(sparkSession: SparkSession): DataFrame= {

    val moviesSchema = new StructType()
      .add("movieId",IntegerType,true)
      .add("title",StringType,true)
      .add("genres",StringType,true)

    // Movies datasets have three columns called movieId, title and genres
    val dfMovies = sparkSession.read
      .option("header","true")
      .option("delimiter",",")
      .schema(moviesSchema)
      .csv("datasets/ml-movies/movies.csv")

    // Use the method show() to visually inspect some of the data points from our dataframe. It will print 10 lines to the console

    dfMovies.show(10)
    dfMovies

  }


  def getRatingDataset(sparkSession: SparkSession): DataFrame= {

    val ratingSchema = new StructType()
      .add("userId",IntegerType,true)
      .add("movieId",IntegerType,true)
      .add("rating",FloatType,true)
      .add("timestamp",StringType,true)

    sparkSession.read
      .format("csv")
      .option("header","true")
      .option("delimiter",",")
      .schema(ratingSchema)
      .csv("datasets/ml-movies/ratings.csv")

  }


  def getTagsDataset(sparkSession: SparkSession): DataFrame= {


    val tagsSchema = new StructType()
      .add("userId",IntegerType,true)
      .add("movieId",IntegerType,true)
      .add("tag",StringType,true)
      .add("timestamp",StringType,true)

    sparkSession.read
      .option("header","true")
      .option("delimiter",",")
      .schema(tagsSchema)
      .csv("datasets/ml-movies/tags.csv")

  }


  def getLinksDataset(sparkSession: SparkSession): DataFrame= {


    val linksSchema = new StructType()
      .add("movieId",IntegerType,true)
      .add("title",IntegerType,true)
      .add("tmdbId",IntegerType,true)

    sparkSession.read
      .option("header","true")
      .option("delimiter",",")
      .schema(linksSchema)
      .csv("datasets/ml-movies/links.csv")

  }

}
