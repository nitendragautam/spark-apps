package com.nitendratech.sparkgeneral.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.ivy.util.StringUtils;
import org.joda.time.DateTime;

/* Create a Spark program to read the airport data from datasets/airports.text,
find all the airports whose latitude are bigger than 40.
   Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
 Each row of the input file contains the following columns:
  Airport ID, Name_of_airport, Main_city_served by airport, Country where airport is located, IATA/FAA code,
  ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

/**
 * Run:
 * $SPARK_HOME/bin/spark-submit --class "com.nitendratech.rdd.AirportByLatitude" --master "local[2]" ./target/javaspark-1.0.jar ./datasets/airports.text
 */
public class AirportByLatitude {

    // regular expression which matches commas but not commas withing double quotations
    private static final String COMMA_DELIMITED=",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]){

        String filePath = args[0];

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("AirportByLatitude");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //Load the input file as String RDD
        JavaRDD<String> airportRDD =sparkContext.textFile(filePath);

        //Filter the data whose 7th column(Latitude)>40
        JavaRDD<String> linesWithLatGreatForty = airportRDD.filter(line ->{
            String [] splits = line.split(COMMA_DELIMITED);

            // Converting the value from String to Float
            return Float.parseFloat(splits[6])>40;
        });


        //Loop through the RDD with latitude greater than forty after applying Map Transformation
        JavaRDD<String> airportWithLatitude=linesWithLatGreatForty.map(line ->{
            String splits[] = line.split(COMMA_DELIMITED);
            //Get the Airport name and Latitude in the final RDD
            return StringUtils.join(new String[]{splits[1],splits[6]},",");
        });

        //Write the RDD in the output file,
        airportWithLatitude.coalesce(1).saveAsTextFile("output/airport_by_latitude_"+ DateTime.now().toString("YYMMDDHHSS")+".text");





    }


}
