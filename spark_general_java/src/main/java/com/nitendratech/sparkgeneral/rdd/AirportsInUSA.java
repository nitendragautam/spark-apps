package com.nitendratech.sparkgeneral.rdd;


import org.apache.ivy.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.joda.time.DateTime;

/*
Create a Spark program to read the airport data from datasets/airports.text, find all the airports
which are located in United States and output the airport's name and the city's name to out/airports_in_usa.text.
Each row of the input file contains the following columns:
Airport ID, Name of airport, Main city served by airport, Country where airport is located,
 IATA/FAA code,ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
         Sample output:
         "Putnam County Airport", "Greencastle"
         "Dowagiac Municipal Airport", "Dowagiac"
           ...
 */

/**
 * Command:
 *
 * $SPARK_HOME/bin/spark-submit --class "com.nitendratech.rdd.AirportsInUSA" --master "local[2]" ./target/javaspark-1.0.jar ./datasets/airports.text
 **
 *local[2]: 2 cores
 *local[*]: all available cores
 * local: 1 core
 */
public class AirportsInUSA {

    // regular expression which matches commas but not commas withing double quotations
    private static final String COMMA_DELIMITED=",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]){

        String filePath = args[0];
        Logger.getLogger("org").setLevel(Level.ERROR);

        //Initialize the Spark Conf Object
        SparkConf conf = new SparkConf().setAppName("AirportsInUSA");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //Load the Text file as String RDD, Each item in the String RDD represents a line
        JavaRDD<String> airports = sparkContext.textFile(filePath);

        //Taking the 4th Split which is the country Name, All the country are double quoted
        //Split method returns list of Split results
        // Returns all the lines with Airport as United States
        JavaRDD<String> airportsInUSA = airports.filter(line -> {
                            String splits[] = line.split(COMMA_DELIMITED);
                            return splits[3].equals("\"United States\"");
                        });


        //get the airport name and city name as pair
        JavaRDD<String> airportAndCityNames = airportsInUSA.map(line -> {
                String[] splits = line.split(COMMA_DELIMITED); //Split each line
                return StringUtils.join(new String[]{splits[1],splits[2]},",");

        }
        );


        airportAndCityNames.saveAsTextFile("output/airports_in_usa_"+ DateTime.now().toString("YYMMDDHHSS")+".text");
    }
}
