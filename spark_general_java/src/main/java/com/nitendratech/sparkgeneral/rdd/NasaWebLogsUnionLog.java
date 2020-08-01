package com.nitendratech.sparkgeneral.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

/* "datasets/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server
for July 1st, 1995.
           "datasets/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from
           both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.
           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes
           Make sure the head lines are removed in the resulting RDD.
         */
public class NasaWebLogsUnionLog {

    public static void main(String [] args){


        SparkConf conf = new SparkConf().setAppName("NasaWebLogsUnionLog").setMaster("local[1]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Set the log to Error
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Read the TSV File

        JavaRDD<String> julyLogs = sparkContext.textFile("datasets/nasa_19950701.tsv");

        JavaRDD<String> augLogs = sparkContext.textFile("datasets/nasa_19950801.tsv");

        //Union the two RDDs
        JavaRDD<String> aggregratedRDD = julyLogs.union(augLogs);

        // Filter the header using the function
        JavaRDD<String> cleanLogsLines = aggregratedRDD.filter(line -> isNotHeader(line));


        // Sampling
        JavaRDD<String> sampleLogs = cleanLogsLines.sample(true,0.1);

        sampleLogs.saveAsTextFile("outputs/sample_nasa_logs_" + new Date().getTime()+".csv");










    }


    //Checks if the line is header or not
    private static boolean isNotHeader(String line){
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
