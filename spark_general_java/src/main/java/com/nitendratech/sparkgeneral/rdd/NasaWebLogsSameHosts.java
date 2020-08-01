package com.nitendratech.sparkgeneral.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

/*
        "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for
                July 1st, 1995.
          "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
          Create a Spark program to generate a new RDD which contains the hosts which are accessed on
          BOTH days.
          Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.
          Example output:
          vagrant.vf.mmc.com
          www-a1.proxy.aol.com
          .....
          Keep in mind, that the original log files contains the following header lines.
          host	logname	time	method	url	response	bytes
          Make sure the head lines are removed in the resulting RDD.
        */
public class NasaWebLogsSameHosts {

    public static void main(String args[]){

        SparkConf conf = new SparkConf().setAppName("NasaWebLogsSameHosts").setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Read the TSV File

        /*
        Since we are only interested in the host name, so we do a map transformation on both input RDDs,
         split the original
         log lines using tab as the delimiter, then we return the first column which is the host name.
         */
        JavaRDD<String> julyLogs = sparkContext.textFile("datasets/nasa_19950701.tsv")
                                                .map(line -> line.split("\t")[0]);

        JavaRDD<String> augLogs = sparkContext.textFile("datasets/nasa_19950801.tsv")
                                                .map(line -> line.split("\t")[0]);;

        //Intersection the Two RDD based on host value

        /*
        Do an intersection operation on the two host names RDDs which
        should return us the common host names between the two RDDs.
         */
        JavaRDD<String> intersectionLogs = julyLogs.intersection(augLogs)
                                            .filter(line -> !line.equals("host")); //each line has value of host in it

        // Save the Log file
    intersectionLogs.saveAsTextFile("outputs/nasa_logs_same_hosts_"+ new Date().getTime()+".csv");



    }


}
