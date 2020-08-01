package com.nitendratech.sparkgeneral.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeActionExample {


    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("TakeActionExample").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<String> wordList = Arrays.asList("spark","hadoop","spark","kafka","scala","Java","Pig");

        // Create RDD from List

        JavaRDD<String> wordRDD = sparkContext.parallelize(wordList);

        // Take 3 sample using take function
        List<String> sampleList= wordRDD.take(3);

        sampleList.forEach(System.out::println);

    }
}
