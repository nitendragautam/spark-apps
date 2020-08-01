package com.nitendratech.sparkgeneral.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CollectExample {

    public static void main(String args[]){

        // Set Log to Error
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("CollectExample").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        List<String> inputWords = Arrays.asList("spark","hadoop", "hive", "spark", "pig", "hadoop");

        //Create String RDD using
        JavaRDD<String> wordsRDD = sparkContext.parallelize(inputWords);

        //Use collect to get the RDD to driver
        List<String> finalWords = wordsRDD.collect();

        for (String word: finalWords) System.out.println(word);


        //finalWords.forEach(word -> System.out.println(word));
        //OR
        finalWords.forEach(System.out::println);


    }
}
