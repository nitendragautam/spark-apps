package com.nitendratech.sparkgeneral.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Examples of `count` and `countByValue`
 */
public class CountExample {

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("CountExample").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<String> wordList = Arrays.asList("hadoop", "spark","pig","hadoop","kafka","hadoop");


        JavaRDD<String> wordRDD = sparkContext.parallelize(wordList);


        System.out.println("Total Count: " +wordRDD.count());

        // Get the count for each word
        Map<String,Long> wordCountByValue = wordRDD.countByValue();

        // Loop through the Map and prints the word and count of each word
        for(Map.Entry<String, Long> entry: wordCountByValue.entrySet()){
            System.out.println(entry.getKey() + " : " +entry.getValue());

        }











    }
}
