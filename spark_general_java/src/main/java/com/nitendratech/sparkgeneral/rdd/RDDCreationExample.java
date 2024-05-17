package com.nitendratech.sparkgeneral.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDDCreationExample {

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("RDDCreationExample").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<String> fruitList = Arrays.asList("apple", "orange","banana","pear",
                                            "apple","berry","mango","blueberry");
        // Create RDD using parallelize keyword
        JavaRDD<String> wordRDD = sparkContext.parallelize(fruitList);

        // Number Example
        List<Integer> numberList = Arrays.asList(2,3,4,7,8,10,15,20,35,67);

        //Create number RDD
        JavaRDD<Integer> numRDD = sparkContext.parallelize(numberList);


        // Get the fruits RDD without apple, Creating RDD from RDD
        JavaRDD<String> noAppleRDD =wordRDD.filter(fruits -> !fruits.equals("apple"));

        System.out.println("Total Fruit Count: " +wordRDD.count());

        System.out.println("Total Fruit Count Without apple: " +noAppleRDD.count());

        // Get the count for each word
        Map<String,Long> wordCountByValue = wordRDD.countByValue();

        // Loop through the Map and prints the word and count of each word
        for(Map.Entry<String, Long> entry: wordCountByValue.entrySet()){
            System.out.println(entry.getKey() + " : " +entry.getValue());

        }


        // Calling Reduce Action
        Integer sumNumbers = numRDD.reduce((x,y) -> x*y);

        System.out.println("Product Value: "+sumNumbers);


    }
}
