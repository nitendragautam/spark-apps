package com.nitendratech.sparkgeneral.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/* Create a Spark program to read the first 100 prime numbers from datasets/prime_nums.text,
          print the sum of those numbers to console.
          Each row of the input file contains 10 prime numbers separated by spaces.
        */
public class SumOfNumbersProblem {

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("SumOfNumbersProblem").setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = javaSparkContext.textFile("datasets/prime_nums.text");

        //Flatten the results and get the numbers
        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        // Check of numbers are Valid and see if numbers are empty or not
        JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());

        // Using Lambda
        //JavaRDD<Integer> integerNumbers = validNumbers.map(number -> Integer.valueOf(number));

        // Using Method Reference and Map Transformation to all the items in RDD
        JavaRDD<Integer> integerNumbers = validNumbers.map(Integer::valueOf);

        // Get the sum
        Integer totalSum = integerNumbers.reduce((x,y) -> x+y);


        System.out.println("Total Sum is: " +totalSum);
    }
}
