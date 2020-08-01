package com.nitendratech.sparkgeneral.rdd;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class PersistanceEx {

public static void main(String args[]){

    // Set the Log Level to Error
    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("PersistanceEx").setMaster("local[*]");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    List<Integer> numList = Arrays.asList(2,3,4,5,6,8,9,11,13,16,18);

    //Create an RDD by using parallelize keyword
    JavaRDD<Integer> numRDD = sparkContext.parallelize(numList);

    // Persist the RDD in memory
    numRDD.persist(StorageLevel.MEMORY_ONLY());

    // Get the count of total numbers in RDD
    Long numCount = numRDD.count();

    System.out.println("Count Value from memory: "+numCount);

    // Apply the reduce operation
    // Get the sum of all numbers
    Integer sumRDD = numRDD.reduce((x, y) -> x+y);

    System.out.println("Sum of Numbers Value: "+sumRDD);

}
}
