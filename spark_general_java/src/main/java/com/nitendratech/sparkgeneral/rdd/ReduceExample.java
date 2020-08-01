package com.nitendratech.sparkgeneral.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ReduceExample {

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ReduceExample").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> sampList = Arrays.asList(2,3,4,7,8,10,15,20,35,67);

        JavaRDD<Integer> samRDD = sparkContext.parallelize(sampList);

        // Calling Reduce Action
        Integer product = samRDD.reduce((x,y) -> x*y);

        System.out.println("Product Value: "+product);




    }
}
