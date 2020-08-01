package com.nitendratech.sparkgeneral;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Word Count Example
 */
public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String [] args){

        if(args.length<1){
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }


        //Get the Spark Session
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        // Read the text File
        JavaRDD<String> inputLines = spark.read().textFile(args[0]).javaRDD();

        //Split the Lines into Words
        JavaRDD<String> words = inputLines
                .flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        //Maps each word to a tuple
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s,1));

        //Reduces by every key(word for ths one)
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1+i2);


        List<Tuple2<String,Integer>> output = counts.collect();

        for (Tuple2<?,?> tuple: output){
            // Prints the word and its total count. tuple._1() -> Word and tuple._2() -> Count
            System.out.println(tuple._1() + ":" + tuple._2());
        }
    }
}