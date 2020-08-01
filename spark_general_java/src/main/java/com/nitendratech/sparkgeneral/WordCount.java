package com.nitendratech.sparkgeneral;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * Command to Run This:
 *
 * $SPARK_HOME/bin/spark-submit --class "com.nitendratech.WordCount" --master "local[2]" ./target/javaspark-1.0.jar ./datasets/word_count.text
 *
 * */

public class WordCount {

    public static void main(String [] args){


        String filePath = args[0];
        // Set the Log level to Error
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("WordCount Example");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = sparkContext.textFile(filePath);

        JavaRDD<String> words = lines
                                 .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        Map<String, Long> wordCounts = words.countByValue(); // Get the counts for each word

        for (Map.Entry<String, Long> entry: wordCounts.entrySet()){
            System.out.println(entry.getKey() + " : " +entry.getValue());
        }
    }


}
