package com.nitendratech.sparkgeneral.ingestion;

/**
 * Created by @author nitendratech on 5/24/20
 */

import com.nitendratech.sparkgeneral.util.AppUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * Converts a input Array into Dataset<String>
 */
public class ArrayToDataSet {

    public static void main(String args[]){

        ArrayToDataSet arrayToDataSet = new ArrayToDataSet();

        arrayToDataSet.startSparkJob();
    }


    private void startSparkJob(){

        SparkSession sparkSession = AppUtil.getSparkSession("ArrayToDataSet","local[*]");

        String[] inputArray = new String[]{"Hello","World","This","Is","Array"};

        List<String> inputList = Arrays.asList(inputArray);

        Dataset<String> ds = sparkSession.createDataset(inputList, Encoders.STRING());



        ds.show();

        /**
         * It will print belwo
         * +-----+
         * |value|
         * +-----+
         * |Hello|
         * |World|
         * | This|
         * |   Is|
         * |Array|
         * +-----+
         */

    }


}
