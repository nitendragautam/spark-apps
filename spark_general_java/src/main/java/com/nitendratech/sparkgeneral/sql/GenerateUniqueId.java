package com.nitendratech.sparkgeneral.sql;

/**
 * Created by @author nitendratech on 5/16/20
 */

import com.nitendratech.sparkgeneral.domain.ShoppingItem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

/**
 * Generates a Unique IDs for each rows in a Spark DataFrame
 */
public class GenerateUniqueId {

    public static void main(String args[]){

        SparkSession sparkSession = SparkSession.builder().appName("GenerateUniqueId")
                                        .master("local[1]")
                                    .getOrCreate();



        Dataset<Row> shoppingDataset =getShoppingDataFrame(sparkSession);


        getUniqueIdMid(shoppingDataset);

        getUniqueIdUsingLit(shoppingDataset);


    }

    private static void getUniqueIdMid(Dataset<Row> dataSet){

        dataSet.withColumn("uniqueId",functions.monotonicallyIncreasingId()).show();



    }

    public static void getUniqueIdUsingLit(Dataset<Row> dataSet){

        dataSet.withColumn("uniqueId",functions.lit(java.util.UUID.randomUUID().toString())).show();

    }



    private static Dataset<Row> getShoppingDataFrame(SparkSession sparkSession){

        List<ShoppingItem> shoppingItems = Arrays.asList(
                new ShoppingItem("1",1000,"Laptop"),
                new ShoppingItem("2",35000,"Car"),
                new ShoppingItem("3",200,"Bag"),
                new ShoppingItem("4",3700,"Macbook")
        );

        return sparkSession.createDataFrame(shoppingItems,ShoppingItem.class);

    }



}
