package com.nitendratech.sparkgeneral.ingestion;

/**
 * Created by @author nitendratech on 5/24/20
 */

import com.nitendratech.sparkgeneral.util.AppUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads and Converts CSV file to Dataset
 */
public class CSVToDatasetHeader {

    public static void main(String args[]){

        CSVToDatasetHeader csvToDatasetHeader = new CSVToDatasetHeader();

        csvToDatasetHeader.startSparkApp();
    }



    private void startSparkApp(){
        SparkSession sparkSession = AppUtil.getSparkSession("CSVToDatasetHeader","local[2]");

        String filePath = "datasets/books.csv";

        Dataset<Row> csvDs = sparkSession
                                .read()
                                .format("csv")
                                .option("inferSchema", "false")
                                .option("header","true")
                                .load(filePath);
        csvDs.show();

        /**prints below
         * +---+--------+--------------------+--------------------+--------------------+
         * | id|authorId|               title|         releaseDate|                link|
         * +---+--------+--------------------+--------------------+--------------------+
         * |  1|       1|Fantastic Beasts ...|            11/18/16|http://amzn.to/2k...|
         * |  2|       1|Harry Potter and ...|             10/6/15|http://amzn.to/2l...|
         * |  3|       1|The Tales of Beed...|             12/4/08|http://amzn.to/2k...|
         */


        // Here everything is a String
        csvDs.printSchema();

        /**
         * root
         *  |-- id: string (nullable = true)
         *  |-- authorId: string (nullable = true)
         *  |-- title: string (nullable = true)
         *  |-- releaseDate: string (nullable = true)
         *  |-- link: string (nullable = true)
         */

        System.out.print(csvDs.count()); //Shows the csvFile Count

    }
}
