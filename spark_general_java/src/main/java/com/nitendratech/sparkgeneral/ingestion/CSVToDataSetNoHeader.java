package com.nitendratech.sparkgeneral.ingestion;

import com.nitendratech.sparkgeneral.util.AppUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by @author nitendratech on 5/24/20
 */

/**
 * Reads the Data from CSV file converts into Dataset<Row>
 */
public class CSVToDataSetNoHeader {

    public static void main(String[] args){

        System.out.println("Staring the Spark Application");

        CSVToDataSetNoHeader csvToDataSetNoHeader = new CSVToDataSetNoHeader();

        csvToDataSetNoHeader.startSparkApp();
    }


    private void startSparkApp(){

        SparkSession sparkSession = AppUtil.getSparkSession("CSV to DataSet","local[2]");

        String filePath = "datasets/tuple-data-file.csv";

        Dataset<Row> tupleFileDataset = sparkSession.read()
                                        .format("csv") //File format type.
                                        .option("inferSchema","true")
                                        .option("header","false") //Dont use first line as Header
                                        .load(filePath);


        tupleFileDataset.show();// Display the data from the tuple
        /** Prints Below
         * +---+---+
         * |_c0|_c1|
         * +---+---+
         * |  1|  5|
         * |  2| 13|
         * |  3| 27|
         * |  4| 39|
         * |  5| 41|
         * |  6| 55|
         * +---+---+
         */
    }
}
