package com.nitendratech.sparkgeneral.jdbc;

import com.nitendratech.sparkgeneral.util.GenerateDataSets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

/**
 * Created by @author nitendratech on 6/21/20
 */

/**
 * Example of Hard Delete in Apache Spark
 */
public class SparkJDBCDeleteTest {


    public static  Dataset<Row> getDataSetAfterDeletion(Dataset<Row> inputDataSet,String filteredQuery){

        Dataset<Row> filteredDataset = inputDataSet.filter(filteredQuery);

    return filteredDataset;

    }


    public static String getDeleteQuery(String tableName, String deleteCondition){

        String deleteQuery ="null";
        if (tableName !=null && deleteCondition!=null){
            deleteQuery ="( DELETE FROM " +tableName + " WHERE " + deleteCondition +" )";

        }

        return deleteQuery;

    }

    public static void main(String args[]){

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkJDBCDeleteTest")
                .master("local[2]")
                .getOrCreate();

        String jdbcUrl ="jdbc:oracle:thin:ot/admin123@//localhost:1521/xe";
        String userName ="ot";
        String passWord="admin123";
        String inputTable="employee_data";
        String outputTable="employee_data";

        String filteredQuery = "transaction_id !=7";
        String inputTableSelectQuery="(SELECT * FROM " +inputTable +")";





        SparkJDBCUtils sparkJDBCUtils = new SparkJDBCUtils(jdbcUrl,userName,passWord);






        // Read from Oracle Table using Load Method
        Dataset<Row> inputRowM1 = sparkJDBCUtils.readUsingLoad(sparkSession,inputTableSelectQuery);

        if(!inputRowM1.isEmpty()){

            System.out.println("Table is not empty");

        } else{

            Dataset<Row> inputEmployeeData = GenerateDataSets.getInitialDataSets(sparkSession);
            inputEmployeeData.show();
            // Test the Write Method


            Dataset<Row> dataAfterDeletion = getDataSetAfterDeletion(inputEmployeeData, filteredQuery);

            dataAfterDeletion.cache();

            sparkJDBCUtils.writeUsingSave(dataAfterDeletion,outputTable,"overwrite");

            Dataset<Row> inputRowM2 = sparkJDBCUtils.readUsingLoad(sparkSession,inputTableSelectQuery);

            inputRowM2.show();

            dataAfterDeletion.unpersist();

        }


        // Print the Load Method Schema
        //inputRowM1.printSchema();

    }
}
