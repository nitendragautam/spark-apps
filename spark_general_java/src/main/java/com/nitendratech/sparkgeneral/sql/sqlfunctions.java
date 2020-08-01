package com.nitendratech.sparkgeneral.sql;


import com.nitendratech.sparkgeneral.util.GenerateDataSets;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Created by @author nitendratech on 5/23/20
 */
public class sqlfunctions {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkJDBCDeleteTest")
                .master("local[2]")
                .getOrCreate();


        String filterValue ="transaction_id != '7'";


        Dataset<Row> inputEmployeeData = GenerateDataSets.getInitialDataSets(sparkSession);


        inputEmployeeData.show();
        //inputEmployeeData.where("transaction_id = '7'").show();

        inputEmployeeData.where(filterValue).show();
    }




}
