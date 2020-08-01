package com.nitendratech.sparkgeneral.jdbc;

/**
 * Created by @author nitendratech on 5/6/20
 */
import org.apache.spark.sql.*;

import java.util.Properties;

/**
 * Uses the Spark Native JDBC Connection
 */
public class SparkJDBCUtils {

    private static String jdbcUrl;
    private static String userName;
    private static String passWord;

    public SparkJDBCUtils(String jdbcUrl,String userName, String passWord){
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.passWord = passWord;
    }


    /**
     * Loading Data from JDBC Source using Load Method
     * @param sparkSession
     * @param tableName
     */
    public  Dataset<Row> readUsingLoad(SparkSession sparkSession, String tableName){

        Dataset<Row> jdbcDataFrame = sparkSession.read()
                .format("jdbc")
                .option("url",jdbcUrl)
                .option("dbtable",tableName)
                .option("user",userName)
                .option("password",passWord)
                .option("fetchsize","1000")
                .load();

        return jdbcDataFrame;

    }

    public Dataset<Row> readUsingJDBCMethod(SparkSession sparkSession,  String tableName){



        Dataset<Row> jdbcDF = sparkSession.read()
                .jdbc(jdbcUrl,tableName,getConnectionProperties());

        return jdbcDF;
    }

    public  void writeUsingSave(Dataset<Row> dataSet,String tableName, String saveMode){
        dataSet.write()
                .format("jdbc")
                .option("url",jdbcUrl)
                .option("dbtable",tableName)
                .option("user",userName)
                .option("password",passWord)
                .mode(saveMode)
                .save();

    }

    public  void writeUsingJDBC(Dataset<Row> dataSet, String tableName){
        dataSet.write()
                .jdbc(jdbcUrl,tableName,getConnectionProperties());

    }

    private static Properties getConnectionProperties(){

        Properties props = new Properties();
        props.put("user",userName);
        props.put("password",passWord);
        return props;
    }






}