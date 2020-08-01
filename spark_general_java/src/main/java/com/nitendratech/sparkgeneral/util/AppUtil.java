package com.nitendratech.sparkgeneral.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * Created by @author nitendratech on 5/24/20
 */
public class AppUtil {

    public static SparkSession getSparkSession(String appName, String master){
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        return sparkSession;

    }
}
