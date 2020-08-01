package com.nitendratech.sparklauncher;

import jdk.internal.dynalink.beans.StaticClass;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import com.nitendratech.sparklauncher.SparkListener;

import java.io.IOException;

/**
 * Created by @author nitendratech on 6/14/20
 */
public class SparkLauncheAndMonitor {

    private static Logger logger = Logger.getLogger(SparkLauncheAndMonitor.class);

    private static final String JAR_LOC="/Users/nitendragautam/nitendra_items/github_projects/study_notes/projects/spark_apps/spark_general_java/target/sparkgeneral-1.0.jar";

    private static final String FILE_LOCATION="/Users/nitendragautam/nitendra_items/github_projects/study_notes/projects/spark_apps/spark_general_java/datasets/word_count.text";



    public static void startSparkByLaunch(String appName, String jarLocation, String fileLocation) throws IOException, InterruptedException {


        logger.info("Starting Spark Application");
        Process spark = new SparkLauncher()
                .setAppName(appName)
                .setAppResource(JAR_LOC)
                .setMainClass("com.nitendratech.sparkgeneral.WordCount")
                .addAppArgs(fileLocation)
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "4")
                .setMaster("local[2]")
                .launch();

        logger.info("Waiting for  Spark Application");
        spark.waitFor();

    }


    private static void startSparkByHandle(String appName,String jarLocation,String fileLocation) throws IOException{

        logger.info("Starting Spark Application");

        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setMaster("local[2]")
                .setAppName(appName)
                .setMainClass("com.nitendratech.sparkgeneral.WordCount")
                .setAppResource(jarLocation)
                .addAppArgs(fileLocation);


        SparkAppHandle sparkAppHandle =sparkLauncher
                .startApplication(new SparkAppHandle.Listener(){
                    boolean isRunning = false;

                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        String appStatus = sparkAppHandle.getState().toString();
                        logger.info(sparkAppHandle.getAppId());
                        logger.info(sparkAppHandle.getState());

                        // If the App is Running
                        if(!isRunning && appStatus.equals("RUNNING")){
                            isRunning=true;
                        }


                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        logger.info("infoChanged:appId-->" + sparkAppHandle.getAppId() + "\tstatus-->" + sparkAppHandle.getState().toString());

                    }

                });


        String appStatus = sparkAppHandle.getState().toString();


                while (!appStatus.equals("FINISHED") || !appStatus.equals("KILLED") || !appStatus.equals("FAILED")){
                    logger.info("infoChanged:appId-->" + sparkAppHandle.getAppId() + "\tstatus-->" + sparkAppHandle.getState().toString());

                    try {
                        Thread.sleep(1000*5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }






        logger.info("Finished Spark Application");


    }


    public static void main(String [] args) throws IOException, InterruptedException {

        startSparkByHandle("SparkLauncheAndMonitor",JAR_LOC,FILE_LOCATION);
        //startSparkByLaunch("SparkLauncheAndMonitor",JAR_LOC,FILE_LOCATION);
}}
