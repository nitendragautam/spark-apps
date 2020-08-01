package com.nitendratech.sparklauncher;

import org.apache.spark.launcher.SparkAppHandle;

/**
 * Created by @author nitendratech on 6/14/20
 */
public class SparkListener implements SparkAppHandle.Listener {
    @Override
    public void stateChanged(SparkAppHandle sparkAppHandle) {

        System.out.println(sparkAppHandle.getAppId());
        System.out.println(sparkAppHandle.getState());

    }

    @Override
    public void infoChanged(SparkAppHandle sparkAppHandle) {

        System.out.println(sparkAppHandle.getState());

    }
}
