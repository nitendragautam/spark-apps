package com.nitendratech.sparkgeneral.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by @author nitendratech on 5/24/20
 */
public class AppUtil {

    public static final Logger logger = Logger.getLogger(AppUtil.class);

    public static SparkSession getSparkSession(String appName, String master){
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .master(master)
                .getOrCreate();

        return sparkSession;

    }



    /*
     * Read Config data from JSON File
     */
    public static JSONObject readJsonFromFile(String pathName) {
        String jsonData = "";
        BufferedReader br = null;
        JSONObject jsonObject = null;
        try {
            String line;
            br = new BufferedReader(new FileReader(pathName));
            while ((line = br.readLine()) != null) {
                jsonData += line + "\n";
            }

            jsonObject = new JSONObject(jsonData);

        } catch (FileNotFoundException e) {

            e.printStackTrace();
        } catch (JSONException e) {

            e.printStackTrace();
        } catch (IOException e) {

            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return jsonObject;

    }

    public static JSONArray getJsonArrayFromJsonObject(JSONObject jsonObject, String key){

        JSONArray jsonArray = null;

        if (!jsonObject.isEmpty()){
            jsonArray = jsonObject.getJSONArray(key);
        }else{
            logger.info("Given Json Object is Empty");

        }

        return jsonArray;

    }


    public static String getValFrJsonArrayNoQuotes(JSONArray jsonArray){

        return jsonArray.toString()
                .replace("[","")
                .replace("]","")
                .replace("\"","");
    }

    public static String getValFrJsonArrayWithQuotes(JSONArray jsonArray){


        return jsonArray.toString()
                .replace("[","")
                .replace("]","");
    }


    /**
     * Gives a Json Array given a Json Value
     * @param jsonValue
     * @return
     */
    public static String getJsonArray(String jsonValue){

        JSONArray jsonArray = new JSONArray();
        jsonArray.put(jsonValue);

        return jsonArray.toString();
    }



}
